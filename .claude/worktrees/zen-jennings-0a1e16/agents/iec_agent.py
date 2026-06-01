"""
IEC agent — fetches electricity invoices from Israel Electric Company API.

Runs daily at 06:00 IL for each branch where iec_token IS NOT NULL.
Upserts invoices into electricity_invoices table.
"""

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from iec_api.iec_client import IecClient
from iec_api.models.jwt import JWT
from iec_api.models.exceptions import IECLoginError

from utils.notify import notify

log = logging.getLogger(__name__)

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'makolet_chain.db')


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _record_agent_run(branch_id, status, message='', docs_count=0, amount=0, started_at=None):
    """Record agent run in agent_runs table."""
    conn = _get_db()
    now = datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    started = started_at or now
    duration = None
    if started_at:
        try:
            t0 = datetime.strptime(started_at, '%Y-%m-%d %H:%M:%S')
            t1 = datetime.strptime(now, '%Y-%m-%d %H:%M:%S')
            duration = int((t1 - t0).total_seconds())
        except Exception:
            pass
    conn.execute('''
        INSERT INTO agent_runs (branch_id, agent, started_at, finished_at, status, docs_count, amount, message, duration_seconds)
        VALUES (?, 'iec', ?, ?, ?, ?, ?, ?, ?)
    ''', (branch_id, started, now, status, docs_count, amount, message, duration))
    conn.commit()
    conn.close()


async def _sync_branch(branch_id: int, iec_user_id: str, iec_token: str,
                        iec_bp_number: str, iec_contract_id: str) -> dict:
    """Sync IEC invoices for a single branch. Returns result dict."""
    started_at = datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M:%S')

    async with IecClient(iec_user_id) as client:
        # Rehydrate token from stored refresh_token
        jwt_token = JWT(
            access_token='',
            refresh_token=iec_token,
            token_type='Bearer',
            expires_in=0,
            scope='openid email profile offline_access',
            id_token=''
        )

        # Refresh to get a valid id_token
        try:
            await client.refresh_token()  # This won't work without a valid token first
        except Exception:
            pass

        # Set the token with refresh_token and try refreshing
        client._token = jwt_token
        try:
            from iec_api import login
            new_jwt = await login.refresh_token(client._session, jwt_token)
            client._token = new_jwt
            client.logged_in = True
        except Exception as e:
            error_msg = f"IEC token refresh failed: {e}"
            log.error("Branch %d: %s", branch_id, error_msg)
            _record_agent_run(branch_id, 'error', error_msg, started_at=started_at)
            notify(
                f"⚠️ IEC — Branch {branch_id}",
                f"Token refresh failed — may need re-onboarding."
            )
            return {'status': 'error', 'message': error_msg}

        # Check if refresh_token was rotated — save the new one
        new_refresh = client._token.refresh_token
        if new_refresh and new_refresh != iec_token:
            log.info("Branch %d: IEC refresh_token rotated, saving new one", branch_id)
            conn = _get_db()
            conn.execute('UPDATE branches SET iec_token = ? WHERE id = ?', (new_refresh, branch_id))
            conn.commit()
            conn.close()

        # Fetch invoices
        try:
            electric_bill = await client.get_electric_bill(iec_bp_number, iec_contract_id)
        except Exception as e:
            error_msg = f"Failed to fetch electric bills: {e}"
            log.error("Branch %d: %s", branch_id, error_msg)
            _record_agent_run(branch_id, 'error', error_msg, started_at=started_at)
            return {'status': 'error', 'message': error_msg}

        if not electric_bill or not electric_bill.invoices:
            msg = "No invoices returned from IEC"
            log.info("Branch %d: %s", branch_id, msg)
            _record_agent_run(branch_id, 'success', msg, started_at=started_at)
            _update_last_sync(branch_id)
            return {'status': 'success', 'message': msg, 'count': 0}

        # Upsert invoices
        conn = _get_db()
        upserted = 0
        total_amount = 0.0

        for inv in electric_bill.invoices:
            invoice_number = str(inv.invoice_id)
            is_paid = 1 if inv.amount_to_pay == 0 else 0
            amount = inv.amount_origin
            due_date = inv.last_date.isoformat() if inv.last_date else None
            total_amount += amount

            # Build period label from dates
            period_label = None
            if inv.from_date and inv.to_date:
                period_label = f"{inv.from_date.strftime('%d/%m/%Y')} - {inv.to_date.strftime('%d/%m/%Y')}"

            # Store raw JSON for future use
            try:
                raw = json.dumps(inv.to_dict(), default=str, ensure_ascii=False)
            except Exception:
                raw = None

            conn.execute('''
                INSERT INTO electricity_invoices
                    (branch_id, invoice_number, period_label, amount, due_date, is_paid, source, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, 'iec_api', ?)
                ON CONFLICT (branch_id, invoice_number) DO UPDATE SET
                    amount = excluded.amount,
                    due_date = excluded.due_date,
                    is_paid = excluded.is_paid,
                    period_label = excluded.period_label,
                    raw_json = excluded.raw_json
            ''', (branch_id, invoice_number, period_label, amount, due_date, is_paid, raw))
            upserted += 1

        conn.commit()
        conn.close()

        _update_last_sync(branch_id)
        msg = f"{upserted} invoices synced, total ₪{total_amount:.0f}, outstanding ₪{electric_bill.total_amount_to_pay:.0f}"
        log.info("Branch %d: %s", branch_id, msg)
        _record_agent_run(branch_id, 'success', msg, docs_count=upserted,
                         amount=electric_bill.total_amount_to_pay, started_at=started_at)

        return {'status': 'success', 'message': msg, 'count': upserted}


def _update_last_sync(branch_id: int):
    now = datetime.now(IL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    conn = _get_db()
    conn.execute('UPDATE branches SET iec_last_sync_at = ? WHERE id = ?', (now, branch_id))
    conn.commit()
    conn.close()


def run_iec_sync(branch_id: int) -> dict:
    """Sync IEC invoices for a single branch (sync wrapper)."""
    conn = _get_db()
    row = conn.execute('''
        SELECT iec_user_id, iec_token, iec_bp_number, iec_contract_id, name
        FROM branches WHERE id = ? AND iec_token IS NOT NULL
    ''', (branch_id,)).fetchone()
    conn.close()

    if not row:
        return {'status': 'skip', 'message': f'Branch {branch_id} has no IEC token'}

    return asyncio.run(_sync_branch(
        branch_id,
        row['iec_user_id'],
        row['iec_token'],
        row['iec_bp_number'],
        row['iec_contract_id']
    ))


def run_iec_all():
    """Run IEC sync for all branches with IEC tokens configured."""
    conn = _get_db()
    rows = conn.execute('''
        SELECT id FROM branches WHERE active = 1 AND iec_token IS NOT NULL
    ''').fetchall()
    conn.close()

    results = {}
    for row in rows:
        bid = row['id']
        log.info("Running IEC sync for branch %d", bid)
        try:
            result = run_iec_sync(bid)
            results[bid] = result
            log.info("Branch %d IEC: %s", bid, result.get('message', ''))
        except Exception as e:
            log.error("Branch %d IEC failed: %s", bid, e)
            results[bid] = {'status': 'error', 'message': str(e)}

    return results


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    import sys
    if len(sys.argv) > 1:
        bid = int(sys.argv[1])
        print(run_iec_sync(bid))
    else:
        print(run_iec_all())
