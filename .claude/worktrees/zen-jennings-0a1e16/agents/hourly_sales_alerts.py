"""Hourly sales alerting — runs every 30 min during store hours via scheduler.

Calls health checks for each active branch and sends brrr alerts on failures.
Rate-limited: max 1 alert per check type per branch per hour.
"""

import logging
import os
import sqlite3
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from agents.hourly_sales_monitor import (
    check_heartbeat, check_hour_coverage,
    check_daily_reconciliation, check_suspicious_spikes,
    _is_store_hours
)
from utils.notify import notify

log = logging.getLogger(__name__)

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'makolet_chain.db')

# In-memory rate limiter: {(branch_id, check_type): last_alert_timestamp}
_alert_history: dict[tuple, float] = {}
ALERT_COOLDOWN_SECONDS = 3600  # 1 hour


def _should_alert(branch_id: int, check_type: str) -> bool:
    """Rate limit: max 1 alert per check type per branch per hour."""
    key = (branch_id, check_type)
    last = _alert_history.get(key, 0)
    if time.time() - last < ALERT_COOLDOWN_SECONDS:
        return False
    return True


def _record_alert(branch_id: int, check_type: str):
    _alert_history[(branch_id, check_type)] = time.time()


def run_hourly_alerts():
    """Main entry point — called by scheduler every 30 min during store hours."""
    now = datetime.now(IL_TZ)
    if not _is_store_hours(now):
        log.info("Hourly alerts: outside store hours, skipping")
        return

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    branches = conn.execute('SELECT id, name FROM branches WHERE active = 1').fetchall()
    today = now.date().isoformat()

    for branch in branches:
        bid = branch['id']
        bname = branch['name']

        try:
            # Heartbeat check
            hb = check_heartbeat(bid, conn)
            if hb['status'] == 'red' and _should_alert(bid, 'heartbeat'):
                notify(
                    f"⚠️ Hourly data — {bname}",
                    f"No data received: {hb['message']}"
                )
                _record_alert(bid, 'heartbeat')
                log.warning("Branch %d (%s): heartbeat RED — alert sent", bid, bname)

            # Hour coverage (only meaningful after 23:30)
            cov = check_hour_coverage(bid, today, conn)
            if cov['status'] == 'red' and _should_alert(bid, 'coverage'):
                notify(
                    f"⚠️ Hour coverage — {bname}",
                    f"Only {cov['covered']}/{cov['total']} hours covered: {cov['message']}"
                )
                _record_alert(bid, 'coverage')
                log.warning("Branch %d (%s): coverage RED — alert sent", bid, bname)

            # Daily reconciliation
            rec = check_daily_reconciliation(bid, today, conn)
            if rec['status'] == 'red' and _should_alert(bid, 'reconciliation'):
                notify(
                    f"⚠️ Reconciliation — {bname}",
                    f"Hourly vs daily mismatch: {rec['message']}"
                )
                _record_alert(bid, 'reconciliation')
                log.warning("Branch %d (%s): reconciliation RED — alert sent", bid, bname)

            # Suspicious spikes
            spikes = check_suspicious_spikes(bid, today, conn)
            if spikes and _should_alert(bid, 'spikes'):
                spike_summary = ', '.join(
                    f"hour {s['hour']}: ₪{s['amount']:,.0f}"
                    for s in spikes[:3]
                )
                notify(
                    f"📊 Spike detected — {bname}",
                    f"{len(spikes)} unusual hourly totals: {spike_summary}"
                )
                _record_alert(bid, 'spikes')
                log.info("Branch %d (%s): %d spikes detected — alert sent", bid, bname, len(spikes))

        except Exception as e:
            log.error("Branch %d (%s): alert check failed: %s", bid, bname, e)

    conn.close()
    log.info("Hourly alerts check complete for %d branches", len(branches))
