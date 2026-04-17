"""MakoletChain Scheduler — nightly + live jobs for all active branches."""

import logging
import os
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
load_dotenv()

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

IL_TZ = ZoneInfo('Asia/Jerusalem')
DB_PATH = os.path.join(os.path.dirname(__file__), 'db', 'makolet_chain.db')
SCHEMA_PATH = os.path.join(os.path.dirname(__file__), 'db', 'schema.sql')


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    with open(SCHEMA_PATH, 'r') as f:
        conn.executescript(f.read())
    conn.close()


def get_active_branches() -> list[int]:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    rows = conn.execute('SELECT id FROM branches WHERE active = 1').fetchall()
    conn.close()
    return [r[0] for r in rows]


def _check_consecutive_failures(bid):
    """Send brrr alert if branch has 6+ consecutive Aviv errors (~30 min).
    Also sends recovery alert when Aviv comes back online after failures."""
    from utils.notify import notify
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        recent = conn.execute('''
            SELECT status FROM agent_runs
            WHERE branch_id=? AND agent='aviv_live'
            ORDER BY started_at DESC LIMIT 8
        ''', (bid,)).fetchall()

        row = conn.execute('SELECT name FROM branches WHERE id=?', (bid,)).fetchone()
        branch_name = row['name'] if row else f'Branch {bid}'
        conn.close()

        if not recent:
            return

        # Recovery detection: latest is success, but previous 6+ were errors
        if recent[0]['status'] == 'success':
            consecutive_errors = 0
            for r in recent[1:]:
                if r['status'] == 'error':
                    consecutive_errors += 1
                else:
                    break
            if consecutive_errors >= 6:
                notify(
                    f"✅ Aviv Live — {branch_name}",
                    f"Back online after {consecutive_errors * 5} minutes of downtime."
                )
                log.info("Branch %d: Aviv recovered after %d consecutive failures", bid, consecutive_errors)
            return

        # Failure detection: 6+ consecutive errors
        if len(recent) >= 6 and all(r['status'] == 'error' for r in recent[:6]):
            consecutive = sum(1 for r in recent if r['status'] == 'error')
            # Only alert once: if 7th run was also error, we already alerted
            if len(recent) >= 7 and recent[6]['status'] == 'error':
                return
            notify(
                f"⚠️ Aviv Live — {branch_name}",
                f"Down for 30 minutes ({consecutive} failed attempts)."
            )
            log.warning("Branch %d: 30min consecutive Aviv failures — brrr alert sent", bid)
    except Exception as e:
        log.error("Failed to check consecutive failures: %s", e)


def run_aviv_all():
    """Run aviv_live for all active branches."""
    from agents.aviv_live import run_aviv_live
    branches = get_active_branches()
    for bid in branches:
        log.info("Running aviv_live for branch %d", bid)
        try:
            result = run_aviv_live(bid)
            log.info("Branch %d: %s", bid, result)
        except Exception as e:
            log.error("Branch %d aviv_live failed: %s", bid, e)
        _check_consecutive_failures(bid)


def nightly_sync():
    """Nightly 02:00 IL — run bilboy + gmail_sync for all active branches."""
    from agents.bilboy import run_bilboy
    from agents.gmail_agent import run_gmail_sync

    branches = get_active_branches()
    log.info("=== Nightly sync started for %d branches ===", len(branches))

    for bid in branches:
        log.info("--- Branch %d ---", bid)
        try:
            bb = run_bilboy(bid)
            log.info("BilBoy branch %d: %s", bid, bb)
        except Exception as e:
            log.error("BilBoy branch %d failed: %s", bid, e)

        try:
            gm = run_gmail_sync(bid)
            log.info("Gmail branch %d: %s", bid, gm)
        except Exception as e:
            log.error("Gmail branch %d failed: %s", bid, e)

    log.info("=== Nightly sync complete ===")


scheduler = BlockingScheduler(
    timezone=IL_TZ,
    job_defaults={
        'misfire_grace_time': 120,
        'coalesce': True,
        'max_instances': 1
    }
)


# 06:30–06:55 IL — early window before main cron kicks in
@scheduler.scheduled_job('cron', hour=6, minute='30,35,40,45,50,55', id='aviv_early')
def scheduled_aviv_early():
    run_aviv_all()


# 07:00–22:55 IL — main window; inner day-aware guard handles Fri/Sat edges
@scheduler.scheduled_job('cron', hour='7-22', minute='*/5', id='aviv_live')
def scheduled_aviv():
    run_aviv_all()


# Nightly 02:00 IL: bilboy + gmail for all branches
@scheduler.scheduled_job('cron', hour=2, minute=0, id='nightly_sync')
def scheduled_nightly():
    nightly_sync()


def run_hours_midday():
    """16:00 — midday estimate (baseline + current shift)."""
    from agents.aviv_live import scrape_hours_midday
    branches = get_active_branches()
    for bid in branches:
        log.info("Hours midday for branch %d", bid)
        try:
            result = scrape_hours_midday(bid)
            log.info("Branch %d midday: %s", bid, result)
        except Exception as e:
            log.error("Branch %d hours midday failed: %s", bid, e)


def run_hours_end_of_day():
    """23:30 — authoritative end-of-day total."""
    from agents.aviv_live import scrape_hours_end_of_day
    branches = get_active_branches()
    for bid in branches:
        log.info("Hours end-of-day for branch %d", bid)
        try:
            result = scrape_hours_end_of_day(bid)
            log.info("Branch %d end-of-day: %s", bid, result)
        except Exception as e:
            log.error("Branch %d hours end-of-day failed: %s", bid, e)


# 16:00 — midday estimate (baseline + current shift)
scheduler.add_job(
    func=run_hours_midday,
    trigger=CronTrigger(hour=16, minute=0, timezone=IL_TZ),
    id='hours_midday',
    name='Hours midday estimate 16:00',
)

# 23:30 — authoritative end-of-day total
scheduler.add_job(
    func=run_hours_end_of_day,
    trigger=CronTrigger(hour=23, minute=30, timezone=IL_TZ),
    id='hours_end_of_day',
    name='Hours authoritative 23:30',
)


def run_aviv_employees():
    """23:45 — fetch per-employee hours from Aviv BI API."""
    from agents.aviv_employees import run_aviv_employees as _run
    branches = get_active_branches()
    for bid in branches:
        log.info("Aviv employees for branch %d", bid)
        try:
            result = _run(bid)
            log.info("Branch %d employees: %s", bid, result)
        except Exception as e:
            log.error("Branch %d aviv_employees failed: %s", bid, e)


def cleanup_orphaned_runs():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    result = conn.execute(
        '''UPDATE agent_runs
           SET status='error', message='orphaned on startup', finished_at=datetime('now')
           WHERE status='running' AND started_at < datetime('now', '-1 hour')'''
    )
    if result.rowcount > 0:
        log.warning("Cleaned up %d orphaned agent_runs on startup", result.rowcount)
    conn.commit()
    conn.close()


# 15:00 — per-employee hours from Aviv BI API (midday snapshot)
scheduler.add_job(
    func=run_aviv_employees,
    trigger=CronTrigger(hour=15, minute=0, timezone=IL_TZ),
    id='aviv_employees_midday',
    name='Aviv employees hours 15:00',
)

# 23:45 — per-employee hours from Aviv BI API (end of day)
scheduler.add_job(
    func=run_aviv_employees,
    trigger=CronTrigger(hour=23, minute=45, timezone=IL_TZ),
    id='aviv_employees',
    name='Aviv employees hours 23:45',
)


def run_iec_sync():
    """06:00 — daily IEC electricity invoice sync."""
    from agents.iec_agent import run_iec_all
    log.info("Running IEC sync for all branches")
    try:
        results = run_iec_all()
        for bid, result in results.items():
            log.info("Branch %d IEC: %s", bid, result.get('message', ''))
    except Exception as e:
        log.error("IEC sync failed: %s", e)


# 06:00 — daily IEC electricity invoice sync
scheduler.add_job(
    func=run_iec_sync,
    trigger=CronTrigger(hour=6, minute=0, timezone=IL_TZ),
    id='iec_sync',
    name='IEC electricity sync 06:00',
)


if __name__ == '__main__':
    if os.getenv('ENABLE_AGENTS', 'true').lower() == 'false':
        log.info('[scheduler] ENABLE_AGENTS=false — skipping all agent scheduling')
        import sys
        sys.exit(0)

    init_db()
    cleanup_orphaned_runs()
    log.info('MakoletChain scheduler started')

    # Run aviv_live once on startup
    log.info('Running startup aviv_live pass...')
    run_aviv_all()

    log.info('Scheduler running — aviv every 5min, hours midday 16:00 + end-of-day 23:30, nightly 02:00 IL')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info('Scheduler stopped.')
