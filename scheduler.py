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


# Switch between chain (one login + one multi-branch call) and legacy
# per-branch loop. Flip USE_CHAIN to False to revert in one line.
USE_CHAIN = True


def run_aviv_all():
    """Run aviv_live for all active branches."""
    if USE_CHAIN:
        _run_aviv_chain()
    else:
        _run_aviv_per_branch()


def _run_aviv_chain():
    """Chain-account path: one login + one multi-branch POST."""
    from agents.aviv_live import run_aviv_live_chain
    log.info("Running aviv_live (chain) for all branches with aviv_branch_id")
    try:
        result = run_aviv_live_chain()
        log.info("Chain result: %s", result)
    except Exception as e:
        log.error("aviv_live chain run failed: %s", e)
    for bid in get_active_branches():
        _check_consecutive_failures(bid)


def _run_aviv_per_branch():
    """Legacy fallback — per-branch loop. Kept intact for one-line revert."""
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


def run_hourly_alerts():
    """Every 30 min during store hours — check hourly_sales health + brrr alerts."""
    from agents.hourly_sales_alerts import run_hourly_alerts as _run
    log.info("Running hourly sales health alerts")
    try:
        _run()
    except Exception as e:
        log.error("Hourly sales alerts failed: %s", e)


# Every 30 min between 07:00-23:00 — hourly sales data health
scheduler.add_job(
    func=run_hourly_alerts,
    trigger=CronTrigger(hour='7-22', minute='0,30', timezone=IL_TZ),
    id='hourly_sales_alerts',
    name='Hourly sales health alerts',
)


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


# === DEPRECATED 2026-05-10 — replaced by aviv_employees_report agent ===
# The old aviv_employees agent ran at 15:00 + 23:45 IL and clobbered new
# aviv_employees_report rows because of UNIQUE(branch_id, month, employee_name)
# in employee_hours. The 23:45 run landed 15 minutes after the 23:30
# aviv_employees_report run and overwrote source='aviv_report' rows with
# source='aviv_api' (audit 2026-05-10: 100% clobber on branch 126, 40% on 127).
# Cutover to aviv_employees_report on 2026-05-10. Keep this block commented
# for ~30 days; once monitoring confirms the new agent is stable, delete
# agents/aviv_employees.py and these lines together.
#
# # 15:00 — per-employee hours from Aviv BI API (midday snapshot)
# scheduler.add_job(
#     func=run_aviv_employees,
#     trigger=CronTrigger(hour=15, minute=0, timezone=IL_TZ),
#     id='aviv_employees_midday',
#     name='Aviv employees hours 15:00',
# )
#
# # 23:45 — per-employee hours from Aviv BI API (end of day)
# scheduler.add_job(
#     func=run_aviv_employees,
#     trigger=CronTrigger(hour=23, minute=45, timezone=IL_TZ),
#     id='aviv_employees',
#     name='Aviv employees hours 23:45',
# )


def run_aviv_report_all(include_previous_month: bool = False):
    """Run aviv employer-report agent for all active branches.

    Thin shim over agents.aviv_employees_report.run_all_branches(), which owns
    the AVIV_EMP_USE_CHAIN chain login (one token reused across branches), the
    branch iteration, the 30s between-branch jitter, and per-branch error
    catching. Previously this reimplemented a per-store loop that never passed
    chain_token — so chain-only stores (NULL per-store creds) errored nightly
    with "No Aviv credentials".
    """
    from agents.aviv_employees_report import run_all_branches
    log.info("=== Aviv employer-report run started "
             "(include_previous_month=%s) ===", include_previous_month)
    results = run_all_branches(include_previous_month=include_previous_month)
    ok = sum(1 for r in results if r.get('ok'))
    log.info("=== Aviv employer-report run complete: %d/%d ok ===",
             ok, len(results))


def run_aviv_report_current():
    run_aviv_report_all(include_previous_month=False)


def run_aviv_report_with_prev():
    run_aviv_report_all(include_previous_month=True)


# Sun-Thu 16:00 IL — current month only
scheduler.add_job(
    func=run_aviv_report_current,
    trigger=CronTrigger(day_of_week='sun,mon,tue,wed,thu', hour=16, minute=0,
                        timezone=IL_TZ),
    id='aviv_report_weekday_afternoon',
    name='Aviv employer-report Sun-Thu 16:00',
)

# Sun-Thu 23:30 IL — current + previous month
scheduler.add_job(
    func=run_aviv_report_with_prev,
    trigger=CronTrigger(day_of_week='sun,mon,tue,wed,thu', hour=23, minute=30,
                        timezone=IL_TZ),
    id='aviv_report_weekday_night',
    name='Aviv employer-report Sun-Thu 23:30 (+prev month)',
)

# Friday 20:00 IL — current month only
scheduler.add_job(
    func=run_aviv_report_current,
    trigger=CronTrigger(day_of_week='fri', hour=20, minute=0, timezone=IL_TZ),
    id='aviv_report_friday',
    name='Aviv employer-report Fri 20:00',
)

# Saturday 23:30 IL — current + previous month
scheduler.add_job(
    func=run_aviv_report_with_prev,
    trigger=CronTrigger(day_of_week='sat', hour=23, minute=30, timezone=IL_TZ),
    id='aviv_report_saturday',
    name='Aviv employer-report Sat 23:30 (+prev month)',
)


def run_iec_sync():
    """06:00 — daily IEC electricity invoice sync via SSH to Israeli VPS.

    IEC API is geo-blocked outside Israel. The sync script runs on the
    Kamatera VPS (185.253.75.56) and POSTs results back via /api/internal/iec-sync.
    """
    import subprocess
    log.info("Running IEC sync via SSH to VPS")
    try:
        proc = subprocess.run(
            ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
             'makolet-iec',
             '/opt/makolet-iec/venv/bin/python /opt/makolet-iec/iec_sync.py'],
            capture_output=True, text=True, timeout=300
        )
        if proc.returncode == 0:
            log.info("IEC sync completed: %s", proc.stdout.strip().split('\n')[-1])
        else:
            log.error("IEC sync failed (exit %d): %s", proc.returncode,
                       proc.stderr.strip() or proc.stdout.strip())
    except subprocess.TimeoutExpired:
        log.error("IEC sync timed out after 300s")
    except Exception as e:
        log.error("IEC sync failed: %s", e)


# 06:00 — daily IEC electricity invoice sync
scheduler.add_job(
    func=run_iec_sync,
    trigger=CronTrigger(hour=6, minute=0, timezone=IL_TZ),
    id='iec_sync',
    name='IEC electricity sync 06:00',
)


def cleanup_old_user_events():
    """Delete user_events older than 90 days (hard retention cap)."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.execute(
            "DELETE FROM user_events WHERE created_at < datetime('now', '-90 days')"
        )
        conn.commit()
        log.info("Cleaned up %d old user_events rows", cur.rowcount)
        conn.close()
    except Exception as e:
        log.error("cleanup_old_user_events failed: %s", e)


# 03:00 IL — daily user_events retention cleanup (90-day cap)
scheduler.add_job(
    func=cleanup_old_user_events,
    trigger=CronTrigger(hour=3, minute=0, timezone=IL_TZ),
    id='cleanup_user_events_daily',
    name='Cleanup user_events older than 90 days',
)


def recompute_analytics_cache():
    """Recompute /admin/analytics aggregates for all ranges. Runs right after
    the 03:00 retention cleanup so the cache reflects the post-prune dataset."""
    try:
        from app import app, _analytics_aggregate, _analytics_cache_set, _VALID_ANALYTICS_RANGES
        with app.app_context():
            for r in _VALID_ANALYTICS_RANGES:
                payload = _analytics_aggregate(r)
                _analytics_cache_set(r, payload)
        log.info("Recomputed analytics_cache for %d ranges", len(_VALID_ANALYTICS_RANGES))
    except Exception as e:
        log.error("recompute_analytics_cache failed: %s", e)


# 03:30 IL — nightly recompute of analytics_cache (right after 03:00 cleanup)
scheduler.add_job(
    func=recompute_analytics_cache,
    trigger=CronTrigger(hour=3, minute=30, timezone=IL_TZ),
    id='recompute_analytics_cache_daily',
    name='Recompute /admin/analytics cache',
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
