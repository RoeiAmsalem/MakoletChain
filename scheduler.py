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


scheduler = BlockingScheduler(timezone=IL_TZ)


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


if __name__ == '__main__':
    init_db()
    log.info('MakoletChain scheduler started')

    # Run aviv_live once on startup
    log.info('Running startup aviv_live pass...')
    run_aviv_all()

    log.info('Scheduler running — aviv every 5min, hours midday 16:00 + end-of-day 23:30, nightly 02:00 IL')
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info('Scheduler stopped.')
