"""MakoletChain Scheduler — nightly jobs for all active branches."""

import logging
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

IL_TZ = ZoneInfo('Asia/Jerusalem')

scheduler = BlockingScheduler(timezone=IL_TZ)


@scheduler.scheduled_job('cron', hour=2, minute=0)
def nightly_sync():
    """Placeholder — will loop over active branches and sync data."""
    log.info('nightly_sync: starting for all active branches')
    # TODO: loop branches, fetch Z-reports, BilBoy docs, etc.
    log.info('nightly_sync: done')


if __name__ == '__main__':
    log.info('MakoletChain scheduler started')
    scheduler.start()
