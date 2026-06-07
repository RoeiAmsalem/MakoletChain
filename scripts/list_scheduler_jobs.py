"""Print every job registered in scheduler.py (id | name | trigger).

Importing scheduler.py registers all jobs on the module-level BlockingScheduler
WITHOUT starting it (start() only runs under __main__), so this is a safe,
read-only way to confirm a job is wired in.

Usage:  python3 scripts/list_scheduler_jobs.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import scheduler  # noqa: E402  — top-level add_job calls register jobs on import

for j in scheduler.scheduler.get_jobs():
    print(f"{j.id} | {j.name} | {j.trigger}")
