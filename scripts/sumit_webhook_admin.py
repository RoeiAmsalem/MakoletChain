#!/usr/bin/env python3
"""Manage the SUMIT webhook subscription (the ONE sanctioned non-read call).

SUMIT's /triggers/triggers/subscribe/ registers a URL on a CRM folder; SUMIT
then POSTs to it whenever a matching event happens. We subscribe our
/api/billing/sumit-webhook receiver to the receipts folder (קבלות,
TriggerType=Create) so a hosted-page payment triggers the read-only billing
sync instantly instead of waiting for the daily sweep.

The public spec has NO list endpoint — removal is keyed on the URL alone, so
whatever URL you subscribe here is also the handle to undo it.

Usage:
  python3 scripts/sumit_webhook_admin.py subscribe   <receiver-url>
  python3 scripts/sumit_webhook_admin.py unsubscribe <receiver-url>

Requires SUMIT_API_KEY + SUMIT_ORG_ID (read from .env next to the repo root
if not already in the environment).
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _load_env():
    path = os.path.join(ROOT, ".env")
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ("subscribe", "unsubscribe"):
        print(__doc__)
        return 2
    action, url = sys.argv[1], sys.argv[2]
    if not url.startswith("https://"):
        print(f"refusing non-https receiver URL: {url}")
        return 2

    _load_env()
    from utils import sumit
    if not sumit.is_connected():
        print("SUMIT_API_KEY / SUMIT_ORG_ID missing")
        return 1

    if action == "subscribe":
        res = sumit.subscribe_trigger(url)
        scope = f"folder {sumit.RECEIPTS_FOLDER_ID} (קבלות) TriggerType=Create"
    else:
        res = sumit.unsubscribe_trigger(url)
        scope = "by URL"

    ok = res.get("Status") == 0
    print(f"{action} {scope}")
    print(f"url: {url}")
    print(f"Status={res.get('Status')} ok={ok} "
          f"err={res.get('UserErrorMessage')!r} data={res.get('Data')!r}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
