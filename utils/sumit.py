"""
SUMIT (sumit.co.il) READ-ONLY API client — Stage 1 billing.

Reads payments + customers so the billing page can show who paid. It NEVER
writes to SUMIT: a hard allowlist refuses any endpoint outside a small read set,
so a charge / customer-create / document-create / standing-order call cannot be
issued from this module even by mistake.

Auth (confirmed from the official spec at api.sumit.co.il/swagger/v1/swagger.json
and exercised in scripts/sumit_probe.py):
  POST https://api.sumit.co.il/<endpoint>/  with JSON body
  { "Credentials": { "CompanyID": <int SUMIT_ORG_ID>, "APIKey": "<SUMIT_API_KEY>" }, ... }
  Response envelope: { "Status": 0, "UserErrorMessage": ..., "Data": {...} }  (0 == success)

Credentials come from the environment (SUMIT_API_KEY + SUMIT_ORG_ID, loaded from
.env). When they are missing, is_connected() returns False and callers render
"לא מחובר ל-SUMIT" instead of erroring.
"""
import os
import threading

import requests

BASE = "https://api.sumit.co.il"
TIMEOUT = 30

# SUMIT meters API calls (plan actions ×5 = monthly quota, overage billed).
# Every HTTP call through this module bumps a thread-local counter so each
# sync can record exactly what it spent (billing_sync_runs.api_calls).
_tl = threading.local()


def reset_call_count():
    _tl.calls = 0


def call_count():
    return getattr(_tl, "calls", 0)


def _count_call():
    _tl.calls = getattr(_tl, "calls", 0) + 1

# Hard READ-ONLY allowlist. Any endpoint not in this set is refused before the
# HTTP call. Nothing here mutates SUMIT state.
_READ_ONLY_ENDPOINTS = {
    "/website/companies/getdetails/",   # auth check
    "/billing/payments/list/",          # list payments
    "/crm/schema/listfolders/",         # discover the customers folder
    "/crm/data/listentities/",          # list customer entities
    "/crm/data/getentity/",             # read one customer (full props)
    "/accounting/documents/list/",      # list issued documents (receipts)
    "/accounting/documents/getdetails/",  # one document incl. embedded customer
}
# Secondary tripwire: reject anything that smells like a write even if it were
# ever added to the allowlist above by accident.
_WRITE_TOKENS = ("charge", "create", "update", "cancel", "send", "delete",
                 "set", "add", "recurring", "beginredirect", "setforcustomer",
                 "movetobooks", "remark", "onboard")

# CRM folder name that holds customer cards for this SUMIT org.
_CUSTOMERS_FOLDER_NAME = "לקוחות"
# Safety cap so a huge customer base can't turn one sync into thousands of calls.
_MAX_CUSTOMERS = 500


class SumitNotConnected(RuntimeError):
    """Raised when SUMIT_API_KEY / SUMIT_ORG_ID are not configured."""


def _credentials():
    key = os.environ.get("SUMIT_API_KEY")
    org = os.environ.get("SUMIT_ORG_ID")
    if not key or not org:
        return None
    try:
        return {"CompanyID": int(org), "APIKey": key}
    except (TypeError, ValueError):
        return None


def is_connected():
    """True iff SUMIT creds are present and well-formed."""
    return _credentials() is not None


def _post(endpoint, **body):
    """READ-ONLY guarded POST. Refuses any non-allowlisted / write-looking path."""
    creds = _credentials()
    if creds is None:
        raise SumitNotConnected("SUMIT_API_KEY / SUMIT_ORG_ID not configured")
    if endpoint not in _READ_ONLY_ENDPOINTS:
        raise RuntimeError(f"SUMIT client refused non-read endpoint: {endpoint}")
    low = endpoint.lower()
    if any(tok in low for tok in _WRITE_TOKENS):
        raise RuntimeError(f"SUMIT client refused write-looking endpoint: {endpoint}")
    _count_call()
    resp = requests.post(BASE + endpoint, json={"Credentials": creds, **body},
                         headers={"Content-Type": "application/json"}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


# ── Trigger (webhook) management — the ONE sanctioned non-read call ─────────
# /triggers/triggers/subscribe/ registers OUR receiver URL on a CRM folder so
# SUMIT pushes an event when a document is created there; unsubscribe/ (by URL)
# is the undo. Neither touches any financial object — they only manage the
# webhook registration itself. They deliberately do NOT go through _post: the
# read-only allowlist + tripwire above stay exactly as strict as before, and
# this path allows nothing but these two endpoints.
_TRIGGER_ENDPOINTS = {
    "/triggers/triggers/subscribe/",
    "/triggers/triggers/unsubscribe/",
}

# CRM folder "קבלות" (receipts) in this SUMIT org — probed via
# /crm/schema/listfolders/ on 2026-07-05. Hosted-page payments create their
# receipt document here, so Create on this folder == "a payment landed".
RECEIPTS_FOLDER_ID = "2053200638"


def _post_trigger(endpoint, **body):
    creds = _credentials()
    if creds is None:
        raise SumitNotConnected("SUMIT_API_KEY / SUMIT_ORG_ID not configured")
    if endpoint not in _TRIGGER_ENDPOINTS:
        raise RuntimeError(f"SUMIT trigger client refused endpoint: {endpoint}")
    _count_call()
    resp = requests.post(BASE + endpoint, json={"Credentials": creds, **body},
                         headers={"Content-Type": "application/json"}, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def subscribe_trigger(url, folder_id=RECEIPTS_FOLDER_ID, trigger_type="Create"):
    """Register `url` as a webhook on a CRM folder. Returns the raw envelope —
    success is Status == 0; the spec's response Data is an empty object (SUMIT
    returns no subscription id; the URL itself is the handle)."""
    return _post_trigger("/triggers/triggers/subscribe/",
                         URL=url, Folder=str(folder_id), TriggerType=trigger_type)


def unsubscribe_trigger(url):
    """Remove the webhook registration for `url` (the spec keys removal on the
    URL alone — there is no list endpoint and no id)."""
    return _post_trigger("/triggers/triggers/unsubscribe/", URL=url)


def _first(val):
    """SUMIT CRM entity properties come back as 1-element lists; unwrap them."""
    if isinstance(val, list):
        return val[0] if val else None
    return val


def ping():
    """Minimal authenticated read. Returns {'ok', 'company', 'error'}."""
    try:
        data = _post("/website/companies/getdetails/")
    except SumitNotConnected:
        return {"ok": False, "company": None, "error": "not_connected"}
    except Exception as e:  # network / HTTP / JSON
        return {"ok": False, "company": None, "error": str(e)}
    if data.get("Status") != 0:
        return {"ok": False, "company": None,
                "error": data.get("UserErrorMessage") or "auth_failed"}
    company = (data.get("Data") or {}).get("Company") or {}
    return {"ok": True, "company": company.get("Name"), "error": None}


def list_payments(since):
    """List payments from `since` (YYYY-MM-DD) to today. Read-only.

    Returns the raw SUMIT payment dicts (ID, CustomerID, Date, Amount,
    ValidPayment, Status, ...). Raises SumitNotConnected if creds missing.
    """
    from datetime import datetime, timedelta, timezone
    # SUMIT treats Date_To as a midnight cutoff; payments carry real
    # timestamps, so SAME-DAY payments fall after it and vanish (proven with
    # the live ₪1 test, 2026-07-02). Send tomorrow so today is included.
    to = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    data = _post("/billing/payments/list/", Date_From=since, Date_To=to, StartIndex=0)
    if data.get("Status") != 0:
        raise RuntimeError(data.get("UserErrorMessage") or "payments list failed")
    return ((data.get("Data") or {}).get("Payments")) or []


def list_documents(since):
    """List issued documents (receipts/invoices) from `since` (YYYY-MM-DD) to
    today. Read-only. Returns the raw SUMIT document dicts (DocumentID,
    DocumentNumber, Date, DocumentValue, CustomerID, CustomerName,
    ExternalReference, ...)."""
    from datetime import datetime, timedelta, timezone
    # Same midnight-cutoff guard as list_payments (document dates are
    # midnight-stamped so today usually works, but don't rely on it).
    to = (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%d")
    data = _post("/accounting/documents/list/", DateFrom=since, DateTo=to,
                 IncludeDrafts=False)
    if data.get("Status") != 0:
        raise RuntimeError(data.get("UserErrorMessage") or "documents list failed")
    return ((data.get("Data") or {}).get("Documents")) or []


def get_document(document_id):
    """One document's full detail, incl. the embedded Customer object
    (Name / EmailAddress / ExternalIdentifier). Read-only."""
    data = _post("/accounting/documents/getdetails/", DocumentID=document_id)
    if data.get("Status") != 0:
        raise RuntimeError(data.get("UserErrorMessage") or "document detail failed")
    return (data.get("Data") or {}).get("Document") or {}


def _customers_folder_id():
    data = _post("/crm/schema/listfolders/")
    folders = next((v for v in (data.get("Data") or {}).values()
                    if isinstance(v, list)), [])
    # Prefer an exact name match ('לקוחות'); never the credit-cards folder.
    for f in folders:
        if isinstance(f, dict) and (f.get("Name") or "") == _CUSTOMERS_FOLDER_NAME:
            return f.get("ID")
    return None


def list_customers():
    """List customer cards with their external identifier + email. Read-only.

    Returns [{'id', 'name', 'email', 'external_identifier'}]. The external
    identifier is what a payment maps to (== manager_billing.sumit_tag).
    Raises SumitNotConnected if creds missing; returns [] if the customers
    folder can't be found.
    """
    folder = _customers_folder_id()
    if folder is None:
        return []
    listing = _post("/crm/data/listentities/", Folder=folder)
    rows = next((v for v in (listing.get("Data") or {}).values()
                 if isinstance(v, list)), [])
    out = []
    for r in rows[:_MAX_CUSTOMERS]:
        cid = r.get("ID") if isinstance(r, dict) else None
        if cid is None:
            continue
        try:
            det = _post("/crm/data/getentity/", Folder=folder, ID=cid)
        except Exception:
            continue
        ent = (det.get("Data") or {}).get("Entity") or (det.get("Data") or {})
        out.append({
            "id": ent.get("ID", cid),
            "name": _first(ent.get("Customers_FullName")),
            "email": _first(ent.get("Customers_EmailAddress")),
            "external_identifier": _first(ent.get("Customers_ExternalIdentifier")),
        })
    return out
