import os
import time
import urllib.request
import urllib.parse

# --- brrr alert batching ------------------------------------------------------
#
# Two-tier alerting to kill alert storms (one glitch hitting many branches used
# to fire one brrr per branch):
#
#   1. Routine per-branch failures are BUFFERED during an agent run and sent as
#      ONE digest at the end (batch_flush). Zero failures => nothing is sent.
#   2. Critical/systemic signals (chain-auth fail, BilBoy token expired, whole-
#      run-fail) bypass the buffer and page IMMEDIATELY.
#
# Storm collapse: identical critical alerts (same dedup_key) within a cooldown
# window send only once — so a token-expired error hitting all 18 branches in
# one run pages once, not 18 times.
#
# Usage in an orchestrator that loops over branches:
#     batch_start("Z run")
#     for bid in branches:
#         run_agent(bid)            # agents call notify(...) normally
#     batch_flush()                 # sends one digest (or nothing / systemic)
#
# A routine notify() call OUTSIDE any active batch behaves exactly as before
# (sends immediately) — so manual /ops runs, recovery alerts, etc. are unchanged.

_CRITICAL_COOLDOWN_SEC = 600  # 10 min: suppress identical critical repeats (retries/storms)

# Severity tags prepended to every brrr title so the tier reads in words, not
# just emoji/color. Derived AUTOMATICALLY from how the alert is sent (critical
# flag / digest verb) — never hardcoded at the call site, so it can't drift.
#   🔴 דחוף  (URGENT) — critical=True + whole-run-fail (systemic)
#   🟠 בינוני (MEDIUM) — routine failure digest
#   🟡 מידע   (INFO)   — health/"flagged" digest + plain immediate notices
SEV_URGENT = "🔴 דחוף:"
SEV_MEDIUM = "🟠 בינוני:"
SEV_INFO = "🟡 מידע:"


def _tag(prefix: str, title: str) -> str:
    return f"{prefix} {title}"

# Module-level batch state. Single-process gunicorn + single scheduler process,
# so a simple module global is safe (no cross-process batching needed).
_batch = None                 # dict while a batch is active, else None
_last_critical = {}           # dedup_key -> last-sent monotonic timestamp


def _send(title: str, message: str) -> bool:
    """Low-level brrr send. Honors BRRR_SILENT (staging). Returns True if a real
    send was attempted (or would-be in silent mode), False on config/error."""
    if os.getenv('BRRR_SILENT', 'false').lower() == 'true':
        print(f"[brrr] BRRR_SILENT=true — would send: {title} | {message}")
        return True
    brrr_url = os.getenv('BRRR_URL', '')
    if not brrr_url:
        return False
    try:
        url = f"{brrr_url}?title={urllib.parse.quote(title)}&message={urllib.parse.quote(message)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'MakoletChain/1.0'})
        urllib.request.urlopen(req, timeout=5)
        return True
    except Exception as e:
        print(f"brrr notification failed: {e}")
        return False


def _dedup_ok(key: str) -> bool:
    """True if a critical with this key hasn't fired within the cooldown window."""
    if not key:
        return True
    now = time.monotonic()
    last = _last_critical.get(key)
    if last is not None and (now - last) < _CRITICAL_COOLDOWN_SEC:
        return False
    _last_critical[key] = now
    return True


def notify(title: str, message: str, critical: bool = False, dedup_key: str = None,
           medium: bool = False):
    """Send a brrr push notification to Roei's phone.

    critical=False (default): if a batch is active, buffer for the end-of-run
        digest; otherwise send immediately (legacy behavior).
    critical=True: page immediately, bypassing any active batch. If dedup_key is
        given, identical criticals within the cooldown window are suppressed
        (collapses a per-branch storm to one page).
    medium=True: immediate standalone send at the MEDIUM tier — for operational
        alerts whose urgency sits between info and critical (billing
        locks-tomorrow, sync failed after retry). The one deliberate exception
        to "severity derives from how the alert is sent": these are single-shot
        alerts, not digests, but INFO would undersell them.
    """
    if critical:
        if not _dedup_ok(dedup_key):
            print(f"[brrr] critical deduped ({dedup_key}): {title}")
            return
        _send(_tag(SEV_URGENT, title), message)
        return

    if medium:
        _send(_tag(SEV_MEDIUM, title), message)
        return

    if _batch is not None:
        # Buffer raw title; the severity tag is applied to the digest at flush.
        _batch['failures'].append((title, message))
        return

    # Plain immediate notice (no batch): not a failure digest, not critical →
    # lowest tier. Covers recovery / manual-run / IEC-refresh style alerts.
    _send(_tag(SEV_INFO, title), message)


def batch_start(label: str, total: int = None, verb: str = "failed"):
    """Begin buffering routine notify() calls for an end-of-run digest.

    label: short run name shown in the digest ("Z run", "Nightly sync", ...).
    total: optional branch count, used to detect whole-run-fail at flush.
    verb:  digest count wording — "failed" for hard failures (default), or e.g.
           "flagged" for warning-only runs (health checks) that never escalate.
    """
    global _batch
    _batch = {'label': label, 'total': total, 'verb': verb, 'failures': []}


def batch_flush(failed: int = None):
    """Send the end-of-run digest and end the batch.

    failed: number of DISTINCT branches that failed this run (the orchestrator
        knows this from per-branch result dicts). Used only for the systemic
        check — pass None to skip it.

    - Empty buffer            => send NOTHING (a fully-successful run is silent).
    - failed >= total (>0)    => CRITICAL systemic page (every branch failed).
    - Otherwise               => ONE digest naming the failed branches.

    Buffered entries are deduped (identical title+message collapse to one line,
    e.g. the same agent retried within a run)."""
    global _batch
    if _batch is None:
        return
    label = _batch['label']
    total = _batch['total']
    verb = _batch.get('verb', 'failed')
    # Dedup identical buffered alerts (retries within a run), preserve order.
    seen = set()
    failures = []
    for title, message in _batch['failures']:
        k = (title, message)
        if k in seen:
            continue
        seen.add(k)
        failures.append((title, message))
    _batch = None

    if not failures:
        print(f"[brrr] {label}: 0 alerts — no digest sent")
        return

    # One line per alert: the title already carries the branch name.
    body = "\n".join(f"• {title}: {message}" for title, message in failures)

    if total and failed and failed >= total:
        # Every branch failed — systemic, page immediately as critical (URGENT).
        _send(_tag(SEV_URGENT, f"{label} — SYSTEMIC FAILURE"),
              f"All {total} branches failed.\n{body}")
    else:
        n = len(failures)
        count = f"{n} branch{'es' if n != 1 else ''} {verb}"
        # "flagged" digests are warning-only (never escalate) → INFO; routine
        # failure digests → MEDIUM.
        prefix = SEV_INFO if verb == "flagged" else SEV_MEDIUM
        _send(_tag(prefix, f"{label}: {count}"), body)
