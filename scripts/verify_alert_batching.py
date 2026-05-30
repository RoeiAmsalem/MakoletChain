"""Verify the two-tier brrr batching (utils/notify.py).

Patches _send to record (title, message) instead of sending, so we can assert
exact send counts. Mirrors the real orchestrator pattern: batch_start → per-branch
notify() → batch_flush(failed=N).

    python3 scripts/verify_alert_batching.py
"""
import sys
sys.path.insert(0, '.')
import utils.notify as N

sent = []
N._send = lambda title, msg: sent.append((title, msg)) or True


def reset():
    sent.clear()
    N._batch = None
    N._last_critical.clear()


fails = []


def check(name, cond, detail=""):
    print(f"{'PASS' if cond else 'FAIL'} — {name}" + (f" :: {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(name)


# A. Multi-branch failure → ONE digest naming all failed branches, not N sends.
reset()
N.batch_start("Z run", total=18)
for b in ["טבעון", "לימן", "דפנה"]:
    N.notify(f"❌ Gmail — {b}", "No Z-reports found in the last 7 days.")
N.batch_flush(failed=3)
check("A multi-branch → 1 digest", len(sent) == 1, f"{len(sent)} sends")
check("A digest names all 3 branches",
      all(b in sent[0][1] for b in ["טבעון", "לימן", "דפנה"]) and "3 branches failed" in sent[0][0],
      sent and sent[0])

# B. Critical storm (BilBoy 401 on all branches) → ONE immediate page, deduped.
reset()
N.batch_start("Nightly sync", total=18)
for _ in range(18):
    N.notify("🔑 BilBoy — X", "BilBoy token expired.", critical=True, dedup_key="bilboy_token_expired")
N.batch_flush(failed=0)  # no routine failures buffered
check("B 401 storm → 1 critical page", len(sent) == 1, f"{len(sent)} sends")

# C. All-branches-failed → critical SYSTEMIC page, not a quiet digest.
reset()
N.batch_start("Nightly sync", total=3)
for b in ["A", "B", "C"]:
    N.notify(f"❌ BilBoy — {b}", "error")
N.batch_flush(failed=3)
check("C all-failed → systemic", len(sent) == 1 and "SYSTEMIC" in sent[0][0], sent and sent[0][0])

# D. Fully-successful run → NO notification at all.
reset()
N.batch_start("Z run", total=18)
N.batch_flush(failed=0)
check("D success → silent", len(sent) == 0, f"{len(sent)} sends")

# E. Unchanged immediate alerts (no active batch) → fire individually.
reset()
N.notify("✅ Aviv Live — טבעון", "Back online.")          # recovery
N.notify("✅ z_report", "סניף 126 — ok")                  # manual /ops run
check("E immediate-when-no-batch", len(sent) == 2, f"{len(sent)} sends")

# F. Digest dedup: identical buffered alerts (retries) collapse to one line.
reset()
N.batch_start("Z run", total=18)
N.notify("❌ Gmail — לימן", "err")
N.notify("❌ Gmail — לימן", "err")   # retry, identical
N.notify("❌ Gmail — דפנה", "err")
N.batch_flush(failed=2)
body_lines = sent[0][1].count("•") if sent else 0
check("F retry dedup → 2 lines", len(sent) == 1 and body_lines == 2, f"sends={len(sent)} lines={body_lines}")

# G. Critical fires separately from the digest during the same run.
reset()
N.batch_start("Nightly sync", total=18)
N.notify("🔑 BilBoy — X", "token expired", critical=True, dedup_key="bilboy_token_expired")  # immediate
N.notify("❌ Gmail — לימן", "err")     # buffered
N.notify("❌ Gmail — דפנה", "err")     # buffered
N.batch_flush(failed=2)
crit = any("BilBoy" in t for t, _ in sent)
digest = any("2 branches failed" in t for t, _ in sent)
check("G critical + digest separate", len(sent) == 2 and crit and digest, f"{[t for t,_ in sent]}")

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL PASS")
