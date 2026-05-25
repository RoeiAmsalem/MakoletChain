"""READ-ONLY: compare prod's live_sales (per-branch own creds) vs staging's
live_sales (chain-account multi-branch) for branches 126 + 127.

Both DBs sit on the same host. Opens BOTH read-only via sqlite3 URI
(?mode=ro) so this script can never write either side.

Per-branch verdict accounts for poll-time skew: prod and staging poll on
their own cadence so a few minutes between fetches will naturally produce
small Δamount even when both sides are healthy. The verdict distinguishes
that timing skew from a real "same-time, different-number" mismatch.
"""
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

PROD_DB = "/opt/makolet-chain/db/makolet_chain.db"
STAGING_DB = "/opt/makolet-chain-staging/db/makolet_chain.db"
BRANCHES = (126, 127)

# A diff inside this window is considered "fetch-skew tolerable" if the two
# polls were also far apart. Both knobs together define the three verdicts.
SKEW_FETCH_GAP_MIN = 2.0   # > 2 minutes between polls counts as "different windows"
SMALL_AMOUNT_DIFF = 1.0    # ≤ ₪1 → effectively a match regardless of gap


def ro_connect(path: str) -> sqlite3.Connection:
    if not Path(path).exists():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def latest_live(conn: sqlite3.Connection, branch_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT branch_id, date, amount, transactions, last_updated, fetched_at "
        "FROM live_sales WHERE branch_id=? "
        "ORDER BY fetched_at DESC LIMIT 1",
        (branch_id,),
    ).fetchone()


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        try:
            return datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None


def fmt_ts(ts: str | None) -> str:
    dt = parse_ts(ts)
    return dt.strftime("%H:%M:%S") if dt else "—"


def verdict(d_amount: float, d_tx: int, fetch_gap_min: float | None) -> str:
    if abs(d_amount) <= SMALL_AMOUNT_DIFF and d_tx == 0:
        return "MATCH ✅"
    if fetch_gap_min is None:
        return "DIFF — missing fetched_at on one side"
    if fetch_gap_min >= SKEW_FETCH_GAP_MIN:
        return f"DIFF within poll skew (gap {fetch_gap_min:.1f}m) — likely timing, recheck"
    return "REAL MISMATCH ⚠️ — same time, different number"


def main() -> int:
    try:
        prod = ro_connect(PROD_DB)
        staging = ro_connect(STAGING_DB)
    except FileNotFoundError as e:
        print(f"STOP: db not found: {e}", file=sys.stderr)
        return 2

    print(f"prod    : {PROD_DB}  (read-only)")
    print(f"staging : {STAGING_DB}  (read-only)")
    print(f"now     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    header = (
        f"{'branch':<8}{'prod_amt':>12}{'prod_tx':>8}{'prod_fetch':>14}"
        f"  {'stg_amt':>12}{'stg_tx':>8}{'stg_fetch':>14}"
        f"  {'Δamt':>10}{'Δtx':>6}{'gap(m)':>10}  verdict"
    )
    print(header)
    print("-" * len(header))

    match_count = 0
    attention_count = 0

    for bid in BRANCHES:
        p = latest_live(prod, bid)
        s = latest_live(staging, bid)
        if not p and not s:
            print(f"  {bid:<6}  no live_sales row on either side")
            continue

        p_amt = float(p["amount"]) if p else None
        p_tx = int(p["transactions"]) if p else None
        s_amt = float(s["amount"]) if s else None
        s_tx = int(s["transactions"]) if s else None

        p_ts = parse_ts(p["fetched_at"]) if p else None
        s_ts = parse_ts(s["fetched_at"]) if s else None
        gap_min = None
        if p_ts and s_ts:
            # Strip tz so naïve datetimes are comparable; both are IL local time
            # in practice but only one side writes with tz.
            pn = p_ts.replace(tzinfo=None)
            sn = s_ts.replace(tzinfo=None)
            gap_min = abs((sn - pn).total_seconds()) / 60.0

        if p and s:
            d_amt = s_amt - p_amt
            d_tx = s_tx - p_tx
            v = verdict(d_amt, d_tx, gap_min)
        elif p and not s:
            d_amt = -p_amt
            d_tx = -p_tx
            v = "DIFF — staging has no row"
        else:
            d_amt = s_amt
            d_tx = s_tx
            v = "DIFF — prod has no row"

        if "MATCH" in v:
            match_count += 1
        else:
            attention_count += 1

        prod_amt_s = f"{p_amt:,.2f}" if p_amt is not None else "—"
        prod_tx_s = f"{p_tx}" if p_tx is not None else "—"
        stg_amt_s = f"{s_amt:,.2f}" if s_amt is not None else "—"
        stg_tx_s = f"{s_tx}" if s_tx is not None else "—"
        d_amt_s = f"{d_amt:+,.2f}" if p and s else "—"
        d_tx_s = f"{d_tx:+d}" if p and s else "—"
        gap_s = f"{gap_min:.1f}" if gap_min is not None else "—"

        print(
            f"  {bid:<6}{prod_amt_s:>12}{prod_tx_s:>8}{fmt_ts(p['fetched_at']) if p else '—':>14}"
            f"  {stg_amt_s:>12}{stg_tx_s:>8}{fmt_ts(s['fetched_at']) if s else '—':>14}"
            f"  {d_amt_s:>10}{d_tx_s:>6}{gap_s:>10}  {v}"
        )

    print()
    print(f"summary: {match_count} match, {attention_count} need attention")
    prod.close()
    staging.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
