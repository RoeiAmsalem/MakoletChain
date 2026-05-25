"""READ-ONLY: end-of-day comparison of every aviv_live pull today,
prod vs staging, branches 126 + 127.

For each staging pull, finds the nearest prod pull in time and prints both
side-by-side with Δamount, gap(min), and a skew-aware verdict.
"""
import sqlite3
import sys
from bisect import bisect_left
from datetime import datetime
from pathlib import Path

PROD_DB = "/opt/makolet-chain/db/makolet_chain.db"
STAGING_DB = "/opt/makolet-chain-staging/db/makolet_chain.db"
BRANCHES = (126, 127)

# Same thresholds as compare_live_sources.py.
SMALL_AMOUNT_DIFF = 1.0     # ≤ ₪1 → MATCH regardless of gap
SKEW_FETCH_GAP_MIN = 2.0    # > 2 min apart → tolerable skew, not real mismatch
NO_NEIGHBOR_MIN = 5.0       # no prod pull within ±5 min → no nearby sample


def ro_connect(path: str) -> sqlite3.Connection:
    if not Path(path).exists():
        raise FileNotFoundError(path)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


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


def naive(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None)


def fetch_pulls(conn: sqlite3.Connection, branch_id: int) -> list[tuple[datetime, float, str]]:
    """Return list of (started_at_naive, amount, status) for aviv_live today, sorted asc."""
    rows = conn.execute(
        "SELECT started_at, amount, status "
        "FROM agent_runs "
        "WHERE agent='aviv_live' AND branch_id=? "
        "AND date(started_at) = date('now') "
        "ORDER BY started_at",
        (branch_id,),
    ).fetchall()
    out: list[tuple[datetime, float, str]] = []
    for r in rows:
        dt = parse_ts(r["started_at"])
        if dt is None:
            continue
        out.append((naive(dt), float(r["amount"] or 0), r["status"] or ""))
    return out


def nearest(prod_times: list[datetime], t: datetime) -> int | None:
    """Index in prod_times nearest to t. None if list empty."""
    if not prod_times:
        return None
    i = bisect_left(prod_times, t)
    candidates = []
    if i < len(prod_times):
        candidates.append(i)
    if i > 0:
        candidates.append(i - 1)
    return min(candidates, key=lambda j: abs((prod_times[j] - t).total_seconds()))


def verdict(d_amount: float, gap_min: float | None) -> str:
    if gap_min is None or gap_min > NO_NEIGHBOR_MIN:
        return "no nearby prod sample"
    if abs(d_amount) <= SMALL_AMOUNT_DIFF:
        return "MATCH ✅"
    if gap_min >= SKEW_FETCH_GAP_MIN:
        return f"skew (gap {gap_min:.1f}m)"
    return "REAL MISMATCH ⚠️"


def main() -> int:
    try:
        prod = ro_connect(PROD_DB)
        staging = ro_connect(STAGING_DB)
    except FileNotFoundError as e:
        print(f"STOP: db not found: {e}", file=sys.stderr)
        return 2

    print(f"prod    : {PROD_DB}  (read-only)")
    print(f"staging : {STAGING_DB}  (read-only)")
    print(f"date    : {datetime.now().date()}")
    print(f"now     : {datetime.now().strftime('%H:%M:%S')}\n")

    overall_match = overall_skew = overall_real = overall_no_neighbor = 0
    overall_max_real_delta = 0.0

    for bid in BRANCHES:
        prod_pulls = fetch_pulls(prod, bid)
        stg_pulls = fetch_pulls(staging, bid)
        prod_times = [t for (t, _, _) in prod_pulls]

        print("=" * 92)
        print(f"branch {bid}  —  staging pulls today: {len(stg_pulls)}  |  prod pulls today: {len(prod_pulls)}")
        print("=" * 92)
        if not stg_pulls:
            print("  (no staging pulls today)")
            continue

        header = (
            f"  {'staging_ts':<10}{'stg_amt':>11}  {'prod_ts':<10}{'prod_amt':>11}"
            f"  {'Δamt':>10}  {'gap(m)':>8}  verdict"
        )
        print(header)
        print("  " + "-" * (len(header) - 2))

        n_match = n_skew = n_real = n_no = 0
        max_real_delta = 0.0

        for s_t, s_amt, s_status in stg_pulls:
            idx = nearest(prod_times, s_t)
            if idx is None:
                line = (f"  {s_t.strftime('%H:%M:%S'):<10}{s_amt:>11,.2f}  "
                        f"{'—':<10}{'—':>11}  {'—':>10}  {'—':>8}  no prod data today")
                print(line)
                n_no += 1
                continue

            p_t, p_amt, _ = prod_pulls[idx]
            gap_sec = abs((p_t - s_t).total_seconds())
            gap_min = gap_sec / 60.0
            d_amt = s_amt - p_amt
            v = verdict(d_amt, gap_min)

            print(
                f"  {s_t.strftime('%H:%M:%S'):<10}{s_amt:>11,.2f}  "
                f"{p_t.strftime('%H:%M:%S'):<10}{p_amt:>11,.2f}  "
                f"{d_amt:>+10,.2f}  {gap_min:>8.1f}  {v}"
            )

            # Order matters: check REAL MISMATCH first because the substring
            # "MATCH" is also inside "MISMATCH".
            if "REAL MISMATCH" in v:
                n_real += 1
                max_real_delta = max(max_real_delta, abs(d_amt))
            elif "MATCH" in v:
                n_match += 1
            elif "skew" in v:
                n_skew += 1
            else:
                n_no += 1

        print()
        print(f"  branch {bid} summary: {n_match} match · {n_skew} skew · "
              f"{n_real} REAL MISMATCH · {n_no} no-nearby"
              + (f" · max same-time Δ = ₪{max_real_delta:,.2f}" if n_real else ""))
        print()

        overall_match += n_match
        overall_skew += n_skew
        overall_real += n_real
        overall_no_neighbor += n_no
        overall_max_real_delta = max(overall_max_real_delta, max_real_delta)

    print("=" * 92)
    print(f"OVERALL: {overall_match} match · {overall_skew} skew · "
          f"{overall_real} REAL MISMATCH · {overall_no_neighbor} no-nearby"
          + (f" · max same-time Δ = ₪{overall_max_real_delta:,.2f}" if overall_real else ""))
    print("read-only: both DBs opened mode=ro, no writes, no Aviv calls.")
    prod.close()
    staging.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
