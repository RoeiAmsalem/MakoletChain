"""READ-ONLY: compare staging z_report_902 (chain-account BI 902 PDF parse)
against prod daily_sales (Gmail-Z, the authoritative source the UI uses).

Both DBs opened mode=ro. No writes, no Aviv calls.
"""
import sqlite3
import sys
from pathlib import Path

PROD_DB = "/opt/makolet-chain/db/makolet_chain.db"
STAGING_DB = "/opt/makolet-chain-staging/db/makolet_chain.db"
BRANCHES = (126, 127)
MATCH_TOL = 1.0   # ≤ ₪1 → match


def ro(path: str) -> sqlite3.Connection:
    if not Path(path).exists():
        raise FileNotFoundError(path)
    c = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def main() -> int:
    prod = ro(PROD_DB)
    staging = ro(STAGING_DB)

    print(f"staging 902 source : {STAGING_DB}  (z_report_902, chain account)")
    print(f"authoritative      : {PROD_DB}  (daily_sales source='z_report' — Gmail-Z)\n")

    overall = {'match': 0, 'mismatch': 0, 'no_compare': 0}
    max_delta = 0.0

    for bid in BRANCHES:
        stg = staging.execute(
            "SELECT date, z_number, amount FROM z_report_902 "
            "WHERE branch_id=? AND z_number IS NOT NULL ORDER BY date",
            (bid,)).fetchall()
        if not stg:
            print(f"=== branch {bid}: no z_report_902 rows on staging ===\n")
            continue

        # All dates we have on staging — fetch prod daily_sales for those dates.
        dates = [r['date'] for r in stg]
        placeholders = ','.join('?' * len(dates))
        prod_rows = prod.execute(
            f"SELECT date, amount FROM daily_sales "
            f"WHERE branch_id=? AND date IN ({placeholders})",
            (bid, *dates)).fetchall()
        prod_by_date = {r['date']: float(r['amount']) for r in prod_rows}

        print("=" * 80)
        print(f"branch {bid} — {len(stg)} staging z_report_902 row(s)")
        print("=" * 80)
        hdr = f"  {'date':<12}{'z#':>6}{'stg_902':>12}{'gmail_Z':>12}{'Δ':>10}  verdict"
        print(hdr); print("  " + "-" * (len(hdr) - 2))

        n_match = n_mismatch = n_no = 0
        branch_max = 0.0
        mismatch_dates: list[str] = []

        for r in stg:
            d = r['date']
            s_amt = float(r['amount'])
            p_amt = prod_by_date.get(d)
            if p_amt is None:
                v = "no comparison (no Gmail-Z row)"
                d_s = "—"
                p_s = "—"
                n_no += 1
            else:
                delta = s_amt - p_amt
                d_s = f"{delta:+,.2f}"
                p_s = f"{p_amt:,.2f}"
                if abs(delta) <= MATCH_TOL:
                    v = "MATCH ✅"
                    n_match += 1
                else:
                    v = "MISMATCH ⚠️"
                    n_mismatch += 1
                    mismatch_dates.append(d)
                branch_max = max(branch_max, abs(delta))
            print(f"  {d:<12}{r['z_number']:>6}{s_amt:>12,.2f}{p_s:>12}{d_s:>10}  {v}")

        print()
        print(f"  branch {bid} summary: {n_match} match · {n_mismatch} mismatch · "
              f"{n_no} no-compare · max |Δ| = ₪{branch_max:,.2f}"
              + (f" · mismatch dates: {mismatch_dates}" if mismatch_dates else ""))
        print()

        overall['match'] += n_match
        overall['mismatch'] += n_mismatch
        overall['no_compare'] += n_no
        max_delta = max(max_delta, branch_max)

    print("=" * 80)
    print(f"OVERALL: {overall['match']} match · {overall['mismatch']} mismatch · "
          f"{overall['no_compare']} no-compare · max |Δ| = ₪{max_delta:,.2f}")
    print("read-only: both DBs opened mode=ro, no writes, no Aviv calls.")
    prod.close(); staging.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
