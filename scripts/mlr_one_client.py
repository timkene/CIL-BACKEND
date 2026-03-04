#!/usr/bin/env python3
"""Print MLR for one client using the new basis (encounterdatefrom + requestdate in contract)."""
import os
import sys

# Optional: add project root for core.database; fallback to duckdb-only
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
DB_PATH = os.getenv("DUCKDB_PATH") or os.path.join(PROJECT_DIR, "ai_driven_data.duckdb")

try:
    import duckdb
except ImportError:
    print("Install duckdb: pip install duckdb pandas", file=sys.stderr)
    sys.exit(1)
import pandas as pd

CLIENT_NAME = "LORNA NIGERIA LIMITED (GODREJ)"

def main():
    if not os.path.isfile(DB_PATH):
        print(f"Database not found: {DB_PATH}", file=sys.stderr)
        sys.exit(1)
    conn = duckdb.connect(DB_PATH, read_only=True)
    # Get current contract for client
    contract = conn.execute("""
        SELECT groupid, groupname, startdate, enddate
        FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
        WHERE groupname = ? AND iscurrent = 1
    """, [CLIENT_NAME]).fetchdf()
    if contract.empty:
        print(f"Client '{CLIENT_NAME}' not found or no current contract.")
        return
    row = contract.iloc[0]
    groupid = str(row["groupid"])
    groupname = row["groupname"]
    start = pd.Timestamp(row["startdate"]).strftime("%Y-%m-%d")
    end = pd.Timestamp(row["enddate"]).strftime("%Y-%m-%d")

    # Claims: encounterdatefrom in contract
    claims_df = conn.execute(f"""
        SELECT COALESCE(SUM(approvedamount), 0) as total
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE nhisgroupid = '{groupid}'
          AND encounterdatefrom >= DATE '{start}'
          AND encounterdatefrom <= DATE '{end}'
    """).fetchdf()
    claims_amount = float(claims_df.iloc[0]["total"])

    def norm_panumber(x):
        """Normalize panumber so 12345.0 and '12345' match (avoid double-counting)."""
        if pd.isna(x):
            return None
        s = str(x).strip()
        if not s:
            return None
        try:
            return str(int(float(s)))
        except (ValueError, TypeError):
            return s

    # Claimed panumbers (encounter in contract) — normalize to match PA table format
    claimed = conn.execute(f"""
        SELECT DISTINCT panumber as p
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE nhisgroupid = '{groupid}'
          AND panumber IS NOT NULL
          AND encounterdatefrom >= DATE '{start}'
          AND encounterdatefrom <= DATE '{end}'
    """).fetchdf()
    claimed_set = {norm_panumber(v) for v in claimed["p"].tolist() if norm_panumber(v)}

    # Unclaimed PA: requestdate in contract, panumber NOT already in claims (truly unclaimed only)
    pa_df = conn.execute(f"""
        SELECT panumber, granted
        FROM "AI DRIVEN DATA"."PA DATA"
        WHERE groupname = ?
          AND requestdate >= TIMESTAMP '{start}'
          AND requestdate <= TIMESTAMP '{end}'
          AND panumber IS NOT NULL AND granted > 0
    """, [groupname]).fetchdf()
    unclaimed = 0.0
    for _, r in pa_df.iterrows():
        p = norm_panumber(r["panumber"])
        if p and p not in claimed_set:
            unclaimed += float(r["granted"])

    # Debit (contract period, exclude TPA)
    debit_df = conn.execute(f"""
        SELECT COALESCE(SUM(Amount), 0) as total
        FROM "AI DRIVEN DATA"."DEBIT_NOTE"
        WHERE CompanyName = ?
          AND "From" >= DATE '{start}' AND "From" <= DATE '{end}'
          AND (Description IS NULL OR LOWER(TRIM(CAST(Description AS VARCHAR))) NOT LIKE '%tpa%')
    """, [groupname]).fetchdf()
    debit_amount = float(debit_df.iloc[0]["total"])

    total_medical = claims_amount + unclaimed
    mlr_pct = (total_medical / debit_amount * 100) if debit_amount > 0 else None

    print(f"{CLIENT_NAME} — MLR (new basis)")
    print("=" * 55)
    print(f"  Contract: {start} → {end}")
    print(f"  Claims (encounter in contract):     ₦{claims_amount:,.2f}")
    print(f"  Unclaimed PA (request in contract): ₦{unclaimed:,.2f}")
    print(f"  Total medical cost:                 ₦{total_medical:,.2f}")
    print(f"  Total debit (contract):             ₦{debit_amount:,.2f}")
    print(f"  MLR (%):                            {mlr_pct:.2f}%" if mlr_pct is not None else "  MLR (%):                            N/A (no debit)")
    print("=" * 55)

if __name__ == "__main__":
    main()
