"""
seed_mlr_to_supabase.py
========================
Reads all active contracts from local DuckDB, computes MLR for each,
and upserts results into Supabase PostgreSQL.

Run:
    python scripts/seed_mlr_to_supabase.py

Set env vars or use .env:
    DUCKDB_PATH   = path to ai_driven_data.duckdb
    SUPABASE_URL  = https://xxxx.supabase.co
    SUPABASE_KEY  = service_role key
"""

import os, sys, json, time, traceback
from datetime import date, datetime, timezone
from pathlib import Path

# ── allow running from project root ─────────────────────────────────────────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import duckdb
from supabase import create_client, Client

# ── config ───────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DSN   = f"md:ai_driven_data?motherduck_token={MOTHERDUCK_TOKEN}"
SUPABASE_URL     = os.getenv("SUPABASE_URL", "https://zxxkcvkrpdsvfljrjgqy.supabase.co")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inp4eGtjdmtycGRzdmZsanJqZ3F5Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mjc0NjM4MiwiZXhwIjoyMDg4MzIyMzgyfQ.nBagWuf_0VhJB5O9CRIBNQyGLLwo236uvjcHei5cG1U")

# ── Supabase client ──────────────────────────────────────────────────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── import MLR service ───────────────────────────────────────────────────────
from apis.mlr.service import compute_mlr_summary


# ── create tables in Supabase ────────────────────────────────────────────────
SCHEMA_SQL = """
-- MLR summary per client per contract period
CREATE TABLE IF NOT EXISTS mlr_summary (
    id                              BIGSERIAL PRIMARY KEY,
    group_id                        BIGINT,
    group_name                      TEXT        NOT NULL,
    start_date                      DATE        NOT NULL,
    end_date                        DATE        NOT NULL,

    -- Premium
    total_debit_amount              NUMERIC(18,2),

    -- Medical costs
    actual_claims_cost              NUMERIC(18,2),
    unclaimed_pa_cost               NUMERIC(18,2),
    total_actual_medical_cost       NUMERIC(18,2),
    claims_paid_cost                NUMERIC(18,2),

    -- MLR
    actual_mlr                      NUMERIC(8,4),
    actual_mlr_pct                  TEXT,
    claims_paid_mlr                 NUMERIC(8,4),
    claims_paid_mlr_pct             TEXT,
    mlr_status                      TEXT,       -- PROFITABLE | WARNING | LOSS

    -- PMPM
    enrolled_members                INTEGER,
    contract_months                 INTEGER,
    elapsed_months                  INTEGER,
    member_months                   INTEGER,
    actual_medical_cost_pmpm        NUMERIC(18,2),
    claims_paid_medical_cost_pmpm   NUMERIC(18,2),
    premium_pmpm                    NUMERIC(18,2),

    -- metadata
    computed_at                     TIMESTAMPTZ DEFAULT NOW(),
    had_error                       BOOLEAN     DEFAULT FALSE,
    error_message                   TEXT,

    UNIQUE (group_id, start_date, end_date)
);

-- Top providers per client per period
CREATE TABLE IF NOT EXISTS mlr_top_providers (
    id              BIGSERIAL PRIMARY KEY,
    summary_id      BIGINT      REFERENCES mlr_summary(id) ON DELETE CASCADE,
    group_id        BIGINT,
    start_date      DATE,
    end_date        DATE,
    rank_by         TEXT,       -- 'cost' or 'count'
    rank            INTEGER,
    provider_id     TEXT,
    provider_name   TEXT,
    visit_count     INTEGER,
    claim_rows      INTEGER,
    total_cost      NUMERIC(18,2),
    pct_of_total    NUMERIC(8,2)
);

-- Top enrollees per client per period
CREATE TABLE IF NOT EXISTS mlr_top_enrollees (
    id              BIGSERIAL PRIMARY KEY,
    summary_id      BIGINT      REFERENCES mlr_summary(id) ON DELETE CASCADE,
    group_id        BIGINT,
    start_date      DATE,
    end_date        DATE,
    rank_by         TEXT,
    rank            INTEGER,
    enrollee_id     TEXT,
    enrollee_name   TEXT,
    visit_count     INTEGER,
    claim_rows      INTEGER,
    total_cost      NUMERIC(18,2),
    pct_of_total    NUMERIC(8,2)
);

-- Top procedures per client per period
CREATE TABLE IF NOT EXISTS mlr_top_procedures (
    id              BIGSERIAL PRIMARY KEY,
    summary_id      BIGINT      REFERENCES mlr_summary(id) ON DELETE CASCADE,
    group_id        BIGINT,
    start_date      DATE,
    end_date        DATE,
    rank_by         TEXT,
    rank            INTEGER,
    procedure_code  TEXT,
    procedure_desc  TEXT,
    claim_count     INTEGER,
    total_cost      NUMERIC(18,2),
    pct_of_total    NUMERIC(8,2)
);
"""


def create_tables():
    """Run schema SQL via Supabase rpc or direct REST."""
    print("Creating tables via Supabase SQL editor REST API...")
    import requests
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }
    url = f"{SUPABASE_URL}/rest/v1/rpc/exec_sql"
    # Try the pg_dump approach — use management API
    # Supabase doesn't expose raw SQL via REST; user must run SCHEMA_SQL in dashboard.
    # We'll print it and skip.
    print("\n" + "="*60)
    print("PASTE THIS SQL INTO: Supabase → SQL Editor → New Query → Run")
    print("="*60)
    print(SCHEMA_SQL)
    print("="*60)
    input("\nPress ENTER once you've run the SQL in Supabase dashboard... ")


def load_active_contracts():
    con = duckdb.connect(MOTHERDUCK_DSN)
    rows = con.execute("""
        SELECT groupid, groupname, startdate::DATE, enddate::DATE
        FROM "AI DRIVEN DATA".GROUP_CONTRACT
        WHERE iscurrent = 1
        ORDER BY groupname
    """).fetchall()
    con.close()
    return rows   # list of (groupid, groupname, start_date, end_date)


def upsert_summary(group_id, group_name, start_date, end_date, result=None, error=None):
    """Upsert one row into mlr_summary. Returns the row id."""
    row = {
        "group_id":   group_id,
        "group_name": group_name,
        "start_date": str(start_date),
        "end_date":   str(end_date),
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        row["had_error"]      = True
        row["error_message"]  = str(error)[:500]
    else:
        r = result
        row.update({
            "total_debit_amount":              r.total_debit_amount,
            "actual_claims_cost":              r.actual_claims_cost,
            "unclaimed_pa_cost":               r.unclaimed_pa_cost,
            "total_actual_medical_cost":       r.total_actual_medical_cost,
            "claims_paid_cost":                r.claims_paid_cost,
            "actual_mlr":                      float(r.actual_mlr),
            "actual_mlr_pct":                  r.actual_mlr_pct,
            "claims_paid_mlr":                 float(r.claims_paid_mlr),
            "claims_paid_mlr_pct":             r.claims_paid_mlr_pct,
            "mlr_status":                      r.mlr_status,
            "enrolled_members":                r.enrolled_members,
            "utilized_members":               r.utilized_members,
            "member_utilization_pct":         r.member_utilization_pct,
            "contract_months":                 r.contract_months,
            "elapsed_months":                  r.elapsed_months,
            "member_months":                   r.member_months,
            "actual_medical_cost_pmpm":        r.actual_medical_cost_pmpm,
            "claims_paid_medical_cost_pmpm":   r.claims_paid_medical_cost_pmpm,
            "premium_pmpm":                    r.premium_pmpm,
            "had_error":                       False,
            "error_message":                   None,
        })

    resp = sb.table("mlr_summary").upsert(
        row, on_conflict="group_id,start_date,end_date"
    ).execute()
    return resp.data[0]["id"] if resp.data else None


def insert_top_rows(table, summary_id, group_id, start_date, end_date, rank_by, rows, mapper):
    if not rows:
        return
    # Delete existing for this summary + rank_by before re-inserting
    sb.table(table).delete().eq("summary_id", summary_id).eq("rank_by", rank_by).execute()
    records = [
        dict(summary_id=summary_id, group_id=group_id,
             start_date=str(start_date), end_date=str(end_date),
             rank_by=rank_by, **mapper(r))
        for r in rows
    ]
    sb.table(table).insert(records).execute()


def provider_map(r):
    return dict(rank=r.rank, provider_id=r.provider_id, provider_name=r.provider_name,
                visit_count=r.visit_count, claim_rows=r.claim_rows,
                total_cost=r.total_cost, pct_of_total=r.pct_of_total)

def enrollee_map(r):
    return dict(rank=r.rank, enrollee_id=r.enrollee_id, enrollee_name=r.enrollee_name,
                visit_count=r.visit_count, claim_rows=r.claim_rows,
                total_cost=r.total_cost, pct_of_total=r.pct_of_total)

def procedure_map(r):
    return dict(rank=r.rank, procedure_code=r.procedure_code, procedure_desc=r.procedure_desc,
                claim_count=r.claim_count, total_cost=r.total_cost, pct_of_total=r.pct_of_total)


def main():
    # create_tables() is a one-time setup; tables already exist in Supabase
    contracts = load_active_contracts()
    total = len(contracts)
    print(f"\nProcessing {total} active contracts...\n")

    success = 0
    failed  = 0
    skipped = 0

    for i, (group_id, group_name, start_date, end_date) in enumerate(contracts, 1):
        label = f"[{i:>3}/{total}] {group_name.strip()}"
        try:
            result = compute_mlr_summary(
                client_name=group_name.strip(),
                start_date=start_date,
                end_date=end_date,
            )

            # Skip if no debit (contract exists but no billing data yet)
            if result.total_debit_amount == 0:
                print(f"  SKIP  {label}  (no debit data)")
                skipped += 1
                upsert_summary(group_id, group_name.strip(), start_date, end_date, result)
                continue

            summary_id = upsert_summary(group_id, group_name.strip(), start_date, end_date, result)

            if summary_id:
                gid, sd, ed = group_id, start_date, end_date
                insert_top_rows("mlr_top_providers",  summary_id, gid, sd, ed, "cost",  result.top_10_providers_by_cost,   provider_map)
                insert_top_rows("mlr_top_providers",  summary_id, gid, sd, ed, "count", result.top_10_providers_by_count,  provider_map)
                insert_top_rows("mlr_top_enrollees",  summary_id, gid, sd, ed, "cost",  result.top_10_enrollees_by_cost,   enrollee_map)
                insert_top_rows("mlr_top_enrollees",  summary_id, gid, sd, ed, "count", result.top_10_enrollees_by_count,  enrollee_map)
                insert_top_rows("mlr_top_procedures", summary_id, gid, sd, ed, "cost",  result.top_10_procedures_by_cost,  procedure_map)
                insert_top_rows("mlr_top_procedures", summary_id, gid, sd, ed, "count", result.top_10_procedures_by_count, procedure_map)

            print(f"  OK    {label}  MLR={result.actual_mlr_pct}  Debit=₦{result.total_debit_amount:,.0f}  [{result.mlr_status}]")
            success += 1

        except Exception as e:
            print(f"  ERROR {label}  → {e}")
            upsert_summary(group_id, group_name.strip(), start_date, end_date, error=e)
            failed += 1

        # small pause to avoid hammering DuckDB on 283 sequential queries
        time.sleep(0.05)

    print(f"\n{'='*60}")
    print(f"  Done.  ✓ {success}  skipped {skipped}  ✗ {failed}  of {total}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
