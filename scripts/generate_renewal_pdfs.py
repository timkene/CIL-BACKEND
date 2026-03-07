"""
generate_renewal_pdfs.py
=========================
Weekly batch: generate renewal analysis PDFs for clients expiring in <=90 days.

Rules:
  - Skips clients already generated this ISO week (no duplicates)
  - Uploads PDFs to Supabase Storage bucket: renewal-pdfs
  - Records metadata in Supabase table: renewal_reports
  - Auto-deletes records + files older than 120 days

Run:
    python scripts/generate_renewal_pdfs.py

Env vars required:
    MOTHERDUCK_TOKEN
    SUPABASE_URL
    SUPABASE_KEY
    ANTHROPIC_API_KEY  (optional — skips AI narratives if missing)

Supabase setup (run once in Supabase SQL Editor):
    CREATE TABLE renewal_reports (
        id              BIGSERIAL PRIMARY KEY,
        group_id        BIGINT       NOT NULL,
        group_name      TEXT         NOT NULL,
        contract_start  DATE         NOT NULL,
        contract_end    DATE         NOT NULL,
        days_to_expiry  INT,
        pdf_path        TEXT,
        pdf_url         TEXT,
        generated_at    TIMESTAMPTZ  DEFAULT NOW(),
        expires_at      TIMESTAMPTZ,
        week_number     INT,
        week_year       INT,
        UNIQUE (group_id, week_number, week_year)
    );

    -- Enable Row Level Security (optional but recommended)
    ALTER TABLE renewal_reports ENABLE ROW LEVEL SECURITY;
    CREATE POLICY "Allow anon read" ON renewal_reports FOR SELECT USING (true);

    -- Storage: create bucket 'renewal-pdfs' as PUBLIC in Supabase dashboard
"""

import os
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import duckdb
from supabase import create_client, Client

# ── Config ────────────────────────────────────────────────────────────────────
MOTHERDUCK_TOKEN  = os.getenv("MOTHERDUCK_TOKEN", "")
MOTHERDUCK_DSN    = f"md:ai_driven_data?motherduck_token={MOTHERDUCK_TOKEN}"
SUPABASE_URL      = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY      = os.getenv("SUPABASE_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

BUCKET = "renewal-pdfs"
TABLE  = "renewal_reports"

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── Import AI client modules ──────────────────────────────────────────────────
from apis.ai_client.data_collector import collect_data
from apis.ai_client.narrator import generate_all_narratives
from apis.ai_client.pdf_generator import generate_pdf


# ── Helpers ───────────────────────────────────────────────────────────────────
def current_iso_week() -> tuple[int, int]:
    """Return (week_number, week_year) for today."""
    iso = date.today().isocalendar()
    return iso[1], iso[0]


def already_generated_this_week(week_num: int, week_year: int) -> set:
    """Return set of group_ids already generated this ISO week."""
    res = (
        sb.table(TABLE)
        .select("group_id")
        .eq("week_number", week_num)
        .eq("week_year", week_year)
        .execute()
    )
    return {r["group_id"] for r in (res.data or [])}


def get_expiring_contracts() -> list:
    """Fetch active contracts expiring in 0–90 days from MotherDuck."""
    con = duckdb.connect(MOTHERDUCK_DSN)
    rows = con.execute("""
        SELECT groupid, groupname,
               startdate::DATE, enddate::DATE,
               DATEDIFF('day', CURRENT_DATE, enddate) AS days_left
        FROM "AI DRIVEN DATA".GROUP_CONTRACT
        WHERE iscurrent = 1
          AND DATEDIFF('day', CURRENT_DATE, enddate) BETWEEN 0 AND 90
        ORDER BY enddate
    """).fetchall()
    con.close()
    return rows


def get_prev_contract(group_id: int, current_start: date):
    """Return (prev_start, prev_end) for the immediately preceding contract, or None."""
    con = duckdb.connect(MOTHERDUCK_DSN)
    row = con.execute("""
        SELECT startdate::DATE, enddate::DATE
        FROM "AI DRIVEN DATA".GROUP_CONTRACT
        WHERE groupid = ?
          AND enddate < ?
        ORDER BY enddate DESC
        LIMIT 1
    """, [group_id, current_start]).fetchone()
    con.close()
    return row


def delete_expired() -> int:
    """Delete renewal_reports rows (+ storage files) older than 120 days."""
    cutoff = (datetime.utcnow() - timedelta(days=120)).isoformat()
    res = sb.table(TABLE).select("id, pdf_path").lt("generated_at", cutoff).execute()
    expired = res.data or []
    for r in expired:
        if r.get("pdf_path"):
            try:
                sb.storage.from_(BUCKET).remove([r["pdf_path"]])
            except Exception:
                pass
        sb.table(TABLE).delete().eq("id", r["id"]).execute()
    return len(expired)


def upload_pdf(pdf_bytes: bytes, filename: str) -> str:
    """Upload PDF bytes to Supabase Storage; return public URL."""
    sb.storage.from_(BUCKET).upload(
        filename,
        pdf_bytes,
        {"content-type": "application/pdf", "upsert": "true"},
    )
    return sb.storage.from_(BUCKET).get_public_url(filename)


def save_record(
    group_id, group_name, contract_start, contract_end,
    days_left, pdf_path, pdf_url, week_num, week_year,
):
    now = datetime.utcnow().isoformat()
    expires = (datetime.utcnow() + timedelta(days=120)).isoformat()
    sb.table(TABLE).upsert(
        {
            "group_id":      group_id,
            "group_name":    group_name,
            "contract_start": str(contract_start),
            "contract_end":   str(contract_end),
            "days_to_expiry": days_left,
            "pdf_path":       pdf_path,
            "pdf_url":        pdf_url,
            "generated_at":   now,
            "expires_at":     expires,
            "week_number":    week_num,
            "week_year":      week_year,
        },
        on_conflict="group_id,week_number,week_year",
    ).execute()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*60}")
    print(f"  Renewal PDF Batch  —  {date.today()}")
    print(f"{'='*60}")

    week_num, week_year = current_iso_week()
    print(f"  ISO Week: {week_year}-W{week_num:02d}")

    # 1. Delete expired records
    print("\n[1/4] Cleaning up records older than 120 days...")
    deleted = delete_expired()
    print(f"  Removed {deleted} expired records")

    # 2. Find contracts expiring in <=90 days
    print("\n[2/4] Fetching contracts expiring in <=90 days...")
    contracts = get_expiring_contracts()
    print(f"  Found {len(contracts)} expiring contracts")

    # 3. Filter out already-generated this week
    done_ids = already_generated_this_week(week_num, week_year)
    print(f"  Already generated this week: {len(done_ids)} clients")
    to_process = [c for c in contracts if c[0] not in done_ids]
    print(f"  To generate: {len(to_process)} clients")

    # 4. Generate PDFs
    print("\n[3/4] Generating PDFs...")
    success = failed = 0

    for group_id, group_name, start_date, end_date, days_left in to_process:
        print(f"\n  -> {group_name}  (expires {end_date}, {days_left} days)")
        try:
            # Previous contract dates
            prev = get_prev_contract(group_id, start_date)
            if prev:
                prev_start, prev_end = prev
            else:
                # Estimate: 1-year lookback
                prev_start = date(start_date.year - 1, start_date.month, start_date.day)
                prev_end   = date(end_date.year - 1,   end_date.month,   end_date.day)
                print(f"    (no previous contract found — using estimated prior year)")

            # Collect data from MotherDuck
            data = collect_data(
                group_name,
                str(start_date), str(end_date),
                str(prev_start), str(prev_end),
                MOTHERDUCK_DSN,  # passed as db_path — bypasses read_only restriction
            )

            # AI narratives
            if ANTHROPIC_API_KEY:
                narratives = generate_all_narratives(data)
            else:
                narratives = {}
                print(f"    (ANTHROPIC_API_KEY not set — skipping AI narratives)")

            # Generate PDF bytes
            pdf_bytes = generate_pdf(data, narratives)

            # Upload to Supabase Storage
            safe = "".join(c if c.isalnum() else "_" for c in group_name)[:40]
            filename = f"{safe}_{week_year}_W{week_num:02d}.pdf"
            pdf_url = upload_pdf(pdf_bytes, filename)

            # Save metadata to Supabase
            save_record(
                group_id, group_name, start_date, end_date,
                days_left, filename, pdf_url, week_num, week_year,
            )

            print(f"    Done — {len(pdf_bytes):,} bytes uploaded")
            success += 1

        except Exception as e:
            print(f"    FAILED: {e}")
            traceback.print_exc()
            failed += 1

    print(f"\n[4/4] Done: {success} generated, {failed} failed, {len(done_ids)} skipped")
    print(f"{'='*60}\n")

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
