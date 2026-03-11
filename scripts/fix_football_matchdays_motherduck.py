#!/usr/bin/env python3
"""
Fix FOOTBALL.matchdays in MotherDuck: diagnose duplicate ids and fix.
Run from DLT root with MOTHERDUCK_TOKEN set (or in .env).

  python scripts/fix_football_matchdays_motherduck.py

Steps:
  1. Show current matchdays (id, sunday_date, status, voting_opens_at)
  2. Reassign the voting_open row to a new unique id if duplicate
  3. Reset sequence past max(id) so next insert gets a clean id
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

MOTHERDUCK_DB = "ai_driven_data"


def get_md_conn():
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_PAT")
    if not token:
        print("Set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in the environment (or add to .env).")
        sys.exit(1)
    import duckdb
    conn = duckdb.connect(f"md:?motherduck_token={token}")
    conn.execute(f"USE {MOTHERDUCK_DB}")
    return conn


def step1_diagnose(conn):
    print("Step 1 — Current rows in FOOTBALL.matchdays:")
    print("-" * 60)
    rows = conn.execute("""
        SELECT id, sunday_date, status, voting_opens_at
        FROM FOOTBALL.matchdays
        ORDER BY id, voting_opens_at
    """).fetchall()
    if not rows:
        print("  (no rows)")
        return
    for r in rows:
        print(f"  id={r[0]}  sunday_date={r[1]}  status={r[2]}  voting_opens_at={r[3]}")
    print()
    return rows


def step2_fix_duplicate(conn):
    print("Step 2 — Fix: give voting_open matchday a new unique id if needed")
    print("-" * 60)
    # Get current max id among non–voting_open rows (or 0)
    max_other = conn.execute("""
        SELECT COALESCE(MAX(id), 0) FROM FOOTBALL.matchdays WHERE status != 'voting_open'
    """).fetchone()[0]
    new_id = int(max_other) + 1
    # Update the voting_open row(s) to use new_id. If multiple voting_open rows, only one gets new_id (duplicates would need multiple passes; we do one)
    try:
        conn.execute("""
            UPDATE FOOTBALL.matchdays
            SET id = ?
            WHERE status = 'voting_open'
        """, [new_id])
        print(f"  Set id = {new_id} for row(s) with status = 'voting_open'.")
    except Exception as e:
        print(f"  Error: {e}")
        raise
    print()
    return new_id


def step3_reset_sequence(conn):
    print("Step 3 — Reset sequence so next insert gets id > max(id)")
    print("-" * 60)
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM FOOTBALL.matchdays").fetchone()[0]
    try:
        conn.execute("SELECT setval('FOOTBALL.matchday_id_seq', ?)", [max_id])
        print(f"  Set sequence to {max_id}. Next nextval() will return {int(max_id) + 1}.")
    except Exception as e:
        print(f"  setval not supported (DuckDB has no setval): {e}")
        print("  App code uses next_id = max(nextval(), MAX(id)+1), so next insert is still safe.")
    print()


def main():
    print("Connecting to MotherDuck...")
    conn = get_md_conn()
    print("Connected.\n")
    step1_diagnose(conn)
    step2_fix_duplicate(conn)
    step3_reset_sequence(conn)
    print("Re-check after fix:")
    step1_diagnose(conn)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
