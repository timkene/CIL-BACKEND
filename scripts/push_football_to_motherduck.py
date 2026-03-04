#!/usr/bin/env python3
"""
Push FOOTBALL schema and data from local DuckDB to MotherDuck.
Use this once so Render (and any client) can use the same data.

Requires: MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in environment.
Run from DLT root: MOTHERDUCK_TOKEN=your_token python scripts/push_football_to_motherduck.py
"""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "ai_driven_data.duckdb"
MOTHERDUCK_DB = "ai_driven_data"

# Tables in FOOTBALL schema (same order as in main_eko.py; copy order respects FKs)
FOOTBALL_TABLES = [
    "players",
    "dues",
    "matchdays",
    "matchday_fixtures",
    "fixture_goals",
    "matchday_votes",
    "matchday_groups",
    "matchday_group_members",
    "matchday_attendance",
    "fixture_ratings",
    "matchday_cards",
    "payment_evidence",
]


def main():
    token = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_PAT")
    if not token:
        print("Set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in the environment.")
        sys.exit(1)

    db_path = os.getenv("DUCKDB_PATH", str(DEFAULT_DB))
    if not Path(db_path).exists():
        print(f"Local database not found: {db_path}")
        sys.exit(1)

    import duckdb

    local_conn = duckdb.connect(db_path, read_only=True)
    md_conn = duckdb.connect(f"md:?motherduck_token={token}")

    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    md_conn.execute("CREATE SCHEMA IF NOT EXISTS FOOTBALL")
    md_conn.execute(f"ATTACH '{db_path}' AS local_db (READ_ONLY)")

    print("=" * 60)
    print("Pushing FOOTBALL schema to MotherDuck")
    print("=" * 60)
    print(f"Local DB: {db_path}")
    print(f"MotherDuck DB: {MOTHERDUCK_DB}\n")

    success = 0
    for table in FOOTBALL_TABLES:
        try:
            n_local = local_conn.execute(
                f'SELECT COUNT(*) FROM FOOTBALL."{table}"'
            ).fetchone()[0]
        except Exception:
            n_local = -1

        try:
            md_conn.execute(f'DROP TABLE IF EXISTS FOOTBALL."{table}"')
            md_conn.execute(
                f'CREATE TABLE FOOTBALL."{table}" AS SELECT * FROM local_db.FOOTBALL."{table}"'
            )
            n_md = md_conn.execute(
                f'SELECT COUNT(*) FROM FOOTBALL."{table}"'
            ).fetchone()[0]
            print(f"  FOOTBALL.{table}: {n_md:,} rows")
            success += 1
        except Exception as e:
            if n_local >= 0:
                print(f"  FOOTBALL.{table}: skip (local had {n_local:,}) - {e}")
            else:
                print(f"  FOOTBALL.{table}: not present locally, skipped")

    md_conn.execute("DETACH local_db")
    local_conn.close()
    md_conn.close()

    print("=" * 60)
    print(f"Done. {success}/{len(FOOTBALL_TABLES)} tables pushed to MotherDuck.")
    print("Set USE_LOCAL_DB=false and MOTHERDUCK_TOKEN on Render to use this DB.")
    print("=" * 60)


if __name__ == "__main__":
    main()
