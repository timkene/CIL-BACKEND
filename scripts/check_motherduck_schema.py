#!/usr/bin/env python3
"""
Check tables in MotherDuck schema "AI DRIVEN DATA".
Run from DLT root with: USE_LOCAL_DB=false MOTHERDUCK_TOKEN=your_token python scripts/check_motherduck_schema.py
Or ensure .env has USE_LOCAL_DB=false and MOTHERDUCK_TOKEN, then: python scripts/check_motherduck_schema.py
"""
import os
import sys
from pathlib import Path

# Load .env from repo root if available
root = Path(__file__).resolve().parents[1]
env_file = root / ".env"
if env_file.exists():
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        pass

USE_LOCAL_DB = os.getenv("USE_LOCAL_DB", "true").lower() in ("true", "1", "yes")
TOKEN = os.getenv("MOTHERDUCK_TOKEN") or os.getenv("MOTHERDUCK_PAT")

def main():
    if USE_LOCAL_DB:
        print("USE_LOCAL_DB is true. Set USE_LOCAL_DB=false to check MotherDuck.")
        sys.exit(1)
    if not TOKEN:
        print("MOTHERDUCK_TOKEN (or MOTHERDUCK_PAT) not set. Set it in .env or environment.")
        sys.exit(1)

    import duckdb
    conn = duckdb.connect(f"md:?motherduck_token={TOKEN}")
    conn.execute("USE ai_driven_data")

    # List all schemas (exclude system)
    schemas = conn.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('information_schema', 'pg_catalog')
        ORDER BY schema_name
    """).fetchall()
    schema_list = sorted(set(s[0] for s in schemas))
    print("Schemas in MotherDuck (ai_driven_data):")
    for s in schema_list:
        print(f"  - {s}")

    # List ALL tables and views (all schemas)
    all_tables = conn.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
    """).fetchall()

    # Count per schema
    from collections import defaultdict
    by_schema = defaultdict(list)
    for schema, table, tbl_type in all_tables:
        by_schema[schema].append((table, tbl_type))

    print(f"\nTotal: {len(all_tables)} tables/views across {len(by_schema)} schemas\n")
    for schema in sorted(by_schema.keys()):
        items = by_schema[schema]
        tables = [t for t, typ in items if typ == 'BASE TABLE']
        views = [t for t, typ in items if typ == 'VIEW']
        print(f"  [{schema}] — {len(tables)} tables" + (f", {len(views)} views" if views else ""))
        for t, typ in sorted(items):
            suffix = " (VIEW)" if typ == 'VIEW' else ""
            print(f"    - {t}{suffix}")

    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
