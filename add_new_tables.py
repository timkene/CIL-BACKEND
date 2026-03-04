#!/usr/bin/env python3
"""
Quick script to add new tables to MotherDuck
Use this when you've added tables to local DB that don't exist in MotherDuck yet.
Set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in environment.
"""

import os
import duckdb
from datetime import datetime

# Configuration
LOCAL_DB = os.path.join(os.path.dirname(__file__), 'ai_driven_data.duckdb')
MOTHERDUCK_TOKEN = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('MOTHERDUCK_PAT') or ''
MOTHERDUCK_DB = 'ai_driven_data'

# NEW TABLES TO ADD (add your new table names here)
new_tables = [
    ('AI DRIVEN DATA', 'MEMBERS'),
    ('AI DRIVEN DATA', 'MEMBER_COVERAGE'),
]

def add_new_tables():
    """Add new tables from local to MotherDuck"""
    print(f"🚀 Adding new tables to MotherDuck at {datetime.now()}\n")

    # Connect to local
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)

    # Connect to MotherDuck
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    md_conn.execute(f"USE {MOTHERDUCK_DB}")

    for schema, table in new_tables:
        try:
            print(f"⏳ Adding: {schema}.{table}")

            # Check if table exists in local
            check_local = f"""
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
                AND table_name = '{table}'
            """

            local_exists = local_conn.execute(check_local).fetchone()[0]

            if local_exists == 0:
                print(f"   ⚠️  Table doesn't exist in local database - skipping")
                continue

            # Check if already exists in MotherDuck
            try:
                md_check = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()
                print(f"   ℹ️  Table already exists in MotherDuck with {md_check[0]:,} rows")
                print(f"   🔄 Dropping and recreating...")
                md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            except:
                print(f"   ✅ Table doesn't exist in MotherDuck yet - creating new")

            # Create schema if needed
            md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

            # Copy table structure and data
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            print(f"   📊 Copying {local_count:,} rows...")

            # Create table from local data
            copy_query = f'''
            CREATE TABLE "{schema}"."{table}" AS
            SELECT * FROM duckdb_scan('{LOCAL_DB}', '{schema}', '{table}')
            '''

            md_conn.execute(copy_query)

            # Verify
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]

            if md_count == local_count:
                print(f"   ✅ Successfully added {md_count:,} rows\n")
            else:
                print(f"   ⚠️  Row count mismatch! Local: {local_count:,}, MotherDuck: {md_count:,}\n")

        except Exception as e:
            print(f"   ❌ Failed to add {table}: {e}\n")
            continue

    local_conn.close()
    md_conn.close()

    print(f"✅ Finished adding new tables at {datetime.now()}")

if __name__ == "__main__":
    add_new_tables()
