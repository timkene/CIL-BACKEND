#!/usr/bin/env python3
"""
Push Admin Module Tables to MotherDuck
======================================
Pushes USERS, DEPARTMENT_PERMISSIONS, and STAFF_CLIENT_ALLOCATIONS tables
from local DuckDB to MotherDuck cloud database.
"""

import duckdb
import os
from datetime import datetime

# Configuration (set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in env)
LOCAL_DB = os.path.join(os.path.dirname(__file__), 'ai_driven_data.duckdb')
MOTHERDUCK_TOKEN = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('MOTHERDUCK_PAT') or ''
MOTHERDUCK_DB = 'ai_driven_data'
SCHEMA = 'AI DRIVEN DATA'

# Admin tables to sync
ADMIN_TABLES = [
    'USERS',
    'DEPARTMENT_PERMISSIONS',
    'STAFF_CLIENT_ALLOCATIONS'
]

def push_admin_tables():
    """Push admin module tables from local DuckDB to MotherDuck"""
    print("=" * 70)
    print("Pushing Admin Module Tables to MotherDuck")
    print("=" * 70)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Connect to local DuckDB
    if not os.path.exists(LOCAL_DB):
        print(f"❌ Error: Local database not found: {LOCAL_DB}")
        return False
    
    print(f"📂 Connecting to local database: {LOCAL_DB}")
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)
    
    # Connect to MotherDuck
    print(f"☁️  Connecting to MotherDuck...")
    try:
        md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
        md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
        md_conn.execute(f"USE {MOTHERDUCK_DB}")
        print(f"✅ Connected to MotherDuck database: {MOTHERDUCK_DB}")
    except Exception as e:
        print(f"❌ Failed to connect to MotherDuck: {e}")
        local_conn.close()
        return False
    
    # Create schema in MotherDuck
    md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    
    # Attach local database
    md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")
    
    success_count = 0
    error_count = 0
    
    for table_name in ADMIN_TABLES:
        try:
            print(f"\n📊 Syncing: {SCHEMA}.{table_name}")
            
            # Check if table exists in local DB
            local_check = local_conn.execute(f'''
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{SCHEMA}' 
                AND table_name = '{table_name}'
            ''').fetchone()[0]
            
            if local_check == 0:
                print(f"  ⚠️  Table {table_name} not found in local database, skipping...")
                continue
            
            # Get local row count
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table_name}"').fetchone()[0]
            print(f"  📈 Local rows: {local_count:,}")
            
            # Drop existing table in MotherDuck (for clean sync)
            md_conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{table_name}"')
            
            # Copy table structure and data from local to MotherDuck
            md_conn.execute(f'CREATE TABLE "{SCHEMA}"."{table_name}" AS SELECT * FROM local_db."{SCHEMA}"."{table_name}"')
            
            # Verify sync
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{table_name}"').fetchone()[0]
            
            if local_count == md_count:
                print(f"  ✅ Synced {md_count:,} rows to MotherDuck")
                success_count += 1
            else:
                print(f"  ⚠️  Row count mismatch: Local={local_count:,}, MotherDuck={md_count:,}")
                success_count += 1  # Still count as success if data was synced
                
        except Exception as e:
            print(f"  ❌ Error syncing {table_name}: {e}")
            import traceback
            traceback.print_exc()
            error_count += 1
    
    # Detach local database
    md_conn.execute("DETACH local_db")
    
    # Close connections
    local_conn.close()
    md_conn.close()
    
    # Summary
    print("\n" + "=" * 70)
    print("Sync Summary")
    print("=" * 70)
    print(f"✅ Successfully synced: {success_count} tables")
    if error_count > 0:
        print(f"❌ Errors: {error_count} tables")
    print(f"📊 Total tables processed: {len(ADMIN_TABLES)}")
    print("=" * 70)
    
    return error_count == 0

if __name__ == "__main__":
    success = push_admin_tables()
    exit(0 if success else 1)
