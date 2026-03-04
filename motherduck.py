"""
Sync Local DuckDB to MotherDuck
================================
Run this once to upload all tables, then daily to keep in sync.
Set MOTHERDUCK_TOKEN or MOTHERDUCK_PAT in environment.
"""

import os
import duckdb
from datetime import datetime

# Configuration
LOCAL_DB = os.path.join(os.path.dirname(__file__), 'ai_driven_data.duckdb')
MOTHERDUCK_TOKEN = os.environ.get('MOTHERDUCK_TOKEN') or os.environ.get('MOTHERDUCK_PAT') or ''
MOTHERDUCK_DB = 'ai_driven_data'  # Database name in MotherDuck

def initial_upload():
    """
    One-time upload: Copy all tables from local to MotherDuck
    """
    print("🚀 Starting initial upload to MotherDuck...")

    # Connect to local DuckDB
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)

    # Connect to MotherDuck without database name first
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')

    # Create the database if it doesn't exist
    print(f"📦 Creating database: {MOTHERDUCK_DB}")
    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    
    # Get all tables from local database
    tables_query = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'AI DRIVEN DATA'
    """
    
    tables = local_conn.execute(tables_query).fetchall()
    
    print(f"📊 Found {len(tables)} tables to upload")
    
    for schema, table in tables:
        print(f"\n⏳ Uploading: {schema}.{table}")
        
        # Create schema in MotherDuck if it doesn't exist
        md_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        
        # Drop table if exists (for clean upload)
        md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        
        # Copy table structure and data from local to MotherDuck
        # This uses DuckDB's ATTACH feature
        md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")
        md_conn.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM local_db."{schema}"."{table}"')
        md_conn.execute("DETACH local_db")
        
        # Get row count to verify
        count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
        print(f"✅ Uploaded {count:,} rows")
    
    local_conn.close()
    md_conn.close()
    
    print("\n🎉 Initial upload complete!")

def daily_sync():
    """
    Daily sync: Update only tables that changed
    Uses TRUNCATE + INSERT for full refresh (simpler and safer)
    """
    print(f"\n🔄 Starting daily sync at {datetime.now()}")
    
    # Connect to both databases
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    
    # Tables to sync (add/remove as needed)
    tables_to_sync = [
        ('AI DRIVEN DATA', 'CLAIMS DATA'),
        ('AI DRIVEN DATA', 'DEBIT_NOTE'),
        ('AI DRIVEN DATA', 'CLIENT_CASH_RECEIVED'),
        ('AI DRIVEN DATA', 'PA DATA'),
        ('AI DRIVEN DATA', 'GROUP_CONTRACT'),
        ('AI DRIVEN DATA', 'GROUPS'),
        ('AI DRIVEN DATA', 'MEMBERS'),  # Fixed: was ENROLLEES
        ('AI DRIVEN DATA', 'MEMBER_COVERAGE'),  # New table
        ('AI DRIVEN DATA', 'MEMBER_PLANS'),
        ('AI DRIVEN DATA', 'PROVIDERS'),
        ('AI DRIVEN DATA', 'PLANS'),
        ('AI DRIVEN DATA', 'GROUP_PLANS'),
        # Financial tables
        ('AI DRIVEN DATA', 'DEBIT_NOTE_ACCRUED'),
        ('AI DRIVEN DATA', 'EXPENSE_AND_COMMISSION'),
        ('AI DRIVEN DATA', 'SALARY_AND_PALLIATIVE'),
        # Add more tables here as needed
    ]
    
    md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")
    
    for schema, table in tables_to_sync:
        try:
            print(f"\n⏳ Syncing: {schema}.{table}")
            
            # Get local count
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            
            # Truncate and reload (full refresh)
            md_conn.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
            md_conn.execute(f'INSERT INTO "{schema}"."{table}" SELECT * FROM local_db."{schema}"."{table}"')
            
            # Verify
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            
            if local_count == md_count:
                print(f"✅ Synced {md_count:,} rows")
            else:
                print(f"⚠️ Warning: Local={local_count:,}, MotherDuck={md_count:,}")
                
        except Exception as e:
            print(f"❌ Failed to sync {table}: {e}")
    
    md_conn.execute("DETACH local_db")
    
    local_conn.close()
    md_conn.close()
    
    print(f"\n✅ Daily sync complete at {datetime.now()}")

def smart_sync():
    """
    Smart sync: Only update tables that actually changed
    (Faster but more complex)
    """
    print(f"\n🧠 Starting smart sync at {datetime.now()}")
    
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    
    tables_to_check = [
        ('AI DRIVEN DATA', 'CLAIMS DATA'),
        ('AI DRIVEN DATA', 'DEBIT_NOTE'),
        ('AI DRIVEN DATA', 'CLIENT_CASH_RECEIVED'),
        ('AI DRIVEN DATA', 'PA DATA'),
        ('AI DRIVEN DATA', 'MEMBERS'),
        ('AI DRIVEN DATA', 'MEMBER_PLANS'),
        ('AI DRIVEN DATA', 'PROVIDERS'),
        ('AI DRIVEN DATA', 'PLANS'),
        ('AI DRIVEN DATA', 'GROUP_PLANS'),
        ('AI DRIVEN DATA', 'GROUP_CONTRACT'),
        ('AI DRIVEN DATA', 'GROUPS'),
    ]
    
    md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")
    
    for schema, table in tables_to_check:
        try:
            # Compare row counts and max dates (if date column exists)
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            
            if local_count != md_count:
                print(f"\n📊 {table}: Local={local_count:,}, Cloud={md_count:,}")
                print(f"⏳ Syncing...")
                
                md_conn.execute(f'TRUNCATE TABLE "{schema}"."{table}"')
                md_conn.execute(f'INSERT INTO "{schema}"."{table}" SELECT * FROM local_db."{schema}"."{table}"')
                
                print(f"✅ Synced!")
            else:
                print(f"✓ {table} unchanged ({local_count:,} rows)")
                
        except Exception as e:
            print(f"❌ Error with {table}: {e}")
    
    md_conn.execute("DETACH local_db")
    
    local_conn.close()
    md_conn.close()
    
    print(f"\n✅ Smart sync complete at {datetime.now()}")


def push_nhia_schema():
    """
    Push NHIA schema from local DuckDB to MotherDuck
    Replicates all tables in the NHIA schema exactly as they are locally
    """
    print("="*70)
    print("🚀 PUSHING NHIA SCHEMA TO MOTHERDUCK")
    print("="*70)
    print()

    # Connect to local DuckDB
    local_conn = duckdb.connect(LOCAL_DB, read_only=True)

    # Connect to MotherDuck
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    
    # Create the database if it doesn't exist
    print(f"📦 Using database: {MOTHERDUCK_DB}")
    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")
    
    # Get all tables from NHIA schema in local database
    tables_query = """
    SELECT table_schema, table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'NHIA'
    ORDER BY table_name
    """
    
    tables = local_conn.execute(tables_query).fetchall()
    
    if not tables:
        print("⚠️  No tables found in NHIA schema in local database")
        local_conn.close()
        md_conn.close()
        return
    
    print(f"📊 Found {len(tables)} tables in NHIA schema to upload")
    print()
    
    # Create NHIA schema in MotherDuck if it doesn't exist
    md_conn.execute('CREATE SCHEMA IF NOT EXISTS "NHIA"')
    
    # Attach local database for efficient copying
    md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")
    
    success_count = 0
    error_count = 0
    
    for schema, table in tables:
        try:
            print(f"⏳ Uploading: {schema}.{table}")
            
            # Get local row count
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            
            # Drop table if exists (for clean upload)
            md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            
            # Copy table structure and data from local to MotherDuck
            md_conn.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM local_db."{schema}"."{table}"')
            
            # Get row count to verify
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            
            if local_count == md_count:
                print(f"   ✅ Uploaded {md_count:,} rows")
                success_count += 1
            else:
                print(f"   ⚠️  Row count mismatch: Local={local_count:,}, MotherDuck={md_count:,}")
                success_count += 1
                
        except Exception as e:
            print(f"   ❌ Failed to upload {table}: {e}")
            error_count += 1
    
    # Detach local database
    md_conn.execute("DETACH local_db")
    
    local_conn.close()
    md_conn.close()
    
    print()
    print("="*70)
    print("✅ NHIA SCHEMA UPLOAD COMPLETE")
    print("="*70)
    print(f"   Successful: {success_count} tables")
    if error_count > 0:
        print(f"   Failed: {error_count} tables")
    print()


def push_procedure_diagnosis_schema():
    """
    Push PROCEDURE_DIAGNOSIS schema from local DuckDB to MotherDuck.
    Gives you the same schema (and data) locally and in the cloud.
    """
    print("="*70)
    print("🚀 PUSHING PROCEDURE_DIAGNOSIS SCHEMA TO MOTHERDUCK")
    print("="*70)
    print()

    local_conn = duckdb.connect(LOCAL_DB, read_only=True)
    md_conn = duckdb.connect(f'md:?motherduck_token={MOTHERDUCK_TOKEN}')
    md_conn.execute(f"CREATE DATABASE IF NOT EXISTS {MOTHERDUCK_DB}")
    md_conn.execute(f"USE {MOTHERDUCK_DB}")

    tables_query = """
    SELECT table_schema, table_name
    FROM information_schema.tables
    WHERE table_schema = 'PROCEDURE_DIAGNOSIS'
    ORDER BY table_name
    """
    tables = local_conn.execute(tables_query).fetchall()

    if not tables:
        print("⚠️  No tables found in PROCEDURE_DIAGNOSIS schema in local database")
        local_conn.close()
        md_conn.close()
        return

    print(f"📊 Found {len(tables)} tables in PROCEDURE_DIAGNOSIS schema to upload")
    print()

    md_conn.execute('CREATE SCHEMA IF NOT EXISTS "PROCEDURE_DIAGNOSIS"')
    md_conn.execute(f"ATTACH '{LOCAL_DB}' AS local_db (READ_ONLY)")

    success_count = 0
    error_count = 0

    for schema, table in tables:
        try:
            print(f"⏳ Uploading: {schema}.{table}")
            local_count = local_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            md_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            md_conn.execute(f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM local_db."{schema}"."{table}"')
            md_count = md_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
            if local_count == md_count:
                print(f"   ✅ Uploaded {md_count:,} rows")
                success_count += 1
            else:
                print(f"   ⚠️  Row count mismatch: Local={local_count:,}, MotherDuck={md_count:,}")
                success_count += 1
        except Exception as e:
            print(f"   ❌ Failed to upload {table}: {e}")
            error_count += 1

    md_conn.execute("DETACH local_db")
    local_conn.close()
    md_conn.close()

    print()
    print("="*70)
    print("✅ PROCEDURE_DIAGNOSIS SCHEMA UPLOAD COMPLETE")
    print("="*70)
    print(f"   Successful: {success_count} tables")
    if error_count > 0:
        print(f"   Failed: {error_count} tables")
    print()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python motherduck.py initial    # First time upload (AI DRIVEN DATA schema)")
        print("  python motherduck.py daily      # Daily full sync")
        print("  python motherduck.py smart      # Only sync changed tables")
        print("  python motherduck.py nhia       # Push NHIA schema to MotherDuck")
        print("  python motherduck.py procedure_diagnosis   # Push PROCEDURE_DIAGNOSIS schema to MotherDuck")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "initial":
        initial_upload()
    elif command == "daily":
        daily_sync()
    elif command == "smart":
        smart_sync()
    elif command == "nhia":
        push_nhia_schema()
    elif command in ("procedure_diagnosis", "pd"):
        push_procedure_diagnosis_schema()
    else:
        print(f"Unknown command: {command}")