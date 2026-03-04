#!/usr/bin/env python3
"""
Load benefitcode and benefitcode_procedure tables from MediCloud SQL Server into DuckDB.
"""
import sys
from pathlib import Path
import pandas as pd
import duckdb
import pyodbc
import toml
import os

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def get_sql_driver():
    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    for d in preferred:
        if d in drivers:
            return d
    raise RuntimeError("No compatible SQL Server driver found.")

def create_medicloud_connection():
    """Create connection to MediCloud SQL Server"""
    secrets_path = project_root / "secrets.toml"
    if not secrets_path.exists():
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")
    
    secrets = toml.load(str(secrets_path))
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['credentials']['server']},{secrets['credentials']['port']};"
        f"DATABASE={secrets['credentials']['database']};"
        f"UID={secrets['credentials']['username']};"
        f"PWD={secrets['credentials']['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def load_benefitcode_tables():
    """Load benefitcode and benefitcode_procedure from MediCloud to DuckDB"""
    db_path = project_root / "ai_driven_data.duckdb"
    schema = "AI DRIVEN DATA"
    
    print("📊 Loading benefitcode tables from MediCloud to DuckDB...")
    
    try:
        # Connect to MediCloud
        print("🔌 Connecting to MediCloud SQL Server...")
        medicloud_conn = create_medicloud_connection()
        print("✅ Connected to MediCloud")
        
        # Connect to DuckDB
        print(f"🔌 Connecting to DuckDB: {db_path}")
        duckdb_conn = duckdb.connect(str(db_path), read_only=False)
        duckdb_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        print("✅ Connected to DuckDB")
        
        # Load benefitcode
        print("\n📋 Loading benefitcode table...")
        try:
            benefitcode_query = "SELECT * FROM dbo.benefitcode"
            benefitcode_df = pd.read_sql(benefitcode_query, medicloud_conn)
            print(f"   ✅ Loaded {len(benefitcode_df)} rows from MediCloud")
            
            # Write to DuckDB
            duckdb_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."benefitcode"')
            duckdb_conn.register('benefitcode_temp', benefitcode_df)
            duckdb_conn.execute(f'CREATE TABLE "{schema}"."benefitcode" AS SELECT * FROM benefitcode_temp')
            duckdb_conn.unregister('benefitcode_temp')
            print(f"   ✅ Written {len(benefitcode_df)} rows to DuckDB")
        except Exception as e:
            print(f"   ❌ Error loading benefitcode: {e}")
            import traceback
            traceback.print_exc()
        
        # Load benefitcode_procedure
        print("\n📋 Loading benefitcode_procedure table...")
        try:
            procedure_query = "SELECT * FROM dbo.benefitcode_procedure"
            procedure_df = pd.read_sql(procedure_query, medicloud_conn)
            print(f"   ✅ Loaded {len(procedure_df)} rows from MediCloud")
            
            # Write to DuckDB
            duckdb_conn.execute(f'DROP TABLE IF EXISTS "{schema}"."benefitcode_procedure"')
            duckdb_conn.register('procedure_temp', procedure_df)
            duckdb_conn.execute(f'CREATE TABLE "{schema}"."benefitcode_procedure" AS SELECT * FROM procedure_temp')
            duckdb_conn.unregister('procedure_temp')
            print(f"   ✅ Written {len(procedure_df)} rows to DuckDB")
        except Exception as e:
            print(f"   ❌ Error loading benefitcode_procedure: {e}")
            import traceback
            traceback.print_exc()
        
        # Verify
        print("\n✅ Verification:")
        try:
            benefitcode_count = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."benefitcode"').fetchone()[0]
            procedure_count = duckdb_conn.execute(f'SELECT COUNT(*) FROM "{schema}"."benefitcode_procedure"').fetchone()[0]
            print(f"   benefitcode: {benefitcode_count} rows")
            print(f"   benefitcode_procedure: {procedure_count} rows")
        except Exception as e:
            print(f"   ⚠️ Could not verify: {e}")
        
        medicloud_conn.close()
        duckdb_conn.close()
        
        print("\n✅ Load complete!")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = load_benefitcode_tables()
    sys.exit(0 if success else 1)
