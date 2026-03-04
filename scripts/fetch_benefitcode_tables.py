#!/usr/bin/env python3
"""
Fetch benefitcode and benefitcode_procedure tables from DuckDB (or load from MediCloud if empty).
"""
import sys
from pathlib import Path
import pandas as pd
import duckdb
import pyodbc
import toml
from datetime import datetime

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

def fetch_benefitcode_tables():
    """Fetch benefitcode tables from DuckDB, load from MediCloud if needed"""
    db_path = project_root / "ai_driven_data.duckdb"
    schema = "AI DRIVEN DATA"
    
    print("📊 Fetching benefitcode tables...")
    
    # Try read-only connection first
    try:
        print(f"🔌 Connecting to DuckDB (read-only): {db_path}")
        conn = duckdb.connect(str(db_path), read_only=True)
        
        # Check if tables exist and have data (try different name variations)
        benefitcode_df = pd.DataFrame()
        benefitcode_procedure_df = pd.DataFrame()
        
        # Try different table name variations
        benefitcode_names = ['benefitcode', 'BENEFITCODE', 'BENEFITCODES', 'BenefitCode']
        for name in benefitcode_names:
            try:
                benefitcode_df = conn.execute(f'SELECT * FROM "{schema}"."{name}"').fetchdf()
                if not benefitcode_df.empty:
                    print(f"✅ Found {name} in DuckDB: {len(benefitcode_df)} rows")
                    break
            except:
                continue
        
        if benefitcode_df.empty:
            print("⚠️  benefitcode table not found in DuckDB")
        
        procedure_names = ['benefitcode_procedure', 'BENEFITCODE_PROCEDURE', 'BENEFITCODE_PROCEDURES', 'BenefitCode_Procedure']
        for name in procedure_names:
            try:
                benefitcode_procedure_df = conn.execute(f'SELECT * FROM "{schema}"."{name}"').fetchdf()
                if not benefitcode_procedure_df.empty:
                    print(f"✅ Found {name} in DuckDB: {len(benefitcode_procedure_df)} rows")
                    break
            except:
                continue
        
        if benefitcode_procedure_df.empty:
            print("⚠️  benefitcode_procedure table not found in DuckDB")
        
        conn.close()
        
        # If tables are empty, load from MediCloud
        if benefitcode_df.empty or benefitcode_procedure_df.empty:
            print("\n📥 Tables are empty or missing. Loading from MediCloud SQL Server...")
            
            # Need write access to load
            print("⚠️  Need write access to load tables. Please stop the API server temporarily.")
            print("   Then run: ./venv/bin/python scripts/load_benefitcode_tables.py")
            print("   Or I can load them now if you stop the API...")
            
            # Try to load anyway
            try:
                medicloud_conn = create_medicloud_connection()
                print("✅ Connected to MediCloud")
                
                # Load benefitcode
                if benefitcode_df.empty:
                    print("📋 Loading benefitcode from MediCloud...")
                    benefitcode_df = pd.read_sql("SELECT * FROM dbo.benefitcode", medicloud_conn)
                    print(f"   ✅ Loaded {len(benefitcode_df)} rows")
                
                # Load benefitcode_procedure
                if benefitcode_procedure_df.empty:
                    print("📋 Loading benefitcode_procedure from MediCloud...")
                    benefitcode_procedure_df = pd.read_sql("SELECT * FROM dbo.benefitcode_procedure", medicloud_conn)
                    print(f"   ✅ Loaded {len(benefitcode_procedure_df)} rows")
                
                medicloud_conn.close()
            except Exception as e:
                print(f"❌ Could not load from MediCloud: {e}")
                print("   Tables will be exported as-is (empty)")
        
        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = project_root / f'benefitcode_tables_{timestamp}.xlsx'
        
        print(f"\n📝 Exporting to Excel: {output_file}")
        with pd.ExcelWriter(str(output_file), engine='openpyxl') as writer:
            benefitcode_df.to_excel(writer, sheet_name='benefitcode', index=False)
            print(f"   ✅ Sheet 'benefitcode': {len(benefitcode_df)} rows, {len(benefitcode_df.columns) if not benefitcode_df.empty else 0} columns")
            
            benefitcode_procedure_df.to_excel(writer, sheet_name='benefitcode_procedure', index=False)
            print(f"   ✅ Sheet 'benefitcode_procedure': {len(benefitcode_procedure_df)} rows, {len(benefitcode_procedure_df.columns) if not benefitcode_procedure_df.empty else 0} columns")
        
        print(f"\n✅ Export complete: {output_file}")
        return str(output_file)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    fetch_benefitcode_tables()
