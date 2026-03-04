#!/usr/bin/env python3
"""
Rebuild FIN_GL_2025_RAW from EACCOUNT Database

This script fetches FIN_GL data from EACCOUNT for 2025 and builds
the FIN_GL_2025_RAW table with proper column mapping:
- GLID -> glid
- AccCode -> glcode (and acccode)
- GLDesc -> gldesc
- GLDate -> gldate
- GLAmount -> glamount
- RefNo -> refno
- code -> code (company ID - this is the key column!)
- acctype -> tagged for CASH and CURRENT ASSETS acccodes

The code column is critical - it contains the company ID used for pairing
transactions in derived tables like CLIENT_CASH_RECEIVED.

Author: KLAIRE AI - CLEARLINE INTERNATIONAL LIMITED
Date: 2025-11-05
"""

import pandas as pd
import pyodbc
import toml
import duckdb
import logging

logger = logging.getLogger(__name__)

def get_sql_driver():
    """Get the best available SQL Server driver"""
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

def create_eacount_connection():
    """Create connection to EACCOUNT database"""
    secrets = toml.load("secrets.toml")
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['eaccount_credentials']['server']},{secrets['eaccount_credentials']['port']};"
        f"DATABASE={secrets['eaccount_credentials']['database']};"
        f"UID={secrets['eaccount_credentials']['username']};"
        f"PWD={secrets['eaccount_credentials']['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=120;"
    )
    return pyodbc.connect(conn_str)

def rebuild_fin_gl_2025_raw(db_path='ai_driven_data.duckdb'):
    """
    Rebuild FIN_GL_2025_RAW table from EACCOUNT database.
    
    Args:
        db_path: Path to DuckDB database file
    
    Returns:
        tuple: (success: bool, row_count: int, error_message: str)
    """
    try:
        logger.info("📥 Connecting to EACCOUNT...")
        conn = create_eacount_connection()
        logger.info("✅ Connected to EACCOUNT!")
        
        # Fetch 2025 data using correct column names
        query = """
            SELECT *
            FROM dbo.FIN_GL
            WHERE YEAR(GLDate) = 2025
            ORDER BY GLDate, AccCode
        """
        logger.info("📥 Fetching 2025 data from EACCOUNT...")
        df = pd.read_sql(query, conn)
        conn.close()
        logger.info(f"✅ Successfully loaded {len(df):,} rows from EACCOUNT")
        
        if df.empty:
            logger.warning("⚠️ No data found for 2025")
            return False, 0, "No data found for 2025"
        
        # Build FIN_GL_2025_RAW
        logger.info("🔨 Building FIN_GL_2025_RAW...")
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
        
        # Map columns from EACCOUNT to our format
        out = pd.DataFrame()
        
        # GLID
        out['glid'] = pd.to_numeric(df['GLID'], errors='coerce')
        
        # Acctype
        out['acctype'] = df.get('acctype')
        
        # GLCode / AccCode
        out['glcode'] = df['AccCode'].astype(str)
        
        # GLDesc
        out['gldesc'] = df.get('GLDesc')
        
        # GLDate
        out['gldate'] = pd.to_datetime(df['GLDate'], errors='coerce')
        
        # GLAmount
        out['glamount'] = pd.to_numeric(df['GLAmount'], errors='coerce')
        
        # RefNo
        out['refno'] = df.get('RefNo')
        
        # CODE - company ID (this is the key column!)
        out['code'] = pd.to_numeric(df['code'], errors='coerce')
        out['code'] = out['code'].astype('Int64').astype(str).replace('<NA>', None)
        
        # AccCode
        out['acccode'] = out['glcode']
        
        # Tag acctype for CASH and CURRENT ASSETS
        CASH = ('1618002','1618003','1618007','1619029','1618046','1618047','1618049','1618051','1619031','1618053','1618055')
        ASSETS = ('1312001','1312002')
        out.loc[out['acccode'].isin(CASH), 'acctype'] = 'CASH'
        out.loc[out['acccode'].isin(ASSETS), 'acctype'] = 'CURRENT ASSETS'
        
        out = out.sort_values(['gldate','glcode','gldesc'], na_position='last').reset_index(drop=True)
        
        # Save to DuckDB
        con.execute('DROP TABLE IF EXISTS "AI DRIVEN DATA"."FIN_GL_2025_RAW"')
        con.register('df_src', out)
        con.execute('CREATE TABLE "AI DRIVEN DATA"."FIN_GL_2025_RAW" AS SELECT * FROM df_src')
        cnt = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."FIN_GL_2025_RAW"').fetchone()[0]
        logger.info(f"✅ FIN_GL_2025_RAW created with {cnt:,} rows")
        
        code_count = con.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."FIN_GL_2025_RAW" WHERE code IS NOT NULL').fetchone()[0]
        logger.info(f"📊 Rows with code (company ID): {code_count:,}")
        
        con.close()
        
        return True, cnt, None
        
    except Exception as e:
        error_msg = f"Error rebuilding FIN_GL_2025_RAW: {e}"
        logger.error(f"❌ {error_msg}")
        import traceback
        logger.error(traceback.format_exc())
        return False, 0, error_msg

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    success, count, error = rebuild_fin_gl_2025_raw()
    if success:
        print(f"✅ Successfully rebuilt FIN_GL_2025_RAW with {count:,} rows")
    else:
        print(f"❌ Failed: {error}")
        exit(1)

