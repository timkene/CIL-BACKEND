#!/usr/bin/env python3
"""
DEBIT NOTE ACCRUED Table Creation Script
========================================

This script creates the DEBIT_NOTE_ACCRUED table from FIN_GL raw data.
It implements the correct logic for pairing INCOME and CURRENT LIABILITIES transactions.

Logic:
1. Each transaction is posted twice (one debited, one credited)
2. INCOME is always debited (negative amount)
3. CURRENT LIABILITIES is always credited (positive amount)
4. Both transactions must have same GLDate and code
5. Their amounts must sum to zero

Author: KLAIRE AI - CLEARLINE INTERNATIONAL LIMITED
Date: 2025-01-25
"""

import duckdb
import pandas as pd
from typing import Optional

# Database configuration
DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

def create_debit_note_accrued_table(conn: duckdb.DuckDBPyConnection) -> dict:
    """
    Creates the DEBIT_NOTE_ACCRUED table with correct pairing logic.
    
    Returns:
        dict: Summary statistics of the created table
    """
    print("🔄 Creating DEBIT_NOTE_ACCRUED table...")
    print("=" * 60)
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."DEBIT_NOTE_ACCRUED"')
    
    # Create the table with correct pairing logic (both orders)
    query = f'''
    CREATE TABLE "{SCHEMA}"."DEBIT_NOTE_ACCRUED" AS
    WITH PairedTransactions AS (
        -- Case 1: INCOME (negative) + CURRENT LIABILITIES (positive)
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t2.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2023_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2023_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'INCOME' AND t1.glamount < 0 AND
            t2.acctype = 'CURRENT LIABILITIES' AND t2.glamount > 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL

        UNION ALL

        -- Case 2: CURRENT LIABILITIES (positive) + INCOME (negative)
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t1.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2023_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2023_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'CURRENT LIABILITIES' AND t1.glamount > 0 AND
            t2.acctype = 'INCOME' AND t2.glamount < 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL

        UNION ALL

        -- 2024 data - Case 1
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t2.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2024_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2024_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'INCOME' AND t1.glamount < 0 AND
            t2.acctype = 'CURRENT LIABILITIES' AND t2.glamount > 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL

        UNION ALL

        -- 2024 data - Case 2
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t1.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2024_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2024_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'CURRENT LIABILITIES' AND t1.glamount > 0 AND
            t2.acctype = 'INCOME' AND t2.glamount < 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL

        UNION ALL

        -- 2025 data - Case 1
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t2.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2025_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2025_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'INCOME' AND t1.glamount < 0 AND
            t2.acctype = 'CURRENT LIABILITIES' AND t2.glamount > 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL

        UNION ALL

        -- 2025 data - Case 2
        SELECT
            t1.gldate as Date,
            eag.groupname,
            t1.glamount as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month
        FROM "{SCHEMA}"."FIN_GL_2025_RAW" t1
        JOIN "{SCHEMA}"."FIN_GL_2025_RAW" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acctype = 'CURRENT LIABILITIES' AND t1.glamount > 0 AND
            t2.acctype = 'INCOME' AND t2.glamount < 0 AND
            (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != ''
            AND eag.groupname IS NOT NULL
    )
    SELECT * FROM PairedTransactions ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(amount) FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"').fetchone()[0]
    
    # Get year breakdown
    year_stats = conn.execute(f'''
        SELECT 
            Year,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"
        GROUP BY Year
        ORDER BY Year
    ''').fetchdf()
    
    # Test ARIK AIR specifically (validation)
    arik_air_test = conn.execute(f'''
        SELECT 
            Year,
            Month,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"
        WHERE UPPER(groupname) LIKE '%ARIK AIR%'
        GROUP BY Year, Month
        ORDER BY Year, Month
    ''').fetchdf()
    
    summary = {
        'total_rows': total_rows,
        'total_amount': total_amount,
        'year_stats': year_stats,
        'arik_air_test': arik_air_test
    }
    
    return summary

def validate_table(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Validates that the DEBIT_NOTE_ACCRUED table was created correctly.
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    print("\n🔍 Validating DEBIT_NOTE_ACCRUED table...")
    
    try:
        # Check if table exists
        table_exists = conn.execute(f'''
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{SCHEMA}' 
            AND table_name = 'DEBIT_NOTE_ACCRUED'
        ''').fetchone()[0] > 0
        
        if not table_exists:
            print("❌ Table does not exist!")
            return False
        
        # Check column structure
        columns = conn.execute(f'DESCRIBE "{SCHEMA}"."DEBIT_NOTE_ACCRUED"').fetchdf()
        expected_columns = ['Date', 'groupname', 'Amount', 'Year', 'Month']
        
        if not all(col in columns['column_name'].values for col in expected_columns):
            print("❌ Missing expected columns!")
            return False
        
        # Check ARIK AIR validation (should have July & September 2025 data)
        arik_july_sept = conn.execute(f'''
            SELECT COUNT(*) as count
            FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"
            WHERE UPPER(groupname) LIKE '%ARIK AIR%'
            AND Year = 2025
            AND Month IN (7, 9)
        ''').fetchone()[0]
        
        if arik_july_sept == 0:
            print("❌ ARIK AIR July & September 2025 data missing!")
            return False
        
        print("✅ Validation passed!")
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def main():
    """Main function to create and validate the DEBIT_NOTE_ACCRUED table."""
    print("🚀 DEBIT NOTE ACCRUED Table Creation Script")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = duckdb.connect(DB_PATH)
        
        # Create the table
        summary = create_debit_note_accrued_table(conn)
        
        # Print summary
        print(f"\n✅ DEBIT_NOTE_ACCRUED table created successfully!")
        print(f"📊 Total rows: {summary['total_rows']:,}")
        print(f"💰 Total amount: ₦{summary['total_amount']:,.0f}")
        
        print(f"\n📈 Year Breakdown:")
        for _, row in summary['year_stats'].iterrows():
            print(f"   {int(row['Year'])}: {int(row['transactions']):,} transactions, ₦{row['total_amount']:,.0f}")
        
        if not summary['arik_air_test'].empty:
            print(f"\n✈️ ARIK AIR Validation:")
            for _, row in summary['arik_air_test'].iterrows():
                print(f"   {int(row['Year'])}-{int(row['Month']):02d}: ₦{row['total_amount']:,.0f}")
        
        # Validate the table
        if validate_table(conn):
            print(f"\n🎉 DEBIT_NOTE_ACCRUED table is ready for use!")
        else:
            print(f"\n⚠️ Validation failed - please check the table!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating DEBIT_NOTE_ACCRUED table: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)

