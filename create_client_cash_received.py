#!/usr/bin/env python3
"""
CLIENT CASH RECEIVED Table Creation Script
==========================================

This script creates the CLIENT_CASH_RECEIVED tables from FIN_GL raw data.
It implements the correct logic for pairing CASH and CURRENT ASSETS transactions.

Logic:
1. CASH AccCodes (11 codes) paired with CURRENT ASSETS AccCodes (2 codes)
2. Same GLDate and code for both transactions
3. Sum of amounts must equal zero
4. Pick the positive amount (from CURRENT ASSETS side)
5. Filter out transactions where groupname is None

Author: KLAIRE AI - CLEARLINE INTERNATIONAL LIMITED
Date: 2025-01-25
"""

import duckdb
import pandas as pd
from typing import Optional, Dict, Any

# Database configuration
DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

# AccCode definitions - EXACT as requested by user
CASH_ACCODES = [
    '1618002', '1618003', '1618007', '1619029', '1618046',
    '1618047', '1618049', '1618051', '1619031', '1618053', '1618055'
]

CURRENT_ASSETS_ACCODES = [
    '1312001', '1312002'
]

def create_client_cash_received_year(conn: duckdb.DuckDBPyConnection, year: int) -> Dict[str, Any]:
    """
    Creates the CLIENT_CASH_RECEIVED table for a specific year.
    
    Args:
        conn: DuckDB connection
        year: Year to process (2023, 2024, or 2025)
    
    Returns:
        dict: Summary statistics of the created table
    """
    print(f"🔄 Creating CLIENT_CASH_RECEIVED_{year} table...")
    print("=" * 60)
    
    table_name = f'FIN_GL_{year}_RAW'
    result_table = f'CLIENT_CASH_RECEIVED_{year}'
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{result_table}"')
    
    # Create the table with CORRECT pairing logic (CASH + CURRENT ASSETS, no amount restrictions)
    query = f'''
    CREATE TABLE "{SCHEMA}"."{result_table}" AS
    WITH PairedTransactions AS (
        -- Case 1: CASH + CURRENT ASSETS (sum to zero)
        SELECT
            t1.gldate as Date,
            COALESCE(eag.groupname, CAST(t1.code AS VARCHAR)) as groupname,
            CASE 
                WHEN t1.glamount > 0 THEN t1.glamount
                ELSE t2.glamount
            END as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month,
            t1.acccode as cash_acccode,
            t2.acccode as assets_acccode,
            t1.glamount as cash_amount,
            t2.glamount as assets_amount,
            (t1.glamount + t2.glamount) as sum_check
        FROM "{SCHEMA}"."{table_name}" t1
        JOIN "{SCHEMA}"."{table_name}" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND (t1.refno IS NULL AND t2.refno IS NULL OR t1.refno = t2.refno)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acccode IN ({','.join([f"'{code}'" for code in CASH_ACCODES])})
            AND t2.acccode IN ({','.join([f"'{code}'" for code in CURRENT_ASSETS_ACCODES])})
            AND (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != '' AND t1.code != 'None'

        UNION ALL

        -- Case 2: CURRENT ASSETS + CASH (sum to zero)
        SELECT
            t1.gldate as Date,
            COALESCE(eag.groupname, CAST(t1.code AS VARCHAR)) as groupname,
            CASE 
                WHEN t1.glamount > 0 THEN t1.glamount
                ELSE t2.glamount
            END as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month,
            t2.acccode as cash_acccode,
            t1.acccode as assets_acccode,
            t2.glamount as cash_amount,
            t1.glamount as assets_amount,
            (t1.glamount + t2.glamount) as sum_check
        FROM "{SCHEMA}"."{table_name}" t1
        JOIN "{SCHEMA}"."{table_name}" t2
            ON t1.gldate = t2.gldate
            AND CAST(t1.code AS VARCHAR) = CAST(t2.code AS VARCHAR)
            AND (t1.refno IS NULL AND t2.refno IS NULL OR t1.refno = t2.refno)
            AND t1.glid < t2.glid
        LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" eag ON CAST(t1.code AS VARCHAR) = CAST(eag.id_company AS VARCHAR)
        WHERE
            t1.acccode IN ({','.join([f"'{code}'" for code in CURRENT_ASSETS_ACCODES])})
            AND t2.acccode IN ({','.join([f"'{code}'" for code in CASH_ACCODES])})
            AND (t1.glamount + t2.glamount) = 0
            AND t1.code IS NOT NULL AND t1.code != '' AND t1.code != 'None'
    )
    SELECT 
        Date, groupname, Amount, Year, Month,
        cash_acccode, assets_acccode, cash_amount, assets_amount, sum_check
    FROM PairedTransactions 
    WHERE groupname IS NOT NULL  -- Filter out None groupnames
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{result_table}"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(amount) FROM "{SCHEMA}"."{result_table}"').fetchone()[0]
    
    # Get month breakdown
    month_stats = conn.execute(f'''
        SELECT 
            Month,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."{result_table}"
        GROUP BY Month
        ORDER BY Month
    ''').fetchdf()
    
    # Get top companies
    top_companies = conn.execute(f'''
        SELECT 
            groupname,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."{result_table}"
        GROUP BY groupname
        ORDER BY total_amount DESC
        LIMIT 10
    ''').fetchdf()
    
    summary = {
        'year': year,
        'total_rows': total_rows,
        'total_amount': total_amount,
        'month_stats': month_stats,
        'top_companies': top_companies
    }
    
    return summary

def create_client_cash_received_combined(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Creates the combined CLIENT_CASH_RECEIVED table from all years.
    
    Returns:
        dict: Summary statistics of the combined table
    """
    print("🔄 Creating CLIENT_CASH_RECEIVED combined table...")
    print("=" * 60)
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."CLIENT_CASH_RECEIVED"')
    
    # Create combined table
    # Note: currently combines 2023–2026. If new yearly tables are added later,
    # update this UNION to include them.
    query = f'''
    CREATE TABLE "{SCHEMA}"."CLIENT_CASH_RECEIVED" AS
    SELECT Date, groupname, Amount, Year, Month, cash_acccode, assets_acccode, cash_amount, assets_amount, sum_check
    FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_2023"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, cash_acccode, assets_acccode, cash_amount, assets_amount, sum_check
    FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_2024"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, cash_acccode, assets_acccode, cash_amount, assets_amount, sum_check
    FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_2025"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, cash_acccode, assets_acccode, cash_amount, assets_amount, sum_check
    FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_2026"
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(amount) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"').fetchone()[0]
    
    # Get year breakdown
    year_stats = conn.execute(f'''
        SELECT 
            Year,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"
        GROUP BY Year
        ORDER BY Year
    ''').fetchdf()
    
    summary = {
        'total_rows': total_rows,
        'total_amount': total_amount,
        'year_stats': year_stats
    }
    
    return summary

def validate_client_cash_received_tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Validates that all CLIENT_CASH_RECEIVED tables were created correctly.
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    print("\n🔍 Validating CLIENT_CASH_RECEIVED tables...")
    
    try:
        # Check if all tables exist
        tables = [
            'CLIENT_CASH_RECEIVED_2023',
            'CLIENT_CASH_RECEIVED_2024',
            'CLIENT_CASH_RECEIVED_2025',
            'CLIENT_CASH_RECEIVED_2026',
            'CLIENT_CASH_RECEIVED',
        ]
        
        for table in tables:
            table_exists = conn.execute(f'''
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{SCHEMA}' 
                AND table_name = '{table}'
            ''').fetchone()[0] > 0
            
            if not table_exists:
                print(f"❌ Table {table} does not exist!")
                return False
        
        # Check for None groupnames
        for year in [2023, 2024, 2025, 2026]:
            none_count = conn.execute(f'''
                SELECT COUNT(*) 
                FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_{year}"
                WHERE groupname IS NULL
            ''').fetchone()[0]
            
            if none_count > 0:
                print(f"❌ CLIENT_CASH_RECEIVED_{year} has {none_count} rows with None groupname!")
                return False
        
        print("✅ All CLIENT_CASH_RECEIVED tables validation passed!")
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False

def main():
    """Main function to create all CLIENT_CASH_RECEIVED tables."""
    print("🚀 CLIENT CASH RECEIVED Tables Creation Script")
    print("=" * 60)
    
    try:
        # Connect to database
        conn = duckdb.connect(DB_PATH)
        
        # Create tables for each year
        summaries = {}
        for year in [2023, 2024, 2025, 2026]:
            summaries[year] = create_client_cash_received_year(conn, year)
            
            print(f"\n✅ CLIENT_CASH_RECEIVED_{year} created successfully!")
            print(f"📊 Total rows: {summaries[year]['total_rows']:,}")
            total_amt = summaries[year]['total_amount'] or 0
            print(f"💰 Total amount: ₦{total_amt:,.0f}")
        
        # Create combined table
        combined_summary = create_client_cash_received_combined(conn)
        
        print(f"\n✅ CLIENT_CASH_RECEIVED combined created successfully!")
        print(f"📊 Total rows: {combined_summary['total_rows']:,}")
        combined_amt = combined_summary['total_amount'] or 0
        print(f"💰 Total amount: ₦{combined_amt:,.0f}")
        
        # Print year breakdown
        print(f"\n📈 Year Breakdown:")
        for _, row in combined_summary['year_stats'].iterrows():
            row_amt = row['total_amount'] or 0
            print(f"   {int(row['Year'])}: {int(row['transactions']):,} transactions, ₦{row_amt:,.0f}")
        
        # Validate all tables
        if validate_client_cash_received_tables(conn):
            print(f"\n🎉 All CLIENT_CASH_RECEIVED tables are ready for use!")
        else:
            print(f"\n⚠️ Validation failed - please check the tables!")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error creating CLIENT_CASH_RECEIVED tables: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
