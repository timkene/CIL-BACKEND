#!/usr/bin/env python3
"""
SALARY AND PALLIATIVE Tables Creation Script

This script creates SALARY_AND_PALLIATIVE tables for 2023, 2024, and 2025
using the specific AccCode pairing logic from GL data.

AccCode Pairs:
1. 3761001 and 2021003
2. 2021039 and 1618024

Matching Conditions:
1. Same date (gldate)
2. Same code column
3. Same RefNo column  
4. Sum of amounts equals zero

Output: Date, Amount (positive amount)
"""

import duckdb
import pandas as pd
import logging
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

# AccCode pair definitions
ACCODE_PAIRS = [
    ('3761001', '2021003'),
    ('2021039', '1618024')
]

def create_salary_and_palliative_year(conn: duckdb.DuckDBPyConnection, year: int) -> Dict[str, Any]:
    """
    Creates the SALARY_AND_PALLIATIVE table for a specific year.
    
    Args:
        conn: DuckDB connection
        year: Year to process (2023, 2024, or 2025)
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info(f"🔄 Creating SALARY_AND_PALLIATIVE_{year} table...")
    print("=" * 60)
    
    table_name = f'FIN_GL_{year}_RAW'
    result_table = f'SALARY_AND_PALLIATIVE_{year}'
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{result_table}"')
    
    # Create the table with correct pairing logic
    query = f'''
    CREATE TABLE "{SCHEMA}"."{result_table}" AS
    WITH PairedTransactions AS (
        -- Pair 1: 3761001 + 2021003
        SELECT
            t1.gldate as Date,
            COALESCE(CAST(t1.code AS VARCHAR), 'None') as groupname,
            CASE 
                WHEN t1.glamount > 0 THEN t1.glamount
                ELSE t2.glamount
            END as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month,
            t1.acccode as acccode_1,
            t2.acccode as acccode_2,
            t1.glamount as amount_1,
            t2.glamount as amount_2,
            (t1.glamount + t2.glamount) as sum_check
        FROM "{SCHEMA}"."{table_name}" t1
        JOIN "{SCHEMA}"."{table_name}" t2
            ON t1.gldate = t2.gldate
            AND (t1.refno IS NULL AND t2.refno IS NULL OR t1.refno = t2.refno)
            AND t1.glid < t2.glid
        WHERE
            t1.acccode = '3761001'
            AND t2.acccode = '2021003'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.gldate IS NOT NULL

        UNION ALL

        -- Pair 2: 2021039 + 1618024
        SELECT
            t1.gldate as Date,
            COALESCE(CAST(t1.code AS VARCHAR), 'None') as groupname,
            CASE 
                WHEN t1.glamount > 0 THEN t1.glamount
                ELSE t2.glamount
            END as Amount,
            EXTRACT(YEAR FROM t1.gldate) as Year,
            EXTRACT(MONTH FROM t1.gldate) as Month,
            t1.acccode as acccode_1,
            t2.acccode as acccode_2,
            t1.glamount as amount_1,
            t2.glamount as amount_2,
            (t1.glamount + t2.glamount) as sum_check
        FROM "{SCHEMA}"."{table_name}" t1
        JOIN "{SCHEMA}"."{table_name}" t2
            ON t1.gldate = t2.gldate
            AND (t1.refno IS NULL AND t2.refno IS NULL OR t1.refno = t2.refno)
            AND t1.glid < t2.glid
        WHERE
            t1.acccode = '2021039'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.gldate IS NOT NULL
    )
    SELECT 
        Date, groupname, Amount, Year, Month,
        acccode_1, acccode_2, amount_1, amount_2, sum_check
    FROM PairedTransactions 
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{result_table}"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(Amount) FROM "{SCHEMA}"."{result_table}"').fetchone()[0] or 0
    
    logger.info(f'✅ SALARY_AND_PALLIATIVE_{year} created successfully!')
    logger.info(f'📊 Total rows: {total_rows:,}')
    logger.info(f'💰 Total amount: ₦{total_amount:,.0f}')
    
    return {
        "total_rows": total_rows,
        "total_amount": total_amount,
        "year": year
    }

def create_salary_and_palliative_combined(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Creates the combined SALARY_AND_PALLIATIVE table from all years.
    
    Args:
        conn: DuckDB connection
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info("🔄 Creating SALARY_AND_PALLIATIVE combined table...")
    print("=" * 60)
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."SALARY_AND_PALLIATIVE"')
    
    # Create combined table
    # Note: currently combines 2023–2026. If new yearly tables are added later,
    # update this UNION to include them.
    query = f'''
    CREATE TABLE "{SCHEMA}"."SALARY_AND_PALLIATIVE" AS
    SELECT Date, groupname, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check
    FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_2023"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check
    FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_2024"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check
    FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_2025"
    UNION ALL
    SELECT Date, groupname, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check
    FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_2026"
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(Amount) FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE"').fetchone()[0] or 0
    
    logger.info(f'✅ SALARY_AND_PALLIATIVE combined created successfully!')
    logger.info(f'📊 Total rows: {total_rows:,}')
    logger.info(f'💰 Total amount: ₦{total_amount:,.0f}')
    
    return {
        "total_rows": total_rows,
        "total_amount": total_amount
    }

def validate_salary_and_palliative_tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Validates all SALARY_AND_PALLIATIVE tables.
    
    Args:
        conn: DuckDB connection
    
    Returns:
        True if validation passes, False otherwise
    """
    logger.info("🔍 Validating SALARY_AND_PALLIATIVE tables...")
    
    try:
        # Check if all tables exist
        tables = [
            'SALARY_AND_PALLIATIVE_2023',
            'SALARY_AND_PALLIATIVE_2024',
            'SALARY_AND_PALLIATIVE_2025',
            'SALARY_AND_PALLIATIVE_2026',
            'SALARY_AND_PALLIATIVE',
        ]
        
        for table in tables:
            table_exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = '{table}'").fetchone()[0] > 0
            if not table_exists:
                logger.error(f"❌ Validation failed: {table} table does not exist.")
                return False
        
        # Check for expected columns
        expected_columns = ['Date', 'groupname', 'Amount', 'Year', 'Month', 'acccode_1', 'acccode_2', 'amount_1', 'amount_2', 'sum_check']
        
        for table in tables:
            columns_info = conn.execute(f"PRAGMA table_info(\"{SCHEMA}\".\"{table}\")").fetchdf()
            actual_columns = columns_info['name'].tolist()
            if not all(col in actual_columns for col in expected_columns):
                logger.error(f"❌ Validation failed: Missing expected columns in {table}. Expected: {expected_columns}, Actual: {actual_columns}")
                return False
        
        # Check for data integrity (e.g., no null amounts, positive amounts)
        for table in tables:
            null_amounts = conn.execute(f"SELECT COUNT(*) FROM \"{SCHEMA}\".\"{table}\" WHERE Amount IS NULL").fetchone()[0]
            if null_amounts > 0:
                logger.error(f"❌ Validation failed: {null_amounts} rows with NULL amounts found in {table}.")
                return False
            
            negative_amounts = conn.execute(f"SELECT COUNT(*) FROM \"{SCHEMA}\".\"{table}\" WHERE Amount < 0").fetchone()[0]
            if negative_amounts > 0:
                logger.error(f"❌ Validation failed: {negative_amounts} rows with negative amounts found in {table}.")
                return False
        
        logger.info("✅ All SALARY_AND_PALLIATIVE tables validation passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ An error occurred during validation: {e}")
        return False

def export_to_excel(conn: duckdb.DuckDBPyConnection):
    """
    Export all SALARY_AND_PALLIATIVE tables to Excel.
    
    Args:
        conn: DuckDB connection
    """
    logger.info("📊 Exporting SALARY_AND_PALLIATIVE tables to Excel...")
    
    try:
        with pd.ExcelWriter('SALARY_AND_PALLIATIVE_TABLES.xlsx', engine='openpyxl') as writer:
            # Export each year table
            for year in [2023, 2024, 2025, 2026]:
                table_name = f'SALARY_AND_PALLIATIVE_{year}'
                df = conn.execute(f'SELECT * FROM "{SCHEMA}"."{table_name}"').fetchdf()
                df.to_excel(writer, sheet_name=str(year), index=False)
                logger.info(f"✅ Exported {table_name}: {len(df)} rows")
            
            # Export combined table
            df_combined = conn.execute(f'SELECT * FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE"').fetchdf()
            df_combined.to_excel(writer, sheet_name='Combined', index=False)
            logger.info(f"✅ Exported SALARY_AND_PALLIATIVE: {len(df_combined)} rows")
            
            # Create summary sheet
            summary_data = []
            for year in [2023, 2024, 2025, 2026]:
                result = conn.execute(f'''
                    SELECT 
                        COUNT(*) as transactions,
                        SUM(Amount) as total_amount,
                        COUNT(CASE WHEN acccode_1 = '3761001' THEN 1 END) as pair_1_count,
                        COUNT(CASE WHEN acccode_1 = '2021039' THEN 1 END) as pair_2_count
                    FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_{year}"
                ''').fetchdf()
                
                summary_data.append({
                    'Year': year,
                    'Transactions': result['transactions'].iloc[0],
                    'Total_Amount': result['total_amount'].iloc[0],
                    'Pair_3761001_2021003': result['pair_1_count'].iloc[0],
                    'Pair_2021039_1618024': result['pair_2_count'].iloc[0]
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            logger.info("✅ Exported Summary sheet")
        
        logger.info("🎉 Excel export completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error exporting to Excel: {e}")

def main():
    """Main function to create all SALARY_AND_PALLIATIVE tables."""
    print("🚀 SALARY AND PALLIATIVE Tables Creation Script")
    print("=" * 60)
    
    # Connect to database
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Create tables for each year
        summaries = []
        for year in [2023, 2024, 2025]:
            summary = create_salary_and_palliative_year(conn, year)
            summaries.append(summary)
        
        # Create combined table
        combined_summary = create_salary_and_palliative_combined(conn)
        
        # Print year breakdown
        print("\n📈 Year Breakdown:")
        for summary in summaries:
            print(f"   {summary['year']}: {summary['total_rows']:,} transactions, ₦{summary['total_amount']:,.0f}")
        
        # Validate tables
        if validate_salary_and_palliative_tables(conn):
            logger.info("✅ All SALARY_AND_PALLIATIVE tables validation passed!")
        else:
            logger.error("❌ SALARY_AND_PALLIATIVE tables validation failed!")
            return False
        
        # Export to Excel
        export_to_excel(conn)
        
        logger.info("🎉 All SALARY_AND_PALLIATIVE tables are ready for use!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating SALARY_AND_PALLIATIVE tables: {e}")
        return False
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
