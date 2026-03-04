#!/usr/bin/env python3
"""
EXPENSE AND COMMISSION Tables Creation Script

This script creates EXPENSE_AND_COMMISSION tables for 2023, 2024, and 2025
using the specific AccCode pairing logic from GL data.

Categories and AccCode Pairs:
- MOTOR VEHICLE REPAIR: 3862400 and 1618006
- SECURITY: 2021066 and 1618024 
- SUBSCRIPTION: 2021045 and 1618024
- DIRECTORS REMUNERATION: 2021002 and 1618024
- DIRECTORS ALLOWANCES: 2021054 and 1618024
- POSTAGES & TELEPHONE: 2021043 and 1618024
- ELECTRICITY & RATES: 2021065 AND 1618024
- PRINTING & STATIONERY: 2021047 and 1618024
- REPAIRS & MAINTENANCE: 2021040 and 1618024
- GENERATOR RUNNING: 2021041 and 1618024
- MOTOR RUNNING EXPENSES: 2021060 and 1618024
- TRANSPORT & TRAVELLING: 2021042 and 1618024
- ADVERTISING: 2021044 and 1618024
- RENT & RATES: 2021011 and 1618024
- COMMISSION: 2021059 and 1618037
- MGT ALLOWANCE: 2021055 and 1618024
- WELFARE: 2021039 and 1618024

Matching Conditions:
1. Same date (gldate)
2. Sum of amounts equals zero
3. Same GLDesc column

Output: Date, Category, Amount (positive amount)
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

# Category and AccCode pair definitions
CATEGORY_PAIRS = {
    'MOTOR VEHICLE REPAIR': ('3862400', '1618006'),
    'SECURITY': ('2021066', '1618024'),
    'SUBSCRIPTION': ('2021045', '1618024'),
    'DIRECTORS REMUNERATION': ('2021002', '1618024'),
    'DIRECTORS ALLOWANCES': ('2021054', '1618024'),
    'POSTAGES & TELEPHONE': ('2021043', '1618024'),
    'ELECTRICITY & RATES': ('2021065', '1618024'),
    'PRINTING & STATIONERY': ('2021047', '1618024'),
    'REPAIRS & MAINTENANCE': ('2021040', '1618024'),
    'GENERATOR RUNNING': ('2021041', '1618024'),
    'MOTOR RUNNING EXPENSES': ('2021060', '1618024'),
    'TRANSPORT & TRAVELLING': ('2021042', '1618024'),
    'ADVERTISING': ('2021044', '1618024'),
    'RENT & RATES': ('2021011', '1618024'),
    'COMMISSION': ('2021059', '1618037'),
    'MGT ALLOWANCE': ('2021055', '1618024'),
    'WELFARE': ('2021039', '1618024')
}

def create_expense_and_commission_year(conn: duckdb.DuckDBPyConnection, year: int) -> Dict[str, Any]:
    """
    Creates the EXPENSE_AND_COMMISSION table for a specific year.
    
    Args:
        conn: DuckDB connection
        year: Year to process (2023, 2024, or 2025)
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info(f"🔄 Creating EXPENSE_AND_COMMISSION_{year} table...")
    print("=" * 60)
    
    table_name = f'FIN_GL_{year}_RAW'
    result_table = f'EXPENSE_AND_COMMISSION_{year}'
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."{result_table}"')
    
    # Build the UNION ALL query for all categories
    union_queries = []
    
    for category, (acccode1, acccode2) in CATEGORY_PAIRS.items():
        query = f'''
        SELECT
            t1.gldate as Date,
            '{category}' as Category,
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
            (t1.glamount + t2.glamount) as sum_check,
            t1.gldesc as gldesc_1,
            t2.gldesc as gldesc_2
        FROM "{SCHEMA}"."{table_name}" t1
        JOIN "{SCHEMA}"."{table_name}" t2
            ON t1.gldate = t2.gldate
            AND COALESCE(t1.gldesc, '') = COALESCE(t2.gldesc, '')
            AND t1.glid < t2.glid
        WHERE
            t1.acccode = '{acccode1}'
            AND t2.acccode = '{acccode2}'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.gldate IS NOT NULL
            AND t1.gldesc IS NOT NULL
            AND t1.gldesc != ''
        '''
        union_queries.append(query)
    
    # Create the table with all categories
    query = f'''
    CREATE TABLE "{SCHEMA}"."{result_table}" AS
    {' UNION ALL '.join(union_queries)}
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."{result_table}"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(Amount) FROM "{SCHEMA}"."{result_table}"').fetchone()[0] or 0
    
    logger.info(f'✅ EXPENSE_AND_COMMISSION_{year} created successfully!')
    logger.info(f'📊 Total rows: {total_rows:,}')
    logger.info(f'💰 Total amount: ₦{total_amount:,.0f}')
    
    # Get category breakdown
    category_breakdown = conn.execute(f'''
        SELECT 
            Category,
            COUNT(*) as transactions,
            SUM(Amount) as total_amount
        FROM "{SCHEMA}"."{result_table}"
        GROUP BY Category
        ORDER BY total_amount DESC
    ''').fetchdf()
    
    logger.info(f'📈 Top categories:')
    for _, row in category_breakdown.head(5).iterrows():
        logger.info(f'   {row["Category"]}: {row["transactions"]} transactions, ₦{row["total_amount"]:,.0f}')
    
    return {
        "total_rows": total_rows,
        "total_amount": total_amount,
        "year": year,
        "category_breakdown": category_breakdown
    }

def create_expense_and_commission_combined(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Creates the combined EXPENSE_AND_COMMISSION table from all years.
    
    Args:
        conn: DuckDB connection
    
    Returns:
        Dictionary with summary statistics
    """
    logger.info("🔄 Creating EXPENSE_AND_COMMISSION combined table...")
    print("=" * 60)
    
    # Drop existing table if it exists
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."EXPENSE_AND_COMMISSION"')
    
    # Create combined table
    # Note: currently combines 2023–2026. If new yearly tables are added later,
    # update this UNION to include them.
    query = f'''
    CREATE TABLE "{SCHEMA}"."EXPENSE_AND_COMMISSION" AS
    SELECT Date, Category, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check, gldesc_1, gldesc_2
    FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_2023"
    UNION ALL
    SELECT Date, Category, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check, gldesc_1, gldesc_2
    FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_2024"
    UNION ALL
    SELECT Date, Category, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check, gldesc_1, gldesc_2
    FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_2025"
    UNION ALL
    SELECT Date, Category, Amount, Year, Month, acccode_1, acccode_2, amount_1, amount_2, sum_check, gldesc_1, gldesc_2
    FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_2026"
    ORDER BY Date DESC, Amount DESC
    '''
    
    conn.execute(query)
    
    # Get summary statistics
    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"').fetchone()[0]
    total_amount = conn.execute(f'SELECT SUM(Amount) FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"').fetchone()[0] or 0
    
    logger.info(f'✅ EXPENSE_AND_COMMISSION combined created successfully!')
    logger.info(f'📊 Total rows: {total_rows:,}')
    logger.info(f'💰 Total amount: ₦{total_amount:,.0f}')
    
    return {
        "total_rows": total_rows,
        "total_amount": total_amount
    }

def validate_expense_and_commission_tables(conn: duckdb.DuckDBPyConnection) -> bool:
    """
    Validates all EXPENSE_AND_COMMISSION tables.
    
    Args:
        conn: DuckDB connection
    
    Returns:
        True if validation passes, False otherwise
    """
    logger.info("🔍 Validating EXPENSE_AND_COMMISSION tables...")
    
    try:
        # Check if all tables exist
        tables = [
            'EXPENSE_AND_COMMISSION_2023',
            'EXPENSE_AND_COMMISSION_2024',
            'EXPENSE_AND_COMMISSION_2025',
            'EXPENSE_AND_COMMISSION_2026',
            'EXPENSE_AND_COMMISSION',
        ]
        
        for table in tables:
            table_exists = conn.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{SCHEMA}' AND table_name = '{table}'").fetchone()[0] > 0
            if not table_exists:
                logger.error(f"❌ Validation failed: {table} table does not exist.")
                return False
        
        # Check for expected columns
        expected_columns = ['Date', 'Category', 'Amount', 'Year', 'Month', 'acccode_1', 'acccode_2', 'amount_1', 'amount_2', 'sum_check', 'gldesc_1', 'gldesc_2']
        
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
        
        logger.info("✅ All EXPENSE_AND_COMMISSION tables validation passed!")
        return True
        
    except Exception as e:
        logger.error(f"❌ An error occurred during validation: {e}")
        return False

def export_to_excel(conn: duckdb.DuckDBPyConnection):
    """
    Export all EXPENSE_AND_COMMISSION tables to Excel.
    
    Args:
        conn: DuckDB connection
    """
    logger.info("📊 Exporting EXPENSE_AND_COMMISSION tables to Excel...")
    
    try:
        with pd.ExcelWriter('EXPENSE_AND_COMMISSION_TABLES.xlsx', engine='openpyxl') as writer:
            # Export each year table
            for year in [2023, 2024, 2025, 2026]:
                table_name = f'EXPENSE_AND_COMMISSION_{year}'
                df = conn.execute(f'SELECT * FROM "{SCHEMA}"."{table_name}"').fetchdf()
                df.to_excel(writer, sheet_name=str(year), index=False)
                logger.info(f"✅ Exported {table_name}: {len(df)} rows")
            
            # Export combined table
            df_combined = conn.execute(f'SELECT * FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION"').fetchdf()
            df_combined.to_excel(writer, sheet_name='Combined', index=False)
            logger.info(f"✅ Exported EXPENSE_AND_COMMISSION: {len(df_combined)} rows")
            
            # Create summary sheet
            summary_data = []
            for year in [2023, 2024, 2025, 2026]:
                result = conn.execute(f'''
                    SELECT 
                        COUNT(*) as transactions,
                        SUM(Amount) as total_amount,
                        COUNT(DISTINCT Category) as unique_categories
                    FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_{year}"
                ''').fetchdf()
                
                summary_data.append({
                    'Year': year,
                    'Transactions': result['transactions'].iloc[0],
                    'Total_Amount': result['total_amount'].iloc[0],
                    'Unique_Categories': result['unique_categories'].iloc[0]
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            logger.info("✅ Exported Summary sheet")
        
        logger.info("🎉 Excel export completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error exporting to Excel: {e}")

def main():
    """Main function to create all EXPENSE_AND_COMMISSION tables."""
    print("🚀 EXPENSE AND COMMISSION Tables Creation Script")
    print("=" * 60)
    
    # Connect to database
    conn = duckdb.connect(DB_PATH)
    
    try:
        # Create tables for each year
        summaries = []
        for year in [2023, 2024, 2025]:
            summary = create_expense_and_commission_year(conn, year)
            summaries.append(summary)
        
        # Create combined table
        combined_summary = create_expense_and_commission_combined(conn)
        
        # Print year breakdown
        print("\n📈 Year Breakdown:")
        for summary in summaries:
            print(f"   {summary['year']}: {summary['total_rows']:,} transactions, ₦{summary['total_amount']:,.0f}")
        
        # Validate tables
        if validate_expense_and_commission_tables(conn):
            logger.info("✅ All EXPENSE_AND_COMMISSION tables validation passed!")
        else:
            logger.error("❌ EXPENSE_AND_COMMISSION tables validation failed!")
            return False
        
        # Export to Excel
        export_to_excel(conn)
        
        logger.info("🎉 All EXPENSE_AND_COMMISSION tables are ready for use!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating EXPENSE_AND_COMMISSION tables: {e}")
        return False
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
