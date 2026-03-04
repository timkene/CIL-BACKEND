import duckdb
import pandas as pd

DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

def create_debit_note_accrued(conn):
    """Create DEBIT_NOTE_ACCRUED table from merged GL data"""
    print("🔄 Creating DEBIT_NOTE_ACCRUED table...")
    
    # Drop existing table
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."DEBIT_NOTE_ACCRUED"')
    
    # Create the derived table
    conn.execute(f'''
        CREATE TABLE "{SCHEMA}"."DEBIT_NOTE_ACCRUED" AS
        WITH paired_transactions AS (
            SELECT 
                t1.GLDate as Date,
                t1.code,
                t2.GLAmount as Amount,  -- Use CURRENT LIABILITIES amount (positive)
                COALESCE(e.groupname, t1.code) as groupname,
                EXTRACT(YEAR FROM t1.GLDate) as year,
                EXTRACT(MONTH FROM t1.GLDate) as month
            FROM "{SCHEMA}"."FIN_GL_2023_2024_RAW" t1
            JOIN "{SCHEMA}"."FIN_GL_2023_2024_RAW" t2 
                ON t1.GLDate = t2.GLDate 
                AND t1.code = t2.code 
                AND t1.code != ''
                AND t1.code IS NOT NULL
            LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" e ON t1.code = CAST(e.ID_Company AS VARCHAR)
            WHERE t1.acctype = 'INCOME' 
                AND t2.acctype = 'CURRENT LIABILITIES'
                AND t1.AccCode = '3040101'  -- Specific AccCode for INCOME
                AND t2.AccCode = '2328002'  -- Specific AccCode for CURRENT LIABILITIES
                AND (t1.GLAmount + t2.GLAmount) = 0
                AND t1.GLAmount < 0
                AND t2.GLAmount > 0
        )
        SELECT 
            Date,
            groupname,
            Amount,
            year,
            month
        FROM paired_transactions
        ORDER BY Date, groupname
    ''')
    
    count = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."DEBIT_NOTE_ACCRUED"').fetchone()[0]
    print(f"✅ DEBIT_NOTE_ACCRUED created with {count:,} rows")
    return count

def create_client_cash_received(conn):
    """Create CLIENT_CASH_RECEIVED table from merged GL data"""
    print("🔄 Creating CLIENT_CASH_RECEIVED table...")
    
    # Drop existing table
    conn.execute(f'DROP TABLE IF EXISTS "{SCHEMA}"."CLIENT_CASH_RECEIVED"')
    
    # Create the derived table - CORRECTED: Use CURRENT ASSETS amount (positive)
    conn.execute(f'''
        CREATE TABLE "{SCHEMA}"."CLIENT_CASH_RECEIVED" AS
        WITH paired_transactions AS (
            SELECT 
                t1.GLDate as Date,
                t1.code,
                t2.GLAmount as Amount,  -- Use CURRENT ASSETS amount (positive)
                COALESCE(e.groupname, t1.code) as groupname,
                EXTRACT(YEAR FROM t1.GLDate) as year,
                EXTRACT(MONTH FROM t1.GLDate) as month
            FROM "{SCHEMA}"."FIN_GL_2023_2024_RAW" t1
            JOIN "{SCHEMA}"."FIN_GL_2023_2024_RAW" t2 
                ON t1.GLDate = t2.GLDate 
                AND t1.code = t2.code 
                AND t1.code != ''
                AND t1.code IS NOT NULL
            LEFT JOIN "{SCHEMA}"."E_ACCOUNT_GROUP" e ON t1.code = CAST(e.ID_Company AS VARCHAR)
            WHERE t1.acctype = 'CASH' 
                AND t2.acctype = 'CURRENT ASSETS'
                AND (t1.GLAmount + t2.GLAmount) = 0
                AND t1.GLAmount < 0
                AND t2.GLAmount > 0
        )
        SELECT 
            Date,
            groupname,
            Amount,
            year,
            month
        FROM paired_transactions
        ORDER BY Date, groupname
    ''')
    
    count = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED"').fetchone()[0]
    print(f"✅ CLIENT_CASH_RECEIVED created with {count:,} rows")
    return count

def main():
    conn = duckdb.connect(DB_PATH)
    print("🔄 Creating derived tables from merged GL data...")
    
    # Create derived tables
    debit_count = create_debit_note_accrued(conn)
    cash_count = create_client_cash_received(conn)
    
    conn.close()
    print(f"🎉 Derived tables created successfully!")
    print(f"   - DEBIT_NOTE_ACCRUED: {debit_count:,} rows")
    print(f"   - CLIENT_CASH_RECEIVED: {cash_count:,} rows")

def create_salary_and_palliative_2023(conn):
    """Create SALARY_AND_PALLIATIVE_2023 table"""
    print("📊 Creating SALARY_AND_PALLIATIVE_2023...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2023" AS
    WITH salary_palliative_pairs AS (
        -- AccCode pairs: (3761001 and 2021003) OR (2021039 and 1618024)
        SELECT 
            t1.gldate as date,
            CASE 
                WHEN t1.acccode = '3761001' AND t2.acccode = '2021003' THEN 'SALARY'
                WHEN t1.acccode = '2021039' AND t2.acccode = '1618024' THEN 'PALLIATIVE'
                ELSE 'UNKNOWN'
            END as category,
            CASE WHEN t1.glamount > 0 THEN t1.glamount ELSE t2.glamount END as amount,
            t1.acccode as acccode_1,
            t2.acccode as acccode_2,
            t1.glamount as amount_1,
            t2.glamount as amount_2,
            (t1.glamount + t2.glamount) as sum_check
        FROM "AI DRIVEN DATA"."FIN_GL_2023_RAW" t1
        JOIN "AI DRIVEN DATA"."FIN_GL_2023_RAW" t2 ON (
            t1.gldate = t2.gldate
            AND t1.gldesc = t2.gldesc
            AND (
                (t1.acccode = '3761001' AND t2.acccode = '2021003')
                OR (t1.acccode = '2021039' AND t2.acccode = '1618024')
            )
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM salary_palliative_pairs
    WHERE category != 'UNKNOWN'
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2023"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ SALARY_AND_PALLIATIVE_2023 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_salary_and_palliative_2024(conn):
    """Create SALARY_AND_PALLIATIVE_2024 table"""
    print("📊 Creating SALARY_AND_PALLIATIVE_2024...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2024" AS
    WITH salary_palliative_pairs AS (
        -- AccCode pairs: (3761001 and 2021003) OR (2021039 and 1618024)
        SELECT 
            t1.gldate as date,
            CASE 
                WHEN t1.acccode = '3761001' AND t2.acccode = '2021003' THEN 'SALARY'
                WHEN t1.acccode = '2021039' AND t2.acccode = '1618024' THEN 'PALLIATIVE'
                ELSE 'UNKNOWN'
            END as category,
            CASE WHEN t1.glamount > 0 THEN t1.glamount ELSE t2.glamount END as amount,
            t1.acccode as acccode_1,
            t2.acccode as acccode_2,
            t1.glamount as amount_1,
            t2.glamount as amount_2,
            (t1.glamount + t2.glamount) as sum_check
        FROM "AI DRIVEN DATA"."FIN_GL_2024_RAW" t1
        JOIN "AI DRIVEN DATA"."FIN_GL_2024_RAW" t2 ON (
            t1.gldate = t2.gldate
            AND t1.gldesc = t2.gldesc
            AND (
                (t1.acccode = '3761001' AND t2.acccode = '2021003')
                OR (t1.acccode = '2021039' AND t2.acccode = '1618024')
            )
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM salary_palliative_pairs
    WHERE category != 'UNKNOWN'
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2024"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ SALARY_AND_PALLIATIVE_2024 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_salary_and_palliative_2025(conn):
    """Create SALARY_AND_PALLIATIVE_2025 table"""
    print("📊 Creating SALARY_AND_PALLIATIVE_2025...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2025" AS
    WITH salary_palliative_pairs AS (
        -- AccCode pairs: (3761001 and 2021003) OR (2021039 and 1618024)
        SELECT 
            t1.gldate as date,
            CASE 
                WHEN t1.acccode = '3761001' AND t2.acccode = '2021003' THEN 'SALARY'
                WHEN t1.acccode = '2021039' AND t2.acccode = '1618024' THEN 'PALLIATIVE'
                ELSE 'UNKNOWN'
            END as category,
            CASE WHEN t1.glamount > 0 THEN t1.glamount ELSE t2.glamount END as amount,
            t1.acccode as acccode_1,
            t2.acccode as acccode_2,
            t1.glamount as amount_1,
            t2.glamount as amount_2,
            (t1.glamount + t2.glamount) as sum_check
        FROM "AI DRIVEN DATA"."FIN_GL_2025_RAW" t1
        JOIN "AI DRIVEN DATA"."FIN_GL_2025_RAW" t2 ON (
            t1.gldate = t2.gldate
            AND t1.gldesc = t2.gldesc
            AND (
                (t1.acccode = '3761001' AND t2.acccode = '2021003')
                OR (t1.acccode = '2021039' AND t2.acccode = '1618024')
            )
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM salary_palliative_pairs
    WHERE category != 'UNKNOWN'
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2025"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ SALARY_AND_PALLIATIVE_2025 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_salary_and_palliative_combined(conn):
    """Create combined SALARY_AND_PALLIATIVE table"""
    print("📊 Creating Combined SALARY_AND_PALLIATIVE table...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE" AS
    SELECT 
        date,
        category,
        amount,
        2023 as year
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2023"
    
    UNION ALL
    
    SELECT 
        date,
        category,
        amount,
        2024 as year
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2024"
    
    UNION ALL
    
    SELECT 
        date,
        category,
        amount,
        2025 as year
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_2025"
    
    ORDER BY year, date, category
    '''
    
    conn.execute(query)
    
    # Get overall summary
    summary_query = '''
    SELECT 
        year,
        category,
        COUNT(*) as total_transactions,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE"
    GROUP BY year, category
    ORDER BY year, total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ Combined SALARY_AND_PALLIATIVE table created successfully!")
    print(f"📊 Overall Summary by Year and Category:")
    for row in summary:
        year, category, count, total = row
        print(f"  {year} - {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

if __name__ == '__main__':
    main()
