#!/usr/bin/env python3
"""
Create EXPENSE AND COMMISSION tables for 2023, 2024, and 2025
Derived from FIN_GL_2023_RAW, FIN_GL_2024_RAW, and FIN_GL_2025_RAW
"""

import duckdb
import pandas as pd

def create_expense_commission_2023(conn):
    """Create EXPENSE AND COMMISSION 2023 table"""
    print("📊 Creating EXPENSE AND COMMISSION 2023...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2023" AS
    WITH expense_commission_pairs AS (
        -- MOTOR VEHICLE REPAIR: 3862400 and 1618006
        SELECT 
            t1.gldate as date,
            'MOTOR VEHICLE REPAIR' as category,
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
            AND t1.acccode = '3862400'
            AND t2.acccode = '1618006'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SECURITY: 2021066 and 1618024
        SELECT 
            t1.gldate as date,
            'SECURITY' as category,
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
            AND t1.acccode = '2021066'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SUBSCRIPTION: 2021045 and 1618024
        SELECT 
            t1.gldate as date,
            'SUBSCRIPTION' as category,
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
            AND t1.acccode = '2021045'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS REMUNERATION: 2021002 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS REMUNERATION' as category,
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
            AND t1.acccode = '2021002'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS ALLOWANCES: 2021054 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS ALLOWANCES' as category,
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
            AND t1.acccode = '2021054'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- POSTAGES & TELEPHONE: 2021043 and 1618024
        SELECT 
            t1.gldate as date,
            'POSTAGES & TELEPHONE' as category,
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
            AND t1.acccode = '2021043'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ELECTRICITY & RATES: 2021065 and 1618024
        SELECT 
            t1.gldate as date,
            'ELECTRICITY & RATES' as category,
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
            AND t1.acccode = '2021065'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- PRINTING & STATIONERY: 2021047 and 1618024
        SELECT 
            t1.gldate as date,
            'PRINTING & STATIONERY' as category,
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
            AND t1.acccode = '2021047'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- REPAIRS & MAINTENANCE: 2021040 and 1618024
        SELECT 
            t1.gldate as date,
            'REPAIRS & MAINTENANCE' as category,
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
            AND t1.acccode = '2021040'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- GENERATOR RUNNING: 2021041 and 1618024
        SELECT 
            t1.gldate as date,
            'GENERATOR RUNNING' as category,
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
            AND t1.acccode = '2021041'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MOTOR RUNNING EXPENSES: 2021060 and 1618024
        SELECT 
            t1.gldate as date,
            'MOTOR RUNNING EXPENSES' as category,
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
            AND t1.acccode = '2021060'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- TRANSPORT & TRAVELLING: 2021042 and 1618024
        SELECT 
            t1.gldate as date,
            'TRANSPORT & TRAVELLING' as category,
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
            AND t1.acccode = '2021042'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ADVERTISING: 2021044 and 1618024
        SELECT 
            t1.gldate as date,
            'ADVERTISING' as category,
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
            AND t1.acccode = '2021044'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- RENT & RATES: 2021011 and 1618024
        SELECT 
            t1.gldate as date,
            'RENT & RATES' as category,
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
            AND t1.acccode = '2021011'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- COMMISSION: 2021059 and 1618037
        SELECT 
            t1.gldate as date,
            'COMMISSION' as category,
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
            AND t1.acccode = '2021059'
            AND t2.acccode = '1618037'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MGT ALLOWANCE: 2021055 and 1618024
        SELECT 
            t1.gldate as date,
            'MGT ALLOWANCE' as category,
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
            AND t1.acccode = '2021055'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- WELFARE: 2021039 and 1618024
        SELECT 
            t1.gldate as date,
            'WELFARE' as category,
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
            AND t1.acccode = '2021039'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM expense_commission_pairs
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2023"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ EXPENSE_AND_COMMISSION_2023 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_expense_commission_2024(conn):
    """Create EXPENSE AND COMMISSION 2024 table"""
    print("📊 Creating EXPENSE AND COMMISSION 2024...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2024" AS
    WITH expense_commission_pairs AS (
        -- MOTOR VEHICLE REPAIR: 3862400 and 1618006
        SELECT 
            t1.gldate as date,
            'MOTOR VEHICLE REPAIR' as category,
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
            AND t1.acccode = '3862400'
            AND t2.acccode = '1618006'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SECURITY: 2021066 and 1618024
        SELECT 
            t1.gldate as date,
            'SECURITY' as category,
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
            AND t1.acccode = '2021066'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SUBSCRIPTION: 2021045 and 1618024
        SELECT 
            t1.gldate as date,
            'SUBSCRIPTION' as category,
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
            AND t1.acccode = '2021045'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS REMUNERATION: 2021002 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS REMUNERATION' as category,
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
            AND t1.acccode = '2021002'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS ALLOWANCES: 2021054 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS ALLOWANCES' as category,
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
            AND t1.acccode = '2021054'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- POSTAGES & TELEPHONE: 2021043 and 1618024
        SELECT 
            t1.gldate as date,
            'POSTAGES & TELEPHONE' as category,
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
            AND t1.acccode = '2021043'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ELECTRICITY & RATES: 2021065 and 1618024
        SELECT 
            t1.gldate as date,
            'ELECTRICITY & RATES' as category,
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
            AND t1.acccode = '2021065'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- PRINTING & STATIONERY: 2021047 and 1618024
        SELECT 
            t1.gldate as date,
            'PRINTING & STATIONERY' as category,
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
            AND t1.acccode = '2021047'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- REPAIRS & MAINTENANCE: 2021040 and 1618024
        SELECT 
            t1.gldate as date,
            'REPAIRS & MAINTENANCE' as category,
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
            AND t1.acccode = '2021040'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- GENERATOR RUNNING: 2021041 and 1618024
        SELECT 
            t1.gldate as date,
            'GENERATOR RUNNING' as category,
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
            AND t1.acccode = '2021041'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MOTOR RUNNING EXPENSES: 2021060 and 1618024
        SELECT 
            t1.gldate as date,
            'MOTOR RUNNING EXPENSES' as category,
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
            AND t1.acccode = '2021060'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- TRANSPORT & TRAVELLING: 2021042 and 1618024
        SELECT 
            t1.gldate as date,
            'TRANSPORT & TRAVELLING' as category,
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
            AND t1.acccode = '2021042'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ADVERTISING: 2021044 and 1618024
        SELECT 
            t1.gldate as date,
            'ADVERTISING' as category,
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
            AND t1.acccode = '2021044'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- RENT & RATES: 2021011 and 1618024
        SELECT 
            t1.gldate as date,
            'RENT & RATES' as category,
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
            AND t1.acccode = '2021011'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- COMMISSION: 2021059 and 1618037
        SELECT 
            t1.gldate as date,
            'COMMISSION' as category,
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
            AND t1.acccode = '2021059'
            AND t2.acccode = '1618037'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MGT ALLOWANCE: 2021055 and 1618024
        SELECT 
            t1.gldate as date,
            'MGT ALLOWANCE' as category,
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
            AND t1.acccode = '2021055'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- WELFARE: 2021039 and 1618024
        SELECT 
            t1.gldate as date,
            'WELFARE' as category,
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
            AND t1.acccode = '2021039'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM expense_commission_pairs
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2024"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ EXPENSE_AND_COMMISSION_2024 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_expense_commission_2025(conn):
    """Create EXPENSE AND COMMISSION 2025 table"""
    print("📊 Creating EXPENSE AND COMMISSION 2025...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2025" AS
    WITH expense_commission_pairs AS (
        -- MOTOR VEHICLE REPAIR: 3862400 and 1618006
        SELECT 
            t1.gldate as date,
            'MOTOR VEHICLE REPAIR' as category,
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
            AND t1.acccode = '3862400'
            AND t2.acccode = '1618006'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SECURITY: 2021066 and 1618024
        SELECT 
            t1.gldate as date,
            'SECURITY' as category,
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
            AND t1.acccode = '2021066'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- SUBSCRIPTION: 2021045 and 1618024
        SELECT 
            t1.gldate as date,
            'SUBSCRIPTION' as category,
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
            AND t1.acccode = '2021045'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS REMUNERATION: 2021002 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS REMUNERATION' as category,
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
            AND t1.acccode = '2021002'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- DIRECTORS ALLOWANCES: 2021054 and 1618024
        SELECT 
            t1.gldate as date,
            'DIRECTORS ALLOWANCES' as category,
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
            AND t1.acccode = '2021054'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- POSTAGES & TELEPHONE: 2021043 and 1618024
        SELECT 
            t1.gldate as date,
            'POSTAGES & TELEPHONE' as category,
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
            AND t1.acccode = '2021043'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ELECTRICITY & RATES: 2021065 and 1618024
        SELECT 
            t1.gldate as date,
            'ELECTRICITY & RATES' as category,
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
            AND t1.acccode = '2021065'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- PRINTING & STATIONERY: 2021047 and 1618024
        SELECT 
            t1.gldate as date,
            'PRINTING & STATIONERY' as category,
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
            AND t1.acccode = '2021047'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- REPAIRS & MAINTENANCE: 2021040 and 1618024
        SELECT 
            t1.gldate as date,
            'REPAIRS & MAINTENANCE' as category,
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
            AND t1.acccode = '2021040'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- GENERATOR RUNNING: 2021041 and 1618024
        SELECT 
            t1.gldate as date,
            'GENERATOR RUNNING' as category,
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
            AND t1.acccode = '2021041'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MOTOR RUNNING EXPENSES: 2021060 and 1618024
        SELECT 
            t1.gldate as date,
            'MOTOR RUNNING EXPENSES' as category,
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
            AND t1.acccode = '2021060'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- TRANSPORT & TRAVELLING: 2021042 and 1618024
        SELECT 
            t1.gldate as date,
            'TRANSPORT & TRAVELLING' as category,
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
            AND t1.acccode = '2021042'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- ADVERTISING: 2021044 and 1618024
        SELECT 
            t1.gldate as date,
            'ADVERTISING' as category,
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
            AND t1.acccode = '2021044'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- RENT & RATES: 2021011 and 1618024
        SELECT 
            t1.gldate as date,
            'RENT & RATES' as category,
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
            AND t1.acccode = '2021011'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- COMMISSION: 2021059 and 1618037
        SELECT 
            t1.gldate as date,
            'COMMISSION' as category,
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
            AND t1.acccode = '2021059'
            AND t2.acccode = '1618037'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- MGT ALLOWANCE: 2021055 and 1618024
        SELECT 
            t1.gldate as date,
            'MGT ALLOWANCE' as category,
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
            AND t1.acccode = '2021055'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
        
        UNION ALL
        
        -- WELFARE: 2021039 and 1618024
        SELECT 
            t1.gldate as date,
            'WELFARE' as category,
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
            AND t1.acccode = '2021039'
            AND t2.acccode = '1618024'
            AND (t1.glamount + t2.glamount) = 0
            AND t1.glid < t2.glid
        )
    )
    
    SELECT 
        date,
        category,
        amount
    FROM expense_commission_pairs
    ORDER BY date, category
    '''
    
    conn.execute(query)
    
    # Get summary
    summary_query = '''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2025"
    GROUP BY category
    ORDER BY total_amount DESC
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ EXPENSE_AND_COMMISSION_2025 created successfully!")
    print(f"📊 Summary by category:")
    for row in summary:
        category, count, total = row
        print(f"  {category}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def create_combined_expense_commission(conn):
    """Create combined EXPENSE AND COMMISSION table"""
    print("📊 Creating Combined EXPENSE AND COMMISSION table...")
    
    query = '''
    CREATE OR REPLACE TABLE "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION" AS
    SELECT 
        date,
        category,
        amount,
        2023 as year
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2023"
    
    UNION ALL
    
    SELECT 
        date,
        category,
        amount,
        2024 as year
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2024"
    
    UNION ALL
    
    SELECT 
        date,
        category,
        amount,
        2025 as year
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_2025"
    
    ORDER BY year, date, category
    '''
    
    conn.execute(query)
    
    # Get overall summary
    summary_query = '''
    SELECT 
        year,
        COUNT(*) as total_transactions,
        SUM(amount) as total_amount
    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION"
    GROUP BY year
    ORDER BY year
    '''
    
    summary = conn.execute(summary_query).fetchall()
    
    print(f"✅ Combined EXPENSE_AND_COMMISSION table created successfully!")
    print(f"📊 Overall Summary by Year:")
    for row in summary:
        year, count, total = row
        print(f"  {year}: {count} transactions, ₦{total:,.2f}")
    
    return summary

def main():
    """Main function to create all EXPENSE AND COMMISSION tables"""
    print("🚀 Creating EXPENSE AND COMMISSION Tables...")
    print("=" * 60)
    
    # Connect to DuckDB
    conn = duckdb.connect('ai_driven_data.duckdb')
    
    try:
        # Create individual year tables
        summary_2023 = create_expense_commission_2023(conn)
        print()
        
        summary_2024 = create_expense_commission_2024(conn)
        print()
        
        summary_2025 = create_expense_commission_2025(conn)
        print()
        
        # Create combined table
        combined_summary = create_combined_expense_commission(conn)
        print()
        
        print("🎉 All EXPENSE AND COMMISSION tables created successfully!")
        print("=" * 60)
        print("📋 Tables created:")
        print("  - EXPENSE_AND_COMMISSION_2023")
        print("  - EXPENSE_AND_COMMISSION_2024") 
        print("  - EXPENSE_AND_COMMISSION_2025")
        print("  - EXPENSE_AND_COMMISSION (combined)")
        
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        raise
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
