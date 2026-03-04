#!/usr/bin/env python3
"""
Create Materialized Tables for Performance Optimization
========================================================

Creates pre-aggregated tables for frequently accessed metrics to improve query performance.
These tables are refreshed periodically (e.g., daily) to keep data current.

Based on ChatGPT advice: "Pre-aggregate data (VERY important)"
"""

import duckdb
from datetime import datetime
from core.database import get_db_connection
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_materialized_tables():
    """Create materialized tables for common aggregations"""
    conn = get_db_connection()
    
    try:
        logger.info("Creating materialized tables for performance optimization...")
        
        # 1. PA Monthly Summary (by year-month)
        logger.info("Creating PA_MONTHLY_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."PA_MONTHLY_SUMMARY" AS
            SELECT 
                EXTRACT(YEAR FROM requestdate) as year,
                EXTRACT(MONTH FROM requestdate) as month,
                COUNT(DISTINCT panumber) as unique_panumbers,
                COUNT(DISTINCT IID) as unique_enrollees,
                COUNT(DISTINCT groupname) as unique_companies,
                COUNT(DISTINCT providerid) as unique_providers,
                COALESCE(SUM(granted), 0) as total_granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE granted > 0
            GROUP BY EXTRACT(YEAR FROM requestdate), EXTRACT(MONTH FROM requestdate)
        """)
        logger.info("✅ Created PA_MONTHLY_SUMMARY")
        
        # 2. Claims Monthly Summary (by year-month)
        logger.info("Creating CLAIMS_MONTHLY_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."CLAIMS_MONTHLY_SUMMARY" AS
            SELECT 
                EXTRACT(YEAR FROM datesubmitted) as year,
                EXTRACT(MONTH FROM datesubmitted) as month,
                COUNT(*) as claim_count,
                COUNT(DISTINCT enrollee_id) as unique_enrollees,
                COUNT(DISTINCT nhisgroupid) as unique_companies,
                COUNT(DISTINCT nhisproviderid) as unique_providers,
                COALESCE(SUM(approvedamount), 0) as total_approved,
                COALESCE(SUM(deniedamount), 0) as total_denied
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE approvedamount > 0 OR deniedamount > 0
            GROUP BY EXTRACT(YEAR FROM datesubmitted), EXTRACT(MONTH FROM datesubmitted)
        """)
        logger.info("✅ Created CLAIMS_MONTHLY_SUMMARY")
        
        # 3. Client PA Summary (by client, year)
        logger.info("Creating CLIENT_PA_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."CLIENT_PA_SUMMARY" AS
            SELECT 
                groupname as client_name,
                EXTRACT(YEAR FROM requestdate) as year,
                COUNT(DISTINCT panumber) as visit_count,
                COUNT(DISTINCT IID) as unique_enrollees,
                COUNT(DISTINCT providerid) as unique_providers,
                COALESCE(SUM(granted), 0) as total_cost
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE granted > 0
                AND groupname IS NOT NULL
            GROUP BY groupname, EXTRACT(YEAR FROM requestdate)
        """)
        logger.info("✅ Created CLIENT_PA_SUMMARY")
        
        # 4. Provider PA Summary (by provider, year)
        logger.info("Creating PROVIDER_PA_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."PROVIDER_PA_SUMMARY" AS
            SELECT 
                providerid,
                EXTRACT(YEAR FROM requestdate) as year,
                COUNT(DISTINCT panumber) as visit_count,
                COUNT(DISTINCT IID) as unique_enrollees,
                COUNT(DISTINCT groupname) as unique_clients,
                COALESCE(SUM(granted), 0) as total_cost
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE granted > 0
                AND providerid IS NOT NULL
            GROUP BY providerid, EXTRACT(YEAR FROM requestdate)
        """)
        logger.info("✅ Created PROVIDER_PA_SUMMARY")
        
        # 5. Procedure PA Summary (by procedure, year)
        logger.info("Creating PROCEDURE_PA_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."PROCEDURE_PA_SUMMARY" AS
            SELECT 
                code as procedurecode,
                EXTRACT(YEAR FROM requestdate) as year,
                COUNT(*) as procedure_count,
                COUNT(DISTINCT panumber) as unique_visits,
                COUNT(DISTINCT IID) as unique_enrollees,
                COALESCE(SUM(granted), 0) as total_cost
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE granted > 0
                AND code IS NOT NULL
            GROUP BY code, EXTRACT(YEAR FROM requestdate)
        """)
        logger.info("✅ Created PROCEDURE_PA_SUMMARY")
        
        # 6. Unclaimed PA Summary (by year)
        logger.info("Creating UNCLAIMED_PA_SUMMARY table...")
        conn.execute("""
            CREATE OR REPLACE TABLE "AI DRIVEN DATA"."UNCLAIMED_PA_SUMMARY" AS
            WITH pa_issued AS (
                SELECT
                    CAST(panumber AS BIGINT) as panumber,
                    EXTRACT(YEAR FROM requestdate) as year,
                    SUM(granted) as granted
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE panumber IS NOT NULL
                    AND granted > 0
                GROUP BY CAST(panumber AS BIGINT), EXTRACT(YEAR FROM requestdate)
            ),
            claims_filed AS (
                SELECT DISTINCT 
                    CAST(panumber AS BIGINT) as panumber,
                    EXTRACT(YEAR FROM datesubmitted) as year
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE panumber IS NOT NULL
            )
            SELECT
                p.year,
                COUNT(DISTINCT p.panumber) as unclaimed_pa_count,
                COALESCE(SUM(p.granted), 0) as unclaimed_pa_cost
            FROM pa_issued p
            LEFT JOIN claims_filed c ON p.panumber = c.panumber AND p.year = c.year
            WHERE c.panumber IS NULL
            GROUP BY p.year
        """)
        logger.info("✅ Created UNCLAIMED_PA_SUMMARY")
        
        logger.info("✅ All materialized tables created successfully!")
        logger.info("💡 Tip: Refresh these tables daily using: python create_materialized_tables.py")
        
    except Exception as e:
        logger.error(f"❌ Error creating materialized tables: {e}")
        raise
    finally:
        # Note: Don't close connection - it's pooled
        pass

if __name__ == "__main__":
    create_materialized_tables()
