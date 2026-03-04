"""
COMPLETE CALCULATION ENGINE
============================

All Python data extraction and metric calculations.
NO AI - pure Python performance.

This file contains all the functions needed to extract data from DuckDB
and calculate every metric shown in Godrej/Alert/NAHCO analyses.
"""

import pandas as pd
import numpy as np
import duckdb
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class CalculationEngine:
    """Complete Python calculation engine - handles all data processing"""

    def __init__(self,
                 use_motherduck: bool = False,
                 motherduck_token: str = None,
                 db_path: str = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'):
        """
        Initialize CalculationEngine with optional MotherDuck support

        Args:
            use_motherduck: If True, connect to MotherDuck cloud database
            motherduck_token: MotherDuck authentication token (required if use_motherduck=True)
            db_path: Path to local DuckDB file (used if use_motherduck=False)
        """
        self.use_motherduck = use_motherduck

        if use_motherduck:
            # Use MotherDuck (cloud)
            if not motherduck_token:
                # Try to load from motherduck.py if available
                try:
                    import sys
                    import importlib.util
                    spec = importlib.util.spec_from_file_location(
                        "motherduck_config",
                        "/Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py"
                    )
                    motherduck_config = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(motherduck_config)
                    motherduck_token = motherduck_config.MOTHERDUCK_TOKEN
                except Exception as e:
                    raise ValueError(f"MotherDuck token required when use_motherduck=True. Error loading from motherduck.py: {e}")

            self.db_path = f'md:?motherduck_token={motherduck_token}'
            self.motherduck_db = 'ai_driven_data'
            print("🌐 Using MotherDuck (cloud)")
        else:
            # Use local DuckDB
            self.db_path = db_path
            self.motherduck_db = None
            print("💻 Using local DuckDB")

        self.conn = None

    def connect(self):
        """Establish database connection"""
        if self.use_motherduck:
            # Connect to MotherDuck
            self.conn = duckdb.connect(self.db_path, read_only=True)
            self.conn.execute(f"USE {self.motherduck_db}")
            print(f"✅ Connected to MotherDuck database: {self.motherduck_db}")
        else:
            # Connect to local DuckDB
            if not Path(self.db_path).exists():
                raise FileNotFoundError(f"Database not found: {self.db_path}")
            self.conn = duckdb.connect(self.db_path, read_only=True)
            print(f"✅ Connected to local database: {self.db_path}")

        return self.conn
    
    def get_company_contract_details(self, company_name: str) -> Dict:
        """Get current contract details"""
        if not self.conn:
            self.connect()
        
        query = """
        SELECT 
            groupname,
            startdate,
            enddate,
            iscurrent,
            CAST(DATEDIFF('month', startdate, enddate) AS INTEGER) as total_months,
            CAST(DATEDIFF('month', CURRENT_DATE, enddate) AS INTEGER) as months_remaining,
            CAST(DATEDIFF('month', startdate, CURRENT_DATE) AS INTEGER) as months_elapsed
        FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
        WHERE groupname = ?
            AND iscurrent = 1
        """
        
        df = self.conn.execute(query, [company_name]).fetchdf()
        
        if df.empty:
            return {'success': False, 'error': 'No current contract found'}
        
        contract = df.iloc[0].to_dict()
        
        # Get group ID
        group_query = """
        SELECT CAST(groupid AS VARCHAR) as groupid
        FROM "AI DRIVEN DATA"."GROUPS"
        WHERE groupname = ?
        """
        
        group_df = self.conn.execute(group_query, [company_name]).fetchdf()
        if group_df.empty:
            return {'success': False, 'error': 'Group not found'}
        
        groupid = str(group_df.iloc[0]['groupid'])
        
        return {
            'success': True,
            'company_name': company_name,
            'groupid': groupid,
            'startdate': contract['startdate'],
            'enddate': contract['enddate'],
            'total_months': max(int(contract['total_months']), 1),
            'months_remaining': max(int(contract['months_remaining']), 0),
            'months_elapsed': max(int(contract['months_elapsed']), 1)
        }
    
    def get_financial_data(self, company_name: str, start_date, end_date,
                          debit_override: Optional[float] = None,
                          cash_override: Optional[float] = None) -> Dict:
        """Get financial data with optional overrides"""
        if not self.conn:
            self.connect()
        
        # Get debit note
        if debit_override is None:
            debit_query = """
            SELECT 
                SUM(Amount) as total_debit
            FROM "AI DRIVEN DATA"."DEBIT_NOTE"
            WHERE CompanyName = ?
                AND "From" >= ?
                AND "From" <= ?
                AND (LOWER(Description) NOT LIKE '%tpa%' OR Description IS NULL)
            """
            
            debit_df = self.conn.execute(debit_query, [company_name, start_date, end_date]).fetchdf()
            total_debit = float(debit_df.iloc[0]['total_debit']) if not debit_df.empty and pd.notna(debit_df.iloc[0]['total_debit']) else 0.0
        else:
            total_debit = float(debit_override)
        
        # Get cash received
        if cash_override is None:
            cash_query = """
            SELECT 
                SUM(Amount) as total_cash
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
            WHERE groupname = ?
                AND Date >= ?
                AND Date <= ?
            """
            
            cash_df = self.conn.execute(cash_query, [company_name, start_date, end_date]).fetchdf()
            total_cash = float(cash_df.iloc[0]['total_cash']) if not cash_df.empty and pd.notna(cash_df.iloc[0]['total_cash']) else 0.0
        else:
            total_cash = float(cash_override)
        
        # Determine commission rate
        if company_name == 'CARAWAY AFRICA NIGERIA LIMITED':
            commission_rate = 0.12
        elif company_name == 'POLARIS BANK PLC':
            commission_rate = 0.15
        elif company_name == 'LORNA NIGERIA LIMITED (GODREJ)':
            commission_rate = 0.20
        else:
            commission_rate = 0.10
        
        commission_amount = total_debit * commission_rate
        payment_rate = (total_cash / total_debit * 100) if total_debit > 0 else 0

        # Collection rate analysis and AR aging
        collection_analysis = {
            'rate_30days': 0,
            'rate_60days': 0,
            'rate_90days': 0,
            'aging_buckets': {
                'current': 0,
                '30_60': 0,
                '60_90': 0,
                'over_90': 0
            },
            'risk_level': 'LOW',
            'uncollected_pct': 0
        }

        try:
            # Get detailed aging data by joining debit notes with cash received
            aging_query = """
            WITH debit_cash AS (
                SELECT
                    d.Amount as debit_amount,
                    d."From" as debit_date,
                    c.Date as payment_date,
                    c.Amount as cash_amount,
                    CASE
                        WHEN c.Date IS NOT NULL
                        THEN DATEDIFF('day', d."From", c.Date)
                        ELSE DATEDIFF('day', d."From", CURRENT_DATE)
                    END as days_to_payment
                FROM "AI DRIVEN DATA"."DEBIT_NOTE" d
                LEFT JOIN "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED" c
                    ON d.CompanyName = c.groupname
                    AND c.Date >= d."From"
                    AND c.Date <= DATEADD('day', 120, d."From")
                WHERE d.CompanyName = ?
                    AND d."From" >= ?
                    AND d."From" <= ?
                    AND (LOWER(d.Description) NOT LIKE '%tpa%' OR d.Description IS NULL)
            )
            SELECT
                SUM(CASE WHEN cash_amount IS NOT NULL AND days_to_payment <= 30 THEN cash_amount ELSE 0 END) as collected_30,
                SUM(CASE WHEN cash_amount IS NOT NULL AND days_to_payment <= 60 THEN cash_amount ELSE 0 END) as collected_60,
                SUM(CASE WHEN cash_amount IS NOT NULL AND days_to_payment <= 90 THEN cash_amount ELSE 0 END) as collected_90,
                SUM(CASE WHEN cash_amount IS NULL AND days_to_payment < 30 THEN debit_amount ELSE 0 END) as current_bucket,
                SUM(CASE WHEN cash_amount IS NULL AND days_to_payment BETWEEN 30 AND 59 THEN debit_amount ELSE 0 END) as bucket_30_60,
                SUM(CASE WHEN cash_amount IS NULL AND days_to_payment BETWEEN 60 AND 89 THEN debit_amount ELSE 0 END) as bucket_60_90,
                SUM(CASE WHEN cash_amount IS NULL AND days_to_payment >= 90 THEN debit_amount ELSE 0 END) as bucket_over_90,
                SUM(debit_amount) as total_debit_check
            FROM debit_cash
            """

            aging_df = self.conn.execute(aging_query, [company_name, start_date, end_date]).fetchdf()

            if not aging_df.empty and aging_df.iloc[0]['total_debit_check'] is not None:
                row = aging_df.iloc[0]
                total_debit_check = float(row['total_debit_check']) if row['total_debit_check'] else total_debit

                # Collection rates
                collection_analysis['rate_30days'] = round((float(row['collected_30']) / total_debit_check * 100), 1) if total_debit_check > 0 else 0
                collection_analysis['rate_60days'] = round((float(row['collected_60']) / total_debit_check * 100), 1) if total_debit_check > 0 else 0
                collection_analysis['rate_90days'] = round((float(row['collected_90']) / total_debit_check * 100), 1) if total_debit_check > 0 else 0

                # Aging buckets
                collection_analysis['aging_buckets']['current'] = float(row['current_bucket']) if row['current_bucket'] else 0
                collection_analysis['aging_buckets']['30_60'] = float(row['bucket_30_60']) if row['bucket_30_60'] else 0
                collection_analysis['aging_buckets']['60_90'] = float(row['bucket_60_90']) if row['bucket_60_90'] else 0
                collection_analysis['aging_buckets']['over_90'] = float(row['bucket_over_90']) if row['bucket_over_90'] else 0

                # Risk level based on overall collection rate
                if payment_rate >= 90:
                    collection_analysis['risk_level'] = 'LOW'
                elif payment_rate >= 70:
                    collection_analysis['risk_level'] = 'MEDIUM'
                else:
                    collection_analysis['risk_level'] = 'HIGH'

                collection_analysis['uncollected_pct'] = round(100 - payment_rate, 1)

        except Exception as e:
            # If aging analysis fails, use basic payment rate
            collection_analysis['risk_level'] = 'LOW' if payment_rate >= 90 else 'MEDIUM' if payment_rate >= 70 else 'HIGH'
            collection_analysis['uncollected_pct'] = round(100 - payment_rate, 1)

        return {
            'success': True,
            'debit': total_debit,
            'cash': total_cash,
            'commission_rate': commission_rate,
            'commission': commission_amount,
            'payment_rate': payment_rate,
            'outstanding': total_debit - total_cash,
            'collection_analysis': collection_analysis
        }
    
    def get_claims_data(self, groupid: str, start_date, end_date) -> Dict:
        """Get claims analysis data"""
        if not self.conn:
            self.connect()
        
        claims_query = """
        SELECT 
            COUNT(*) as total_claims,
            COUNT(DISTINCT enrollee_id) as unique_claimed,
            COUNT(DISTINCT nhisproviderid) as unique_providers,
            SUM(approvedamount) as total_cost,
            AVG(approvedamount) as avg_cost,
            MAX(approvedamount) as max_cost
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE CAST(nhisgroupid AS VARCHAR) = ?
            AND datesubmitted >= ?
            AND datesubmitted <= ?
            AND approvedamount > 0
        """
        
        claims_df = self.conn.execute(claims_query, [groupid, start_date, end_date]).fetchdf()
        
        if claims_df.empty:
            return {
                'success': True,
                'total_claims': 0,
                'unique_claimed': 0,
                'unique_providers': 0,
                'total_cost': 0.0,
                'avg_cost': 0.0,
                'max_cost': 0.0
            }
        
        result = claims_df.iloc[0].to_dict()
        result['success'] = True
        
        # Convert numpy types to Python types
        for key in result:
            if isinstance(result[key], (np.integer, np.floating)):
                result[key] = float(result[key]) if isinstance(result[key], np.floating) else int(result[key])
        
        return result
    
    def get_pa_data(self, company_name: str, groupid: str, start_date, end_date) -> Dict:
        """Get Prior Authorization data"""
        if not self.conn:
            self.connect()
        
        pa_query = """
        SELECT 
            COUNT(*) as total_pa_count,
            SUM(granted) as total_pa_granted,
            AVG(granted) as avg_pa_amount,
            COUNT(DISTINCT IID) as unique_members_pa
        FROM "AI DRIVEN DATA"."PA DATA"
        WHERE groupname = ?
            AND requestdate >= ?
            AND requestdate <= ?
            AND granted > 0
        """
        
        pa_df = self.conn.execute(pa_query, [company_name, start_date, end_date]).fetchdf()
        
        if pa_df.empty or pd.isna(pa_df.iloc[0]['total_pa_granted']):
            return {
                'success': True,
                'total_pa_count': 0,
                'total_pa_granted': 0.0,
                'avg_pa_amount': 0.0,
                'unique_members_pa': 0,
                'unclaimed_count': 0,
                'unclaimed_amount': 0.0
            }
        
        result = pa_df.iloc[0].to_dict()
        
        # Calculate unclaimed PA
        unclaimed_query = """
        WITH pa_issued AS (
            SELECT
                CAST(panumber AS BIGINT) as panumber,
                SUM(granted) as granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE groupname = ?
                AND requestdate >= ?
                AND requestdate <= ?
                AND panumber IS NOT NULL
                AND granted > 0
            GROUP BY CAST(panumber AS BIGINT)
        ),
        claims_filed AS (
            SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE CAST(nhisgroupid AS VARCHAR) = ?
                AND datesubmitted >= ?
                AND datesubmitted <= ?
                AND panumber IS NOT NULL
        )
        SELECT
            COUNT(*) as unclaimed_count,
            COALESCE(SUM(p.granted), 0) as unclaimed_amount
        FROM pa_issued p
        LEFT JOIN claims_filed c ON p.panumber = c.panumber
        WHERE c.panumber IS NULL
        """

        unclaimed_df = self.conn.execute(unclaimed_query, [company_name, start_date, end_date, groupid, start_date, end_date]).fetchdf()
        
        if not unclaimed_df.empty:
            result['unclaimed_count'] = int(unclaimed_df.iloc[0]['unclaimed_count'])
            result['unclaimed_amount'] = float(unclaimed_df.iloc[0]['unclaimed_amount'])
        else:
            result['unclaimed_count'] = 0
            result['unclaimed_amount'] = 0.0

        # PA Effectiveness Metrics
        pa_effectiveness = {
            'total_requests': 0,
            'approved': 0,
            'denied': 0,
            'approval_rate': 0,
            'conversion_rate': 0,
            'avg_lag_days': 0,
            'cost_variance': {
                'pa_authorized': 0,
                'claims_actual': 0,
                'variance_pct': 0
            },
            'effectiveness_score': 'UNKNOWN'
        }

        try:
            # PA approval metrics
            approval_query = """
            SELECT
                COUNT(*) as total_requests,
                SUM(CASE WHEN granted > 0 THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN granted = 0 OR granted IS NULL THEN 1 ELSE 0 END) as denied
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE groupname = ?
                AND requestdate >= ?
                AND requestdate <= ?
            """

            approval_df = self.conn.execute(approval_query, [company_name, start_date, end_date]).fetchdf()

            if not approval_df.empty:
                pa_effectiveness['total_requests'] = int(approval_df.iloc[0]['total_requests'])
                pa_effectiveness['approved'] = int(approval_df.iloc[0]['approved'])
                pa_effectiveness['denied'] = int(approval_df.iloc[0]['denied'])
                pa_effectiveness['approval_rate'] = round(
                    (pa_effectiveness['approved'] / pa_effectiveness['total_requests'] * 100), 1
                ) if pa_effectiveness['total_requests'] > 0 else 0

            # PA-to-Claims conversion
            conversion_query = """
            WITH pa_approved AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE groupname = ?
                    AND requestdate >= ?
                    AND requestdate <= ?
                    AND panumber IS NOT NULL
                    AND granted > 0
            ),
            pa_claimed AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE CAST(nhisgroupid AS VARCHAR) = ?
                    AND datesubmitted >= ?
                    AND datesubmitted <= ?
                    AND panumber IS NOT NULL
            )
            SELECT
                COUNT(DISTINCT p.panumber) as total_approved_pa,
                COUNT(DISTINCT c.panumber) as converted_pa
            FROM pa_approved p
            LEFT JOIN pa_claimed c ON p.panumber = c.panumber
            """

            conversion_df = self.conn.execute(conversion_query, [
                company_name, start_date, end_date,
                groupid, start_date, end_date
            ]).fetchdf()

            if not conversion_df.empty and conversion_df.iloc[0]['total_approved_pa']:
                total_approved_pa = int(conversion_df.iloc[0]['total_approved_pa'])
                converted_pa = int(conversion_df.iloc[0]['converted_pa'])
                pa_effectiveness['conversion_rate'] = round(
                    (converted_pa / total_approved_pa * 100), 1
                ) if total_approved_pa > 0 else 0

            # Average lag time (PA approval to claim submission)
            lag_query = """
            SELECT
                AVG(DATEDIFF('day', pa.requestdate, c.datesubmitted)) as avg_lag
            FROM "AI DRIVEN DATA"."PA DATA" pa
            INNER JOIN "AI DRIVEN DATA"."CLAIMS DATA" c
                ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
            WHERE pa.groupname = ?
                AND pa.requestdate >= ?
                AND pa.requestdate <= ?
                AND CAST(c.nhisgroupid AS VARCHAR) = ?
                AND c.datesubmitted >= pa.requestdate
            """

            lag_df = self.conn.execute(lag_query, [company_name, start_date, end_date, groupid]).fetchdf()

            if not lag_df.empty and lag_df.iloc[0]['avg_lag'] is not None:
                pa_effectiveness['avg_lag_days'] = round(float(lag_df.iloc[0]['avg_lag']), 0)

            # Cost variance (PA authorized vs actual claims)
            variance_query = """
            WITH pa_costs AS (
                SELECT SUM(granted) as total_pa
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE groupname = ?
                    AND requestdate >= ?
                    AND requestdate <= ?
                    AND granted > 0
            ),
            claims_costs AS (
                SELECT SUM(approvedamount) as total_claims
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE CAST(nhisgroupid AS VARCHAR) = ?
                    AND datesubmitted >= ?
                    AND datesubmitted <= ?
            )
            SELECT
                p.total_pa,
                c.total_claims
            FROM pa_costs p, claims_costs c
            """

            variance_df = self.conn.execute(variance_query, [
                company_name, start_date, end_date,
                groupid, start_date, end_date
            ]).fetchdf()

            if not variance_df.empty:
                pa_authorized = float(variance_df.iloc[0]['total_pa']) if variance_df.iloc[0]['total_pa'] else 0
                claims_actual = float(variance_df.iloc[0]['total_claims']) if variance_df.iloc[0]['total_claims'] else 0

                pa_effectiveness['cost_variance']['pa_authorized'] = pa_authorized
                pa_effectiveness['cost_variance']['claims_actual'] = claims_actual

                if pa_authorized > 0:
                    variance_pct = ((claims_actual - pa_authorized) / pa_authorized) * 100
                    pa_effectiveness['cost_variance']['variance_pct'] = round(variance_pct, 1)

            # Effectiveness score
            approval_rate = pa_effectiveness['approval_rate']
            conversion_rate = pa_effectiveness['conversion_rate']

            if conversion_rate > 70 and approval_rate > 80 and approval_rate < 95:
                pa_effectiveness['effectiveness_score'] = 'GOOD'
            elif conversion_rate < 50 or approval_rate > 95:
                pa_effectiveness['effectiveness_score'] = 'POOR'
            else:
                pa_effectiveness['effectiveness_score'] = 'MODERATE'

        except Exception as e:
            # If PA effectiveness analysis fails, keep defaults
            pa_effectiveness['effectiveness_score'] = 'UNKNOWN'

        result['pa_effectiveness'] = pa_effectiveness
        result['success'] = True
        return result
    
    def get_enrollment_data(self, groupid: str) -> Dict:
        """Get enrollment data"""
        if not self.conn:
            self.connect()
        
        enrollment_query = """
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT planid) as unique_plans
        FROM "AI DRIVEN DATA"."MEMBERS"
        WHERE CAST(groupid AS VARCHAR) = ?
            AND iscurrent = 1
            AND (isterminated = 0 OR isterminated IS NULL)
        """
        
        enroll_df = self.conn.execute(enrollment_query, [groupid]).fetchdf()
        
        if enroll_df.empty:
            return {'success': True, 'total': 0, 'unique_plans': 0}
        
        result = enroll_df.iloc[0].to_dict()
        result['success'] = True
        return result
    
    def calculate_mlr_metrics(self, financial: Dict, claims: Dict, pa: Dict, 
                             enrollment: Dict, months_elapsed: int) -> Dict:
        """Calculate comprehensive MLR metrics"""
        
        medical_cost = claims.get('total_cost', 0) + pa.get('unclaimed_amount', 0)
        commission = financial.get('commission', 0)
        
        # BV-MLR
        debit = financial.get('debit', 0)
        if debit > 0:
            bv_mlr = ((medical_cost + commission) / debit) * 100
        else:
            bv_mlr = None
        
        # CASH-MLR
        cash = financial.get('cash', 0)
        if cash > 0:
            cash_mlr = ((medical_cost + commission) / cash) * 100
        else:
            cash_mlr = None
        
        # PMPM
        enrolled = enrollment.get('total', 0)
        if enrolled > 0 and months_elapsed > 0:
            pmpm = medical_cost / (enrolled * months_elapsed)
        else:
            pmpm = 0
        
        # Monthly burn rate
        monthly_burn = medical_cost / months_elapsed if months_elapsed > 0 else 0
        
        target_pmpm = 3000
        target_mlr = 75
        
        pmpm_variance = ((pmpm - target_pmpm) / target_pmpm * 100) if target_pmpm > 0 else 0
        
        return {
            'medical_cost': medical_cost,
            'claims_cost': claims.get('total_cost', 0),
            'unclaimed_pa': pa.get('unclaimed_amount', 0),
            'commission': commission,
            'commission_rate': financial.get('commission_rate', 0) * 100,
            'bv_mlr': bv_mlr,
            'cash_mlr': cash_mlr,
            'pmpm': pmpm,
            'target_pmpm': target_pmpm,
            'pmpm_variance': pmpm_variance,
            'monthly_burn': monthly_burn,
            'target_mlr': target_mlr,
            'is_profitable': bv_mlr < target_mlr if bv_mlr is not None else False
        }
    
    def analyze_member_concentration(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze high-cost member concentration"""
        if not self.conn:
            self.connect()
        
        # Get top 10 members
        query = """
        SELECT 
            enrollee_id,
            SUM(approvedamount) as total_cost,
            COUNT(*) as claim_count,
            COUNT(DISTINCT panumber) as visit_count
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE CAST(nhisgroupid AS VARCHAR) = ?
            AND datesubmitted >= ?
            AND datesubmitted <= ?
            AND approvedamount > 0
        GROUP BY enrollee_id
        ORDER BY total_cost DESC
        LIMIT 10
        """
        
        top_df = self.conn.execute(query, [groupid, start_date, end_date]).fetchdf()
        
        if top_df.empty:
            return {
                'success': True,
                'type': 'NO DATA',
                'top_members': [],
                'top_5_pct': 0,
                'top_10_pct': 0
            }
        
        # Get total cost
        total_query = """
        SELECT SUM(approvedamount) as total
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE CAST(nhisgroupid AS VARCHAR) = ?
            AND datesubmitted >= ?
            AND datesubmitted <= ?
            AND approvedamount > 0
        """
        
        total_df = self.conn.execute(total_query, [groupid, start_date, end_date]).fetchdf()
        total_cost = float(total_df.iloc[0]['total']) if not total_df.empty else 0
        
        if total_cost > 0:
            top_df['pct_of_total'] = (top_df['total_cost'] / total_cost * 100).round(2)
        else:
            top_df['pct_of_total'] = 0
        
        top_5_pct = float(top_df.head(5)['pct_of_total'].sum())
        top_10_pct = float(top_df['pct_of_total'].sum())
        
        # Determine type
        concentration_type = "EPISODIC" if top_5_pct > 40 else "STRUCTURAL"
        
        return {
            'success': True,
            'type': concentration_type,
            'top_5_pct': top_5_pct,
            'top_10_pct': top_10_pct,
            'top_members': top_df.to_dict('records')
        }
    
    def analyze_conditions(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze condition breakdown"""
        if not self.conn:
            self.connect()
        
        query = """
        WITH claims_with_diagnosis AS (
            SELECT 
                c.approvedamount,
                c.diagnosiscode,
                d.diagnosisdesc
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
            WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
                AND c.datesubmitted >= ?
                AND c.datesubmitted <= ?
                AND c.approvedamount > 0
        ),
        categorized AS (
            SELECT 
                approvedamount,
                CASE
                    -- Maternity
                    WHEN diagnosiscode LIKE 'O%' OR LOWER(diagnosisdesc) LIKE '%cesarean%' 
                        OR LOWER(diagnosisdesc) LIKE '%delivery%' OR LOWER(diagnosisdesc) LIKE '%pregnancy%'
                        OR LOWER(diagnosisdesc) LIKE '%maternal%' OR LOWER(diagnosisdesc) LIKE '%antenatal%'
                        THEN 'Maternity'
                    -- Surgery
                    WHEN LOWER(diagnosisdesc) LIKE '%appendicitis%' OR LOWER(diagnosisdesc) LIKE '%hernia%'
                        OR LOWER(diagnosisdesc) LIKE '%gallbladder%' OR LOWER(diagnosisdesc) LIKE '%cholecyst%'
                        OR diagnosiscode LIKE 'K35%' OR diagnosiscode LIKE 'K40%' OR diagnosiscode LIKE 'K41%'
                        THEN 'Surgery'
                    -- Trauma
                    WHEN diagnosiscode LIKE 'S%' OR diagnosiscode LIKE 'T%'
                        OR LOWER(diagnosisdesc) LIKE '%fracture%' OR LOWER(diagnosisdesc) LIKE '%accident%'
                        OR LOWER(diagnosisdesc) LIKE '%injury%' OR LOWER(diagnosisdesc) LIKE '%trauma%'
                        THEN 'Trauma'
                    -- Chronic - Hypertension
                    WHEN diagnosiscode LIKE 'I10%' OR diagnosiscode LIKE 'I11%'
                        OR LOWER(diagnosisdesc) LIKE '%hypertension%'
                        THEN 'Chronic-Hypertension'
                    -- Chronic - Diabetes
                    WHEN diagnosiscode LIKE 'E10%' OR diagnosiscode LIKE 'E11%'
                        OR LOWER(diagnosisdesc) LIKE '%diabetes%'
                        THEN 'Chronic-Diabetes'
                    -- Chronic - Respiratory
                    WHEN diagnosiscode LIKE 'J45%' OR LOWER(diagnosisdesc) LIKE '%asthma%'
                        OR LOWER(diagnosisdesc) LIKE '%copd%'
                        THEN 'Chronic-Respiratory'
                    -- Chronic - Kidney
                    WHEN diagnosiscode LIKE 'N18%' OR LOWER(diagnosisdesc) LIKE '%chronic%kidney%'
                        OR LOWER(diagnosisdesc) LIKE '%renal%failure%'
                        THEN 'Chronic-Kidney'
                    -- Preventable - Malaria
                    WHEN diagnosiscode LIKE 'B5%' OR LOWER(diagnosisdesc) LIKE '%malaria%'
                        THEN 'Preventable-Malaria'
                    -- Preventable - URI
                    WHEN diagnosiscode LIKE 'J06%' OR LOWER(diagnosisdesc) LIKE '%upper%respiratory%'
                        OR LOWER(diagnosisdesc) LIKE '%uri%'
                        THEN 'Preventable-URI'
                    -- Preventable - UTI
                    WHEN diagnosiscode LIKE 'N39%' OR LOWER(diagnosisdesc) LIKE '%urinary%tract%'
                        OR LOWER(diagnosisdesc) LIKE '%uti%'
                        THEN 'Preventable-UTI'
                    -- Preventable - Diarrhea
                    WHEN diagnosiscode LIKE 'A09%' OR LOWER(diagnosisdesc) LIKE '%diarrhea%'
                        OR LOWER(diagnosisdesc) LIKE '%gastroenteritis%'
                        THEN 'Preventable-Diarrhea'
                    -- Catastrophic - Cancer
                    WHEN diagnosiscode LIKE 'C%' OR LOWER(diagnosisdesc) LIKE '%cancer%'
                        OR LOWER(diagnosisdesc) LIKE '%malignant%' OR LOWER(diagnosisdesc) LIKE '%carcinoma%'
                        THEN 'Catastrophic-Cancer'
                    -- Catastrophic - Stroke
                    WHEN diagnosiscode LIKE 'I63%' OR diagnosiscode LIKE 'I64%'
                        OR LOWER(diagnosisdesc) LIKE '%stroke%' OR LOWER(diagnosisdesc) LIKE '%cerebrovascular%'
                        THEN 'Catastrophic-Stroke'
                    ELSE 'Other'
                END as category
            FROM claims_with_diagnosis
        )
        SELECT 
            category,
            SUM(approvedamount) as total_cost,
            COUNT(*) as count
        FROM categorized
        GROUP BY category
        """
        
        cat_df = self.conn.execute(query, [groupid, start_date, end_date]).fetchdf()
        
        if cat_df.empty:
            return {
                'success': True,
                'categories': [],
                'maternity_pct': 0,
                'one_off_pct': 0,
                'chronic_pct': 0,
                'preventable_pct': 0,
                'catastrophic_pct': 0
            }
        
        total_cost = cat_df['total_cost'].sum()
        
        if total_cost > 0:
            cat_df['pct'] = (cat_df['total_cost'] / total_cost * 100).round(2)
        else:
            cat_df['pct'] = 0
        
        # Calculate category percentages
        maternity_pct = float(cat_df[cat_df['category'] == 'Maternity']['pct'].sum())
        
        one_off_pct = float(cat_df[cat_df['category'].isin(['Maternity', 'Surgery', 'Trauma'])]['pct'].sum())
        
        chronic_pct = float(cat_df[cat_df['category'].str.startswith('Chronic')]['pct'].sum())
        
        preventable_pct = float(cat_df[cat_df['category'].str.startswith('Preventable')]['pct'].sum())
        
        catastrophic_pct = float(cat_df[cat_df['category'].str.startswith('Catastrophic')]['pct'].sum())
        
        return {
            'success': True,
            'categories': cat_df.to_dict('records'),
            'maternity_pct': maternity_pct,
            'one_off_pct': one_off_pct,
            'chronic_pct': chronic_pct,
            'preventable_pct': preventable_pct,
            'catastrophic_pct': catastrophic_pct
        }
    
    def analyze_providers(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze top providers"""
        if not self.conn:
            self.connect()
        
        query = """
        SELECT 
            c.nhisproviderid,
            p.providername,
            SUM(c.approvedamount) as total_cost,
            COUNT(*) as claim_count,
            COUNT(DISTINCT c.enrollee_id) as unique_members
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON
            CAST(c.nhisproviderid AS VARCHAR) = CAST(TRY_CAST(p.providerid AS BIGINT) AS VARCHAR)
        WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
            AND c.datesubmitted >= ?
            AND c.datesubmitted <= ?
            AND c.approvedamount > 0
        GROUP BY c.nhisproviderid, p.providername
        ORDER BY total_cost DESC
        LIMIT 10
        """
        
        prov_df = self.conn.execute(query, [groupid, start_date, end_date]).fetchdf()
        
        if prov_df.empty:
            return {
                'success': True,
                'top_providers': [],
                'unknown_pct': 0,
                'unknown_amount': 0
            }
        
        # Calculate total
        total_cost = prov_df['total_cost'].sum()
        
        if total_cost > 0:
            prov_df['pct'] = (prov_df['total_cost'] / total_cost * 100).round(2)
        else:
            prov_df['pct'] = 0
        
        # Unknown provider analysis
        unknown_df = prov_df[prov_df['providername'].isna() | (prov_df['providername'] == '')]
        unknown_amount = float(unknown_df['total_cost'].sum())
        unknown_pct = (unknown_amount / total_cost * 100) if total_cost > 0 else 0
        
        return {
            'success': True,
            'top_providers': prov_df.to_dict('records'),
            'unknown_pct': unknown_pct,
            'unknown_amount': unknown_amount
        }

    def calculate_monthly_pmpm_trend(self, groupid: str, start_date, end_date, enrollment: Dict) -> Dict:
        """Calculate monthly PMPM trends and detect gaming patterns"""
        if not self.conn:
            self.connect()

        try:
            query = """
            WITH monthly_claims AS (
                SELECT
                    STRFTIME(datesubmitted, '%Y-%m') as month,
                    SUM(approvedamount) as total_claims,
                    COUNT(DISTINCT enrollee_id) as active_claimants
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE CAST(nhisgroupid AS VARCHAR) = ?
                    AND datesubmitted >= ?
                    AND datesubmitted <= ?
                    AND approvedamount > 0
                GROUP BY STRFTIME(datesubmitted, '%Y-%m')
            ),
            monthly_members AS (
                SELECT
                    STRFTIME(?, '%Y-%m') as month,
                    ? as member_count
            )
            SELECT
                c.month,
                c.total_claims,
                COALESCE(m.member_count, ?) as members,
                CASE
                    WHEN COALESCE(m.member_count, ?) > 0
                    THEN c.total_claims / COALESCE(m.member_count, ?)
                    ELSE 0
                END as pmpm
            FROM monthly_claims c
            LEFT JOIN monthly_members m ON c.month = m.month
            ORDER BY c.month
            """

            total_members = enrollment.get('total', 1)

            df = self.conn.execute(query, [
                groupid, start_date, end_date,
                start_date, total_members,
                total_members, total_members, total_members
            ]).fetchdf()

            if df.empty:
                return {
                    'success': False,
                    'error': 'No monthly data available'
                }

            # Convert to records
            monthly_data = df.to_dict('records')

            # Calculate metrics
            avg_pmpm = float(df['pmpm'].mean())
            current_pmpm = float(df.iloc[-1]['pmpm']) if len(df) > 0 else 0

            # Detect spikes (>20% month-over-month increase)
            spike_months = []
            for i in range(1, len(df)):
                prev_pmpm = float(df.iloc[i-1]['pmpm'])
                curr_pmpm = float(df.iloc[i]['pmpm'])
                if prev_pmpm > 0:
                    pct_change = ((curr_pmpm - prev_pmpm) / prev_pmpm) * 100
                    if pct_change > 20:
                        spike_months.append(df.iloc[i]['month'])

            # Detect gaming pattern (final 2 months spike >25%)
            gaming_risk = 'LOW'
            if len(df) >= 3:
                baseline_pmpm = float(df.iloc[:-2]['pmpm'].mean())
                final_2_pmpm = float(df.iloc[-2:]['pmpm'].mean())
                if baseline_pmpm > 0:
                    final_increase = ((final_2_pmpm - baseline_pmpm) / baseline_pmpm) * 100
                    if final_increase > 25:
                        gaming_risk = 'HIGH'
                    elif final_increase > 15:
                        gaming_risk = 'MEDIUM'

            # Determine trend
            if len(df) >= 2:
                first_half_avg = float(df.iloc[:len(df)//2]['pmpm'].mean())
                second_half_avg = float(df.iloc[len(df)//2:]['pmpm'].mean())
                if second_half_avg > first_half_avg * 1.1:
                    trend = 'INCREASING'
                elif second_half_avg < first_half_avg * 0.9:
                    trend = 'DECREASING'
                else:
                    trend = 'STABLE'
            else:
                trend = 'INSUFFICIENT_DATA'

            return {
                'success': True,
                'monthly_data': monthly_data,
                'avg_pmpm': avg_pmpm,
                'current_pmpm': current_pmpm,
                'trend': trend,
                'spike_months': spike_months,
                'gaming_risk': gaming_risk
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def analyze_chronic_disease_burden(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze chronic disease prevalence and impact"""
        if not self.conn:
            self.connect()

        try:
            # Define chronic condition ICD-10 codes
            chronic_conditions = {
                'diabetes': ['E10', 'E11'],
                'hypertension': ['I10', 'I11'],
                'heart_disease': ['I20', 'I21', 'I22', 'I23', 'I24', 'I25', 'I50'],
                'copd_asthma': ['J44', 'J45'],
                'kidney_disease': ['N18'],
                'mental_health': ['F']
            }

            prevalence = {}

            for condition, icd_codes in chronic_conditions.items():
                # Build LIKE conditions for each ICD code
                like_conditions = ' OR '.join([f"diagnosiscode LIKE '{code}%'" for code in icd_codes])

                query = f"""
                SELECT
                    COUNT(DISTINCT c.enrollee_id) as patient_count,
                    SUM(c.approvedamount) as total_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
                    AND c.datesubmitted >= ?
                    AND c.datesubmitted <= ?
                    AND ({like_conditions})
                """

                result = self.conn.execute(query, [groupid, start_date, end_date]).fetchdf()

                if not result.empty and result.iloc[0]['patient_count'] > 0:
                    prevalence[condition] = {
                        'count': int(result.iloc[0]['patient_count']),
                        'cost': float(result.iloc[0]['total_cost'])
                    }
                else:
                    prevalence[condition] = {'count': 0, 'cost': 0.0}

            # Get total population for percentages
            total_pop_query = """
            SELECT COUNT(DISTINCT enrollee_id) as total
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE CAST(nhisgroupid AS VARCHAR) = ?
                AND datesubmitted >= ?
                AND datesubmitted <= ?
            """

            total_pop = self.conn.execute(total_pop_query, [groupid, start_date, end_date]).fetchdf()
            total_population = int(total_pop.iloc[0]['total']) if not total_pop.empty else 1

            # Calculate percentages
            for condition in prevalence:
                prevalence[condition]['pct'] = round((prevalence[condition]['count'] / total_population) * 100, 1) if total_population > 0 else 0

            # Comorbidity analysis
            comorbidity_query = """
            WITH member_conditions AS (
                SELECT
                    enrollee_id,
                    COUNT(DISTINCT
                        CASE
                            WHEN diagnosiscode LIKE 'E10%' OR diagnosiscode LIKE 'E11%' THEN 'diabetes'
                            WHEN diagnosiscode LIKE 'I10%' OR diagnosiscode LIKE 'I11%' THEN 'hypertension'
                            WHEN diagnosiscode LIKE 'I20%' OR diagnosiscode LIKE 'I2_' OR diagnosiscode LIKE 'I50%' THEN 'heart'
                            WHEN diagnosiscode LIKE 'J44%' OR diagnosiscode LIKE 'J45%' THEN 'respiratory'
                            WHEN diagnosiscode LIKE 'N18%' THEN 'kidney'
                            WHEN diagnosiscode LIKE 'F%' THEN 'mental'
                        END
                    ) as condition_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE CAST(nhisgroupid AS VARCHAR) = ?
                    AND datesubmitted >= ?
                    AND datesubmitted <= ?
                    AND diagnosiscode IS NOT NULL
                GROUP BY enrollee_id
            )
            SELECT
                AVG(condition_count) as avg_conditions,
                SUM(CASE WHEN condition_count >= 2 THEN 1 ELSE 0 END) as multi_condition_count
            FROM member_conditions
            WHERE condition_count > 0
            """

            comorbidity_df = self.conn.execute(comorbidity_query, [groupid, start_date, end_date]).fetchdf()

            if not comorbidity_df.empty and comorbidity_df.iloc[0]['avg_conditions'] is not None:
                avg_conditions = float(comorbidity_df.iloc[0]['avg_conditions'])
                multi_count = int(comorbidity_df.iloc[0]['multi_condition_count'])
                pct_multi = round((multi_count / total_population) * 100, 1) if total_population > 0 else 0
            else:
                avg_conditions = 0
                pct_multi = 0

            comorbidity = {
                'avg_conditions_per_member': round(avg_conditions, 1),
                'pct_with_multiple': pct_multi,
                'top_combinations': []  # Could be expanded with more complex query
            }

            # Cost impact analysis
            total_chronic_cost = sum([p['cost'] for p in prevalence.values()])
            total_cost_query = """
            SELECT SUM(approvedamount) as total
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE CAST(nhisgroupid AS VARCHAR) = ?
                AND datesubmitted >= ?
                AND datesubmitted <= ?
            """

            total_cost_df = self.conn.execute(total_cost_query, [groupid, start_date, end_date]).fetchdf()
            total_cost = float(total_cost_df.iloc[0]['total']) if not total_cost_df.empty else 0

            chronic_pct = round((total_chronic_cost / total_cost) * 100, 1) if total_cost > 0 else 0
            acute_cost = total_cost - total_chronic_cost

            # Get chronic member count
            total_chronic_members = len(set([k for k in prevalence.keys()]))
            avg_cost_chronic = total_chronic_cost / total_chronic_members if total_chronic_members > 0 else 0
            avg_cost_non_chronic = acute_cost / (total_population - total_chronic_members) if (total_population - total_chronic_members) > 0 else 0

            cost_impact = {
                'chronic_cost': total_chronic_cost,
                'chronic_pct': chronic_pct,
                'acute_cost': acute_cost,
                'avg_cost_chronic': avg_cost_chronic,
                'avg_cost_non_chronic': avg_cost_non_chronic
            }

            # Risk level
            total_chronic_pct = sum([p['count'] for p in prevalence.values()]) / total_population * 100 if total_population > 0 else 0
            risk_level = 'HIGH' if total_chronic_pct > 30 else 'MEDIUM' if total_chronic_pct > 20 else 'LOW'

            return {
                'success': True,
                'prevalence': prevalence,
                'comorbidity': comorbidity,
                'cost_impact': cost_impact,
                'risk_level': risk_level
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def decompose_claims_trend(self, groupid: str, start_date, end_date, enrollment: Dict) -> Dict:
        """Decompose claims trend into unit cost and utilization components"""
        if not self.conn:
            self.connect()

        try:
            # Calculate prior period (12 months before start_date)
            from datetime import datetime, timedelta
            start_dt = datetime.strptime(str(start_date), '%Y-%m-%d')
            end_dt = datetime.strptime(str(end_date), '%Y-%m-%d')

            days_in_period = (end_dt - start_dt).days
            prior_start = start_dt - timedelta(days=days_in_period)
            prior_end = start_dt - timedelta(days=1)

            # Current period metrics
            current_query = """
            SELECT
                AVG(approvedamount) as avg_claim_cost,
                COUNT(*) as total_claims,
                COUNT(DISTINCT enrollee_id) as unique_members
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE CAST(nhisgroupid AS VARCHAR) = ?
                AND datesubmitted >= ?
                AND datesubmitted <= ?
                AND approvedamount > 0
            """

            current_df = self.conn.execute(current_query, [groupid, start_date, end_date]).fetchdf()

            # Prior period metrics
            prior_df = self.conn.execute(current_query, [groupid, prior_start, prior_end]).fetchdf()

            if current_df.empty:
                return {
                    'success': False,
                    'error': 'No current period data available'
                }

            current_avg = float(current_df.iloc[0]['avg_claim_cost']) if current_df.iloc[0]['avg_claim_cost'] else 0
            current_claims = int(current_df.iloc[0]['total_claims'])
            current_members = int(current_df.iloc[0]['unique_members']) if current_df.iloc[0]['unique_members'] else enrollment.get('total', 1)

            if not prior_df.empty and prior_df.iloc[0]['avg_claim_cost']:
                prior_avg = float(prior_df.iloc[0]['avg_claim_cost'])
                prior_claims = int(prior_df.iloc[0]['total_claims'])
                prior_members = int(prior_df.iloc[0]['unique_members']) if prior_df.iloc[0]['unique_members'] else current_members
            else:
                # No prior data, return current only
                return {
                    'success': True,
                    'unit_cost_trend': {
                        'current_avg': current_avg,
                        'prior_avg': 0,
                        'pct_change': 0,
                        'top_increases': []
                    },
                    'utilization_trend': {
                        'current_per_1000': (current_claims / current_members * 1000) if current_members > 0 else 0,
                        'prior_per_1000': 0,
                        'pct_change': 0
                    },
                    'total_trend': 0,
                    'interpretation': 'INSUFFICIENT_DATA'
                }

            # Unit cost trend
            unit_cost_pct = ((current_avg - prior_avg) / prior_avg * 100) if prior_avg > 0 else 0

            # Utilization trend
            current_per_1000 = (current_claims / current_members * 1000) if current_members > 0 else 0
            prior_per_1000 = (prior_claims / prior_members * 1000) if prior_members > 0 else 0
            util_pct = ((current_per_1000 - prior_per_1000) / prior_per_1000 * 100) if prior_per_1000 > 0 else 0

            # Total trend (compound effect)
            total_trend = ((1 + unit_cost_pct/100) * (1 + util_pct/100) - 1) * 100

            # Interpretation
            if abs(unit_cost_pct) < 3 and abs(util_pct) < 3:
                interpretation = 'STABLE'
            elif unit_cost_pct > util_pct:
                interpretation = 'COST_DRIVEN'
            elif util_pct > unit_cost_pct:
                interpretation = 'VOLUME_DRIVEN'
            else:
                interpretation = 'BOTH_INCREASING'

            return {
                'success': True,
                'unit_cost_trend': {
                    'current_avg': current_avg,
                    'prior_avg': prior_avg,
                    'pct_change': round(unit_cost_pct, 1),
                    'top_increases': []  # Could add procedure-level analysis
                },
                'utilization_trend': {
                    'current_per_1000': round(current_per_1000, 1),
                    'prior_per_1000': round(prior_per_1000, 1),
                    'pct_change': round(util_pct, 1)
                },
                'total_trend': round(total_trend, 1),
                'interpretation': interpretation
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

    def analyze_provider_bands(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze provider bands using the existing bands column in PROVIDERS table"""
        if not self.conn:
            self.connect()

        try:
            # Get all providers used by this group with their bands
            query = """
            SELECT
                c.nhisproviderid,
                p.providername,
                p.bands,
                SUM(c.approvedamount) as total_cost,
                COUNT(*) as claim_count,
                COUNT(DISTINCT c.enrollee_id) as unique_members
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON
                CAST(c.nhisproviderid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
            WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
                AND c.datesubmitted >= ?
                AND c.datesubmitted <= ?
                AND c.approvedamount > 0
            GROUP BY c.nhisproviderid, p.providername, p.bands
            ORDER BY total_cost DESC
            """

            df = self.conn.execute(query, [groupid, start_date, end_date]).fetchdf()

            if df.empty:
                return {
                    'success': False,
                    'error': 'No provider data available'
                }

            # Calculate total cost
            total_cost = df['total_cost'].sum()

            # Add percentage column
            df['pct_of_total'] = (df['total_cost'] / total_cost * 100).round(2)

            # Clean up bands column (remove whitespace, handle nulls)
            df['bands'] = df['bands'].fillna('UNKNOWN').str.strip().str.upper()

            # Create providers with bands list (top 10)
            providers_with_bands = []
            for _, row in df.head(10).iterrows():
                providers_with_bands.append({
                    'providerid': str(row['nhisproviderid']),
                    'providername': row['providername'] if pd.notna(row['providername']) else 'Unknown Provider',
                    'total_cost': float(row['total_cost']),
                    'pct_of_total': float(row['pct_of_total']),
                    'band': row['bands'],
                    'claim_count': int(row['claim_count']),
                    'unique_members': int(row['unique_members'])
                })

            # Calculate band distribution (all providers, not just top 10)
            band_distribution = {}

            # Group by band
            band_summary = df.groupby('bands').agg({
                'nhisproviderid': 'count',
                'total_cost': 'sum'
            }).reset_index()

            band_summary.columns = ['band', 'count', 'total_cost']
            band_summary['pct'] = (band_summary['total_cost'] / total_cost * 100).round(2)

            # Convert to dictionary format
            for _, row in band_summary.iterrows():
                band_key = row['band'].lower().replace(' ', '_')
                band_distribution[band_key] = {
                    'count': int(row['count']),
                    'total_cost': float(row['total_cost']),
                    'pct': float(row['pct'])
                }

            # Find dominant band (highest % of cost)
            dominant_band = band_summary.loc[band_summary['pct'].idxmax(), 'band'] if len(band_summary) > 0 else 'UNKNOWN'

            # Calculate total unique providers
            total_providers = len(df)

            return {
                'success': True,
                'providers_with_bands': providers_with_bands,
                'band_distribution': band_distribution,
                'total_providers': total_providers,
                'dominant_band': dominant_band
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Error analyzing provider bands: {str(e)}'
            }

    # ═══════════════════════════════════════════════════════════════════════════
    # BENEFIT LIMIT ANALYSIS FUNCTIONS
    # ═══════════════════════════════════════════════════════════════════════════

    def analyze_company_benefit_limits(self, company_name: str, groupid: str, start_date, end_date) -> Dict:
        """
        Analyze members who exceeded benefit limits (monetary or count-based)

        CORRECTED METHODOLOGY:
        - Uses `encounterdatefrom` for claims filtering (NOT datesubmitted)
        - Members with claims/PA = ACTIVE (regardless of MEMBERS.iscurrent flag)
        - Members without claims/PA = NO ACTIVITY
        - Includes both Claims and Unclaimed PA in utilization totals

        Returns:
            Dict with violations grouped by:
            - Active members (have claims/PA)
            - No activity members (no claims/PA but in MEMBERS table)
        """
        if not self.conn:
            self.connect()

        try:
            query = """
            WITH all_members AS (
                -- Get ALL members in the group (from MEMBERS table)
                SELECT DISTINCT
                    m.enrollee_id,
                    m.memberid,
                    mp.planid,
                    p.planname
                FROM "AI DRIVEN DATA"."MEMBERS" m
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON
                    m.memberid = mp.memberid
                    AND mp.iscurrent = 1
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON
                    CAST(mp.planid AS VARCHAR) = CAST(p.planid AS VARCHAR)
                WHERE CAST(m.groupid AS VARCHAR) = ?
            ),

            claims_usage AS (
                -- Get ALL claims using encounterdatefrom
                SELECT
                    c.enrollee_id,
                    c.code as procedurecode,
                    am.memberid,
                    am.planid,
                    am.planname,
                    bcp.benefitcodeid,
                    bc.benefitcodedesc as benefitcode,
                    SUM(c.approvedamount) as total_spent,
                    COUNT(*) as visit_count,
                    'CLAIMS' as source
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN all_members am ON
                    c.enrollee_id = am.enrollee_id
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bcp ON
                    LOWER(TRIM(c.code)) = LOWER(TRIM(bcp.procedurecode))
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON
                    bcp.benefitcodeid = bc.benefitcodeid
                WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
                    AND c.encounterdatefrom >= ?
                    AND c.encounterdatefrom <= ?
                    AND c.approvedamount > 0
                    AND bcp.benefitcodeid IS NOT NULL
                GROUP BY
                    c.enrollee_id,
                    c.code,
                    am.memberid,
                    am.planid,
                    am.planname,
                    bcp.benefitcodeid,
                    bc.benefitcodedesc
            ),

            unclaimed_pa_usage AS (
                -- Get unclaimed PA for members
                SELECT
                    pa.IID as enrollee_id,
                    pa.code as procedurecode,
                    am.memberid,
                    am.planid,
                    am.planname,
                    bcp.benefitcodeid,
                    bc.benefitcodedesc as benefitcode,
                    SUM(pa.granted) as total_spent,
                    COUNT(*) as visit_count,
                    'UNCLAIMED_PA' as source
                FROM "AI DRIVEN DATA"."PA DATA" pa
                INNER JOIN all_members am ON
                    pa.IID = am.enrollee_id
                -- Exclude PA that have been claimed (anti-join)
                LEFT JOIN (
                    SELECT DISTINCT CAST(CAST(panumber AS BIGINT) AS VARCHAR) as panumber
                    FROM "AI DRIVEN DATA"."CLAIMS DATA"
                    WHERE CAST(nhisgroupid AS VARCHAR) = ?
                        AND encounterdatefrom >= ?
                        AND encounterdatefrom <= ?
                        AND panumber IS NOT NULL
                        AND panumber > 0
                ) claimed ON pa.panumber = claimed.panumber
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bcp ON
                    LOWER(TRIM(pa.code)) = LOWER(TRIM(bcp.procedurecode))
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON
                    bcp.benefitcodeid = bc.benefitcodeid
                WHERE pa.groupname = ?
                    AND pa.requestdate >= ?
                    AND pa.requestdate <= ?
                    AND pa.granted > 0
                    AND claimed.panumber IS NULL  -- Only unclaimed PA
                    AND bcp.benefitcodeid IS NOT NULL
                GROUP BY
                    pa.IID,
                    pa.code,
                    am.memberid,
                    am.planid,
                    am.planname,
                    bcp.benefitcodeid,
                    bc.benefitcodedesc
            ),

            all_utilization AS (
                -- Combine claims and unclaimed PA
                SELECT * FROM claims_usage
                UNION ALL
                SELECT * FROM unclaimed_pa_usage
            ),

            members_with_activity AS (
                -- Members who have claims or PA = ACTIVE
                SELECT DISTINCT enrollee_id
                FROM all_utilization
            ),

            member_benefit_usage AS (
                -- Aggregate by member, plan, and benefit
                SELECT
                    u.enrollee_id,
                    u.memberid,
                    u.planid,
                    u.planname,
                    u.benefitcode,
                    u.benefitcodeid,
                    SUM(u.total_spent) as total_spent,
                    SUM(u.visit_count) as visit_count,
                    -- Get limits from PLANBENEFITCODE_LIMIT
                    MAX(pl.maxlimit) as maxlimit,
                    MAX(pl.countperannum) as countperannum,
                    -- Determine if member is ACTIVE or NO ACTIVITY
                    CASE
                        WHEN EXISTS (
                            SELECT 1 FROM members_with_activity mwa
                            WHERE mwa.enrollee_id = u.enrollee_id
                        )
                        THEN 'ACTIVE'
                        ELSE 'NO ACTIVITY'
                    END as member_status
                FROM all_utilization u
                LEFT JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" pl ON
                    CAST(u.planid AS BIGINT) = pl.planid
                    AND u.benefitcodeid = pl.benefitcodeid
                GROUP BY
                    u.enrollee_id,
                    u.memberid,
                    u.planid,
                    u.planname,
                    u.benefitcode,
                    u.benefitcodeid
            ),

            violations AS (
                SELECT
                    enrollee_id,
                    memberid,
                    planid,
                    planname,
                    benefitcode,
                    total_spent,
                    visit_count,
                    maxlimit,
                    countperannum,
                    -- Calculate monetary overage
                    CASE
                        WHEN maxlimit IS NOT NULL AND maxlimit > 0 AND total_spent > maxlimit
                        THEN total_spent - maxlimit
                        ELSE 0
                    END as monetary_overage,
                    -- Calculate utilization percentage
                    CASE
                        WHEN maxlimit IS NOT NULL AND maxlimit > 0
                        THEN ROUND((total_spent / maxlimit * 100), 2)
                        ELSE NULL
                    END as utilization_pct,
                    -- Calculate count overage
                    CASE
                        WHEN countperannum IS NOT NULL AND countperannum > 0 AND visit_count > countperannum
                        THEN visit_count - countperannum
                        ELSE 0
                    END as count_overage,
                    -- Flag if no limit exists
                    CASE
                        WHEN (maxlimit IS NULL OR maxlimit = 0) AND (countperannum IS NULL OR countperannum = 0)
                        THEN 1
                        ELSE 0
                    END as no_limit_flag,
                    member_status
                FROM member_benefit_usage
            )

            SELECT
                enrollee_id,
                memberid,
                planid,
                planname,
                benefitcode,
                total_spent,
                visit_count,
                maxlimit,
                countperannum,
                monetary_overage,
                utilization_pct,
                count_overage,
                no_limit_flag,
                member_status,
                -- Calculate remaining balance
                CASE
                    WHEN maxlimit IS NOT NULL AND maxlimit > 0
                    THEN maxlimit - total_spent
                    ELSE NULL
                END as remaining_balance
            FROM violations
            WHERE monetary_overage > 0 OR count_overage > 0
            ORDER BY member_status, monetary_overage DESC, count_overage DESC
            """

            df = self.conn.execute(query, [
                groupid,  # For all_members
                groupid, start_date, end_date,  # For claims_usage
                groupid, start_date, end_date,  # For unclaimed_pa_usage (anti-join)
                company_name, start_date, end_date   # For unclaimed_pa_usage
            ]).fetchdf()

            if df.empty:
                return {
                    'success': True,
                    'violations': [],
                    'active_violations': [],
                    'terminated_violations': [],
                    'total_monetary_loss': 0,
                    'active_monetary_loss': 0,
                    'terminated_monetary_loss': 0,
                    'total_members_over_limit': 0,
                    'active_members_over_limit': 0,
                    'terminated_members_over_limit': 0,
                    'total_benefits_violated': 0,
                    'members_no_limit': 0,
                    'no_limit_spending': 0,
                    'summary_by_benefit': []
                }

            # Separate active and no activity
            active_df = df[df['member_status'] == 'ACTIVE']
            terminated_df = df[df['member_status'] == 'NO ACTIVITY']

            # Calculate summary metrics for active members
            active_monetary_loss = float(active_df['monetary_overage'].sum()) if not active_df.empty else 0.0
            active_members_over_limit = len(active_df['enrollee_id'].unique()) if not active_df.empty else 0

            # Calculate summary metrics for no activity members
            terminated_monetary_loss = float(terminated_df['monetary_overage'].sum()) if not terminated_df.empty else 0.0
            terminated_members_over_limit = len(terminated_df['enrollee_id'].unique()) if not terminated_df.empty else 0

            # Total metrics
            total_monetary_loss = active_monetary_loss + terminated_monetary_loss
            total_members_over_limit = active_members_over_limit + terminated_members_over_limit
            total_benefits_violated = len(df)

            # Summary by benefit (top violations)
            summary_by_benefit = []
            if not df.empty:
                benefit_summary = df.groupby('benefitcode').agg({
                    'monetary_overage': 'sum',
                    'enrollee_id': 'nunique',
                    'total_spent': 'sum',
                    'maxlimit': 'first'
                }).reset_index()

                benefit_summary.columns = ['benefit_name', 'total_overage', 'members_affected', 'total_spent', 'limit']
                benefit_summary = benefit_summary.sort_values('total_overage', ascending=False)
                summary_by_benefit = benefit_summary.to_dict('records')

            return {
                'success': True,
                'violations': df.to_dict('records'),
                'active_violations': active_df.to_dict('records') if not active_df.empty else [],
                'terminated_violations': terminated_df.to_dict('records') if not terminated_df.empty else [],
                'total_monetary_loss': total_monetary_loss,
                'active_monetary_loss': active_monetary_loss,
                'terminated_monetary_loss': terminated_monetary_loss,
                'total_members_over_limit': total_members_over_limit,
                'active_members_over_limit': active_members_over_limit,
                'terminated_members_over_limit': terminated_members_over_limit,
                'total_benefits_violated': total_benefits_violated,
                'members_no_limit': 0,  # Not tracking this in corrected version
                'no_limit_spending': 0,  # Not tracking this in corrected version
                'summary_by_benefit': summary_by_benefit
            }

        except Exception as e:
            return {
                'success': False,
                'error': f'Error analyzing benefit limits: {str(e)}',
                'violations': [],
                'active_violations': [],
                'terminated_violations': [],
                'total_monetary_loss': 0,
                'active_monetary_loss': 0,
                'terminated_monetary_loss': 0,
                'total_members_over_limit': 0,
                'active_members_over_limit': 0,
                'terminated_members_over_limit': 0,
                'total_benefits_violated': 0,
                'members_no_limit': 0,
                'no_limit_spending': 0,
                'summary_by_benefit': []
            }


    def get_benefit_insights_for_claude(self, benefit_analysis: Dict) -> str:
        """
        Format benefit limit analysis for Claude API inclusion
        Returns a concise summary for AI-powered recommendations
        """
        if not benefit_analysis.get('success'):
            return "No benefit limit data available."

        total_loss = benefit_analysis.get('total_monetary_loss', 0)
        members_over = benefit_analysis.get('total_members_over_limit', 0)
        benefits_violated = benefit_analysis.get('total_benefits_violated', 0)
        members_no_limit = benefit_analysis.get('members_no_limit', 0)
        no_limit_spending = benefit_analysis.get('no_limit_spending', 0)

        insight = f"""
BENEFIT LIMIT VIOLATIONS:
- Total monetary loss from over-limit claims: ₦{total_loss:,.2f}
- Members who exceeded limits: {members_over}
- Benefit types violated: {benefits_violated}
- Members with NO limits defined: {members_no_limit}
- Spending on unlimited benefits: ₦{no_limit_spending:,.2f}
"""

        # Add top violators if available
        violations = benefit_analysis.get('violations', [])
        if violations:
            top_violations = sorted(violations, key=lambda x: x.get('monetary_overage', 0), reverse=True)[:5]
            insight += "\nTOP BENEFIT VIOLATIONS:\n"
            for v in top_violations:
                enrollee = v.get('enrollee_id', 'Unknown')
                benefit = v.get('benefitcode', v.get('procedurecode', 'Unknown'))
                overage = v.get('monetary_overage', 0)
                limit = v.get('maxlimit', 'N/A')
                spent = v.get('total_spent', 0)
                insight += f"  - {enrollee}: {benefit} | Limit: ₦{limit:,.0f} | Spent: ₦{spent:,.2f} | Overage: ₦{overage:,.2f}\n"

        return insight

    def analyze_plan_distribution(self, company_name: str, groupid: str, start_date, end_date) -> Dict:
        """Analyze plan enrollment vs utilization with proper claims + PA attribution"""
        if not self.conn:
            self.connect()

        try:
            # ========================================
            # STEP 1: Get Plan Enrollment from GROUP_PLANS
            # ========================================
            enrollment_query = """
            SELECT
                gp.planid,
                p.planname,
                p.plancode,
                gp.individualprice,
                gp.familyprice,
                COALESCE(gp.maxnumdependant, 3) as maxnumdependant,
                COALESCE(gp.countofindividual, 0) as countofindividual,
                COALESCE(gp.countoffamily, 0) as countoffamily
            FROM "AI DRIVEN DATA"."GROUP_PLANS" gp
            INNER JOIN "AI DRIVEN DATA"."GROUPS" g ON
                CAST(gp.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON
                CAST(gp.planid AS VARCHAR) = CAST(p.planid AS VARCHAR)
            WHERE g.groupname = ?
                AND gp.iscurrent = true
            """

            enrollment_df = self.conn.execute(enrollment_query, [company_name]).fetchdf()

            if enrollment_df.empty:
                return {
                    'success': False,
                    'error': 'No plan enrollment data found for this company'
                }

            # Calculate total enrolled per plan
            enrollment_df['total_enrolled'] = (
                enrollment_df['countofindividual'] +
                (enrollment_df['countoffamily'] * (enrollment_df['maxnumdependant'] + 1))
            ).astype(int)

            total_enrolled = enrollment_df['total_enrolled'].sum()

            if total_enrolled == 0:
                return {
                    'success': False,
                    'error': 'No enrolled members found in plans'
                }

            enrollment_df['enrollment_pct'] = (
                (enrollment_df['total_enrolled'] / total_enrolled * 100).round(2)
            )

            # Convert planid to string for joining and clean up any .0 suffix
            enrollment_df['planid'] = enrollment_df['planid'].astype(str).str.replace(r'\.0$', '', regex=True)

            # ========================================
            # STEP 2: Get CLAIMS COST by Plan (INCLUDING TERMINATED MEMBERS)
            # Route: CLAIMS → MEMBER (legacycode) → MEMBER_PLANS (iscurrent=1) → planid
            # ========================================

            print("\n" + "="*60)
            print("PLAN CLAIMS ATTRIBUTION - CORRECTED VERSION")
            print("Using MEMBER + MEMBER_PLANS for complete history")
            print("="*60)

            # Diagnostic: Check total claims for this group
            diagnostic_query = """
            SELECT
                COUNT(*) as total_claims,
                SUM(approvedamount) as total_amount,
                COUNT(DISTINCT enrollee_id) as unique_enrollees
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE CAST(nhisgroupid AS VARCHAR) = ?
                AND datesubmitted >= ?
                AND datesubmitted <= ?
                AND approvedamount > 0
            """
            diag_df = self.conn.execute(diagnostic_query, [groupid, start_date, end_date]).fetchdf()
            print(f"\n=== Total Claims for Group ===")
            print(f"Total claims: {diag_df['total_claims'].iloc[0]:,}")
            print(f"Total amount: ₦{diag_df['total_amount'].iloc[0]:,.2f}")
            print(f"Unique enrollees: {diag_df['unique_enrollees'].iloc[0]:,}")

            # CORRECTED APPROACH: Use MEMBER + MEMBER_PLANS (has both planid and iscurrent!)
            claims_by_plan_query = """
            WITH claims_with_plans AS (
                SELECT
                    c.enrollee_id,
                    c.approvedamount,
                    m.memberid,
                    mp.planid,
                    p.planname
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                -- Join to MEMBER (all enrollees including terminated)
                INNER JOIN "AI DRIVEN DATA".MEMBER m ON
                    c.enrollee_id = m.legacycode
                -- Join to MEMBER_PLANS with iscurrent=1 (most recent plan assignment)
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON
                    m.memberid = mp.memberid
                    AND mp.iscurrent = 1
                -- Join to PLANS for planname
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON
                    CAST(mp.planid AS VARCHAR) = CAST(p.planid AS VARCHAR)
                WHERE CAST(m.groupid AS VARCHAR) = ?
                    AND c.datesubmitted >= ?
                    AND c.datesubmitted <= ?
                    AND c.approvedamount > 0
                    AND mp.planid IS NOT NULL
            )
            SELECT
                CAST(planid AS VARCHAR) as planid,
                planname,
                SUM(approvedamount) as claims_cost,
                COUNT(*) as claim_count,
                COUNT(DISTINCT enrollee_id) as unique_claimants
            FROM claims_with_plans
            GROUP BY planid, planname
            """

            claims_df = self.conn.execute(
                claims_by_plan_query,
                [groupid, start_date, end_date]
            ).fetchdf()

            if not claims_df.empty:
                print(f"\n✅ Claims Attribution Success!")
                print(f"   Plans with claims: {len(claims_df)}")
                print(f"   Total claims cost: ₦{claims_df['claims_cost'].sum():,.2f}")
                print(f"   Total unique claimants: {claims_df['unique_claimants'].sum():,}")

                # Show breakdown by plan
                for _, row in claims_df.iterrows():
                    print(f"   - {row['planname']}: ₦{row['claims_cost']:,.2f} ({row['claim_count']:,} claims)")
            else:
                print("\n⚠️  WARNING: No claims could be attributed to plans")
                print("   This may indicate:")
                print("   - No MEMBER_PLANS records with iscurrent=1")
                print("   - Mismatch between enrollee_id and legacycode")
                print("   - No planid assignments in MEMBER_PLANS")

                # Fallback to proportional distribution
                print("\n   Falling back to PROPORTIONAL DISTRIBUTION...")
                claims_proportional_query = """
                WITH all_claims AS (
                    SELECT
                        SUM(c.approvedamount) as total_claims_amount,
                        COUNT(*) as total_claim_count,
                        COUNT(DISTINCT c.enrollee_id) as total_unique_claimants
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    INNER JOIN "AI DRIVEN DATA".MEMBER m ON
                        c.enrollee_id = m.legacycode
                    WHERE CAST(m.groupid AS VARCHAR) = ?
                        AND c.datesubmitted >= ?
                        AND c.datesubmitted <= ?
                        AND c.approvedamount > 0
                ),
                group_plans_list AS (
                    SELECT
                        gp.planid,
                        p.planname,
                        gp.countofindividual + (gp.countoffamily * (COALESCE(gp.maxnumdependant, 3) + 1)) as plan_enrolled
                    FROM "AI DRIVEN DATA"."GROUP_PLANS" gp
                    INNER JOIN "AI DRIVEN DATA"."GROUPS" g ON
                        CAST(gp.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                    LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON
                        CAST(gp.planid AS VARCHAR) = CAST(p.planid AS VARCHAR)
                    WHERE g.groupname = ?
                        AND gp.iscurrent = true
                ),
                total_enrolled AS (
                    SELECT SUM(plan_enrolled) as total
                    FROM group_plans_list
                )
                SELECT
                    CAST(gpl.planid AS VARCHAR) as planid,
                    gpl.planname,
                    ROUND((gpl.plan_enrolled::DECIMAL / te.total) * ac.total_claims_amount, 2) as claims_cost,
                    ROUND((gpl.plan_enrolled::DECIMAL / te.total) * ac.total_claim_count) as claim_count,
                    ROUND((gpl.plan_enrolled::DECIMAL / te.total) * ac.total_unique_claimants) as unique_claimants
                FROM group_plans_list gpl
                CROSS JOIN all_claims ac
                CROSS JOIN total_enrolled te
                WHERE te.total > 0
                """

                claims_df = self.conn.execute(
                    claims_proportional_query,
                    [groupid, start_date, end_date, company_name]
                ).fetchdf()

                if not claims_df.empty:
                    print(f"   ✅ Proportional distribution complete")
                    print(f"   ⚠️  NOTE: Claims distributed based on enrollment (not actual plan usage)")

            # No longer tracking "terminated claims" as separate entity
            # All members (active + terminated) now properly attributed via MEMBER_COVERAGE
            terminated_claims_cost = 0
            terminated_claim_count = 0
            terminated_enrollees = 0

            # ========================================
            # STEP 3: Get UNCLAIMED PA by Plan
            # Enhanced with two attribution methods:
            # 1. Direct: PA.plancode → PLANS.plancode
            # 2. Fallback: PA.IID → MEMBER.legacycode → MEMBER_PLANS → planid
            # ========================================

            pa_by_plan_query = """
            WITH pa_issued AS (
                SELECT
                    pa.panumber,
                    pa.IID as enrollee_id,
                    pa.plancode,
                    pa.granted,
                    -- Try direct plancode match first
                    p1.planname as direct_planname,
                    p1.planid as direct_planid,
                    -- Try enrollee lookup as fallback
                    m.memberid,
                    mp.planid as plan_planid,
                    p2.planname as plan_planname
                FROM "AI DRIVEN DATA"."PA DATA" pa
                -- Direct plancode match
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p1 ON
                    CAST(pa.plancode AS VARCHAR) = CAST(p1.plancode AS VARCHAR)
                -- Enrollee-based match (for PAs without plancode)
                LEFT JOIN "AI DRIVEN DATA".MEMBER m ON
                    pa.IID = m.legacycode
                LEFT JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON
                    m.memberid = mp.memberid
                    AND mp.iscurrent = 1
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p2 ON
                    CAST(mp.planid AS VARCHAR) = CAST(p2.planid AS VARCHAR)
                WHERE pa.groupname = ?
                    AND pa.requestdate >= ?
                    AND pa.requestdate <= ?
                    AND pa.granted > 0
            ),
            claimed_pa AS (
                SELECT DISTINCT
                    CAST(c.panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE CAST(c.nhisgroupid AS VARCHAR) = ?
                    AND c.datesubmitted >= ?
                    AND c.datesubmitted <= ?
                    AND c.panumber IS NOT NULL
            ),
            unclaimed_pa_with_plans AS (
                SELECT
                    pa.panumber,
                    pa.granted,
                    -- Use direct match if available, otherwise use plan-based match
                    COALESCE(pa.direct_planid, pa.plan_planid) as planid,
                    COALESCE(pa.direct_planname, pa.plan_planname, 'Unknown Plan') as planname
                FROM pa_issued pa
                LEFT JOIN claimed_pa cp ON
                    CAST(pa.panumber AS BIGINT) = cp.panumber
                WHERE cp.panumber IS NULL
                    AND COALESCE(pa.direct_planid, pa.plan_planid) IS NOT NULL
            )
            SELECT
                CAST(planid AS VARCHAR) as planid,
                planname,
                SUM(granted) as unclaimed_pa_cost,
                COUNT(*) as unclaimed_pa_count
            FROM unclaimed_pa_with_plans
            GROUP BY planid, planname
            """

            pa_df = self.conn.execute(
                pa_by_plan_query,
                [company_name, start_date, end_date, groupid, start_date, end_date]
            ).fetchdf()

            if not pa_df.empty:
                print(f"\n✅ PA Attribution Success!")
                print(f"   Plans with unclaimed PA: {len(pa_df)}")
                print(f"   Total unclaimed PA: ₦{pa_df['unclaimed_pa_cost'].sum():,.2f}")

                # Show breakdown by plan
                for _, row in pa_df.iterrows():
                    print(f"   - {row['planname']}: ₦{row['unclaimed_pa_cost']:,.2f} ({row['unclaimed_pa_count']:,} PAs)")
            else:
                print("\n✅ No unclaimed PA (or no PA data for this period)")

            # ========================================
            # STEP 4: Merge all cost data with enrollment
            # ========================================

            print(f"\n=== Merging All Data ===")
            print(f"Enrollment plans: {len(enrollment_df)}")
            print(f"Claims plans: {len(claims_df)}")
            print(f"PA plans: {len(pa_df)}")

            # Start with enrollment
            merged_df = enrollment_df.copy()

            # Add claims data
            if not claims_df.empty:
                # Clean up planid - remove .0 suffix and convert to string
                claims_df['planid'] = claims_df['planid'].astype(str).str.replace(r'\.0$', '', regex=True)

                # Check for matches
                matching_planids = set(enrollment_df['planid']) & set(claims_df['planid'])
                print(f"   Matching planids (enrollment ∩ claims): {len(matching_planids)}")

                merged_df = merged_df.merge(
                    claims_df[['planid', 'claims_cost', 'claim_count', 'unique_claimants']],
                    on='planid',
                    how='left'
                )
            else:
                merged_df['claims_cost'] = 0
                merged_df['claim_count'] = 0
                merged_df['unique_claimants'] = 0

            # Add PA data (now uses planid for better attribution!)
            if not pa_df.empty:
                # Clean up planid
                pa_df['planid'] = pa_df['planid'].astype(str).str.replace(r'\.0$', '', regex=True)

                # Merge on planid (we now have planid for PA too!)
                matching_pa_planids = set(enrollment_df['planid']) & set(pa_df['planid'])
                print(f"   Matching planids (enrollment ∩ PA): {len(matching_pa_planids)}")

                merged_df = merged_df.merge(
                    pa_df[['planid', 'unclaimed_pa_cost', 'unclaimed_pa_count']],
                    on='planid',
                    how='left'
                )
            else:
                merged_df['unclaimed_pa_cost'] = 0
                merged_df['unclaimed_pa_count'] = 0

            # Fill nulls
            merged_df['claims_cost'] = merged_df['claims_cost'].fillna(0)
            merged_df['claim_count'] = merged_df['claim_count'].fillna(0).astype(int)
            merged_df['unique_claimants'] = merged_df['unique_claimants'].fillna(0).astype(int)
            merged_df['unclaimed_pa_cost'] = merged_df['unclaimed_pa_cost'].fillna(0)
            merged_df['unclaimed_pa_count'] = merged_df['unclaimed_pa_count'].fillna(0).astype(int)

            # Calculate TOTAL COST per plan (Claims + Unclaimed PA)
            merged_df['total_cost'] = (
                merged_df['claims_cost'] + merged_df['unclaimed_pa_cost']
            ).round(2)

            # Calculate total cost across all plans
            total_cost_all_plans = merged_df['total_cost'].sum()

            print(f"\n✅ Merge Complete!")
            print(f"   Total cost across all plans: ₦{total_cost_all_plans:,.2f}")

            # ========================================
            # STEP 5: Calculate Metrics
            # ========================================

            # Cost percentage
            if total_cost_all_plans > 0:
                merged_df['cost_pct'] = (
                    (merged_df['total_cost'] / total_cost_all_plans * 100).round(2)
                )
            else:
                merged_df['cost_pct'] = 0

            # Utilization variance (Cost % - Enrollment %)
            merged_df['utilization_variance'] = (
                merged_df['cost_pct'] - merged_df['enrollment_pct']
            ).round(2)

            # Claimant ratio
            merged_df['claimant_ratio'] = (
                (merged_df['unique_claimants'] / merged_df['total_enrolled'] * 100)
                .round(2)
                .fillna(0)
            )

            # Cost per member
            merged_df['cost_per_member'] = (
                (merged_df['total_cost'] / merged_df['total_enrolled'])
                .round(2)
                .fillna(0)
            )

            # Status based on utilization variance
            def get_status(variance):
                if variance > 20:
                    return 'OVER_UTILIZING'
                elif variance < -20:
                    return 'UNDER_UTILIZING'
                else:
                    return 'BALANCED'

            merged_df['status'] = merged_df['utilization_variance'].apply(get_status)

            # Recommendations
            def get_recommendations(row):
                variance = row['utilization_variance']
                recs = []
                if variance > 20:
                    recs.append(f"Increase premium by minimum {abs(variance):.0f}%")
                    recs.append("Implement enhanced pre-authorization controls")
                    recs.append("Review provider network for cost efficiency")
                elif variance < -20:
                    recs.append("Maintain current pricing - highly profitable")
                    recs.append("Consider slight reduction to capture market share")
                    recs.append("Monitor for adverse selection if market conditions change")
                else:
                    recs.append("Keep current pricing - appropriate utilization")
                    recs.append("Apply standard inflation adjustment (3-5%)")
                return recs

            merged_df['recommendations'] = merged_df.apply(get_recommendations, axis=1)

            # ========================================
            # STEP 7: Generate Insights
            # ========================================

            insights = []

            # Check for severe over/under utilization
            for _, row in merged_df.iterrows():
                variance = row['utilization_variance']
                if abs(variance) > 20:
                    if variance > 0:
                        insights.append(
                            f"🔴 {row['planname']} severely OVER-utilizing ({variance:+.1f}% variance) - "
                            f"₦{row['total_cost']:,.0f} cost on {row['enrollment_pct']:.1f}% enrollment"
                        )
                    else:
                        insights.append(
                            f"🟢 {row['planname']} UNDER-utilizing ({variance:.1f}% variance) - "
                            f"Pure profit plan with ₦{row['total_cost']:,.0f} cost on {row['enrollment_pct']:.1f}% enrollment"
                        )

            # Check for plans with zero claims
            zero_cost_plans = merged_df[merged_df['total_cost'] == 0]
            for _, row in zero_cost_plans.iterrows():
                insights.append(
                    f"⚠️ {row['planname']} has ZERO claims/PA despite {row['total_enrolled']} enrolled - "
                    f"investigate member engagement"
                )

            # Terminated enrollees warning
            if terminated_claims_cost > 0:
                insights.append(
                    f"⚠️ ₦{terminated_claims_cost:,.0f} in claims from {terminated_enrollees} TERMINATED enrollees - "
                    f"cannot be attributed to current plans"
                )

            # Overall distribution insight
            if len(merged_df) > 1:
                max_enrollment_plan = merged_df.loc[merged_df['enrollment_pct'].idxmax(), 'planname']
                max_cost_plan = merged_df.loc[merged_df['cost_pct'].idxmax(), 'planname']

                if max_enrollment_plan != max_cost_plan:
                    insights.append(
                        f"💡 Enrollment concentrated in '{max_enrollment_plan}' but costs concentrated in "
                        f"'{max_cost_plan}' - pricing imbalance detected"
                    )

            # ========================================
            # STEP 6: Prepare Return Data
            # ========================================

            # Convert to dict for JSON serialization
            plan_details = merged_df.to_dict('records')

            # Clean up numpy types
            for plan in plan_details:
                for key, value in plan.items():
                    if isinstance(value, (np.integer, np.floating)):
                        plan[key] = float(value) if isinstance(value, np.floating) else int(value)

            # Calculate summary statistics
            over_utilizing_count = len(merged_df[merged_df['status'] == 'OVER_UTILIZING'])
            under_utilizing_count = len(merged_df[merged_df['status'] == 'UNDER_UTILIZING'])

            print("\n" + "="*60)
            print("PLAN ANALYSIS COMPLETE")
            print("="*60)
            print(f"Total Plans: {len(merged_df)}")
            print(f"Over-Utilizing: {over_utilizing_count}")
            print(f"Under-Utilizing: {under_utilizing_count}")
            print(f"Normal: {len(merged_df) - over_utilizing_count - under_utilizing_count}")
            print(f"Total Cost: ₦{total_cost_all_plans:,.2f}")
            print(f"  - Claims: ₦{merged_df['claims_cost'].sum():,.2f}")
            print(f"  - Unclaimed PA: ₦{merged_df['unclaimed_pa_cost'].sum():,.2f}")
            print("="*60 + "\n")

            return {
                'success': True,
                'total_enrolled': int(total_enrolled),
                'total_plans': len(merged_df),
                'total_claims_cost': float(merged_df['claims_cost'].sum()),
                'total_pa_cost': float(merged_df['unclaimed_pa_cost'].sum()),
                'total_cost': float(total_cost_all_plans),
                'terminated_cost': terminated_claims_cost,
                'terminated_enrollees': terminated_enrollees,
                'plan_details': plan_details,
                'insights': insights
            }

        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': f'Error analyzing plan distribution: {str(e)}',
                'traceback': traceback.format_exc()
            }

    def analyze_fraud(self, groupid: str, start_date, end_date) -> Dict:
        """Analyze fraud indicators"""
        if not self.conn:
            self.connect()
        
        # Unknown provider analysis
        provider_result = self.analyze_providers(groupid, start_date, end_date)
        unknown_pct = provider_result.get('unknown_pct', 0)
        unknown_amount = provider_result.get('unknown_amount', 0)
        
        # Risk level based on unknown provider %
        if unknown_pct > 30:
            risk_level = "HIGH RISK"
        elif unknown_pct > 10:
            risk_level = "MEDIUM RISK"
        else:
            risk_level = "LOW RISK"
        
        # Same-day multiple claims
        same_day_query = """
        SELECT 
            enrollee_id,
            DATE(encounterdatefrom) as service_date,
            COUNT(*) as claims_same_day
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        WHERE CAST(nhisgroupid AS VARCHAR) = ?
            AND datesubmitted >= ?
            AND datesubmitted <= ?
        GROUP BY enrollee_id, DATE(encounterdatefrom)
        HAVING COUNT(*) >= 5
        ORDER BY claims_same_day DESC
        """
        
        same_day_df = self.conn.execute(same_day_query, [groupid, start_date, end_date]).fetchdf()
        same_day_count = len(same_day_df)
        
        return {
            'success': True,
            'risk_level': risk_level,
            'unknown_pct': unknown_pct,
            'unknown_amount': unknown_amount,
            'same_day_count': same_day_count,
            'same_day_instances': same_day_df.to_dict('records') if not same_day_df.empty else []
        }

    def calculate_renewal_risk_score(self, mlr: Dict, concentration: Dict,
                                     chronic_disease: Dict, financial: Dict,
                                     trend_decomp: Dict) -> Dict:
        """Calculate comprehensive renewal risk score (0-100)"""

        try:
            # Component 1: MLR Risk (30% weight)
            bv_mlr = mlr.get('bv_mlr', 0)
            if bv_mlr < 65:
                mlr_score = 20
            elif bv_mlr < 85:
                mlr_score = min(30 + ((bv_mlr - 65) / 20) * 30, 60)  # 30-60
            elif bv_mlr < 100:
                mlr_score = min(60 + ((bv_mlr - 85) / 15) * 20, 80)  # 60-80
            else:
                mlr_score = min(80 + ((bv_mlr - 100) / 20) * 20, 100)  # 80-100

            # Component 2: HCC Persistence (25% weight)
            concentration_type = concentration.get('type', 'UNKNOWN')
            chronic_pct = chronic_disease.get('cost_impact', {}).get('chronic_pct', 0) if chronic_disease.get('success') else 0

            if concentration_type == 'EPISODIC' and chronic_pct < 30:
                hcc_score = 30  # Low risk - episodic events
            elif concentration_type == 'STRUCTURAL' or chronic_pct > 40:
                hcc_score = 90  # High risk - persistent chronic conditions
            else:
                hcc_score = 60  # Medium risk

            # Component 3: Chronic Disease Burden (20% weight)
            if chronic_disease.get('success'):
                prevalence = chronic_disease.get('prevalence', {})
                total_chronic_members = sum([p.get('count', 0) for p in prevalence.values()])
                # Estimate total population from MLR or use default
                total_pop = 100  # This should come from enrollment data
                chronic_burden_pct = (total_chronic_members / total_pop * 100) if total_pop > 0 else chronic_pct

                if chronic_burden_pct < 20:
                    chronic_score = 20
                elif chronic_burden_pct < 30:
                    chronic_score = 50
                elif chronic_burden_pct < 40:
                    chronic_score = 70
                else:
                    chronic_score = 90
            else:
                chronic_score = 50  # Default to medium if no data

            # Component 4: Cash Collection (15% weight)
            payment_rate = financial.get('payment_rate', 0)

            if payment_rate > 90:
                collection_score = 10
            elif payment_rate > 70:
                collection_score = 50
            else:
                collection_score = 90

            # Component 5: Claims Trend (10% weight)
            if trend_decomp.get('success'):
                total_trend = trend_decomp.get('total_trend', 0)

                if total_trend < 5:
                    trend_score = 20
                elif total_trend < 10:
                    trend_score = 50
                elif total_trend < 15:
                    trend_score = 70
                else:
                    trend_score = 90
            else:
                trend_score = 50  # Default to medium if no data

            # Calculate weighted total score
            total_risk_score = round(
                (mlr_score * 0.30) +
                (hcc_score * 0.25) +
                (chronic_score * 0.20) +
                (collection_score * 0.15) +
                (trend_score * 0.10)
            )

            # Determine risk category
            if total_risk_score <= 30:
                risk_category = 'LOW_RISK'
                renewal_action = 'RENEW AT CURRENT TERMS'
                recommended_premium_change = 5.0
            elif total_risk_score <= 60:
                risk_category = 'MEDIUM_RISK'
                renewal_action = 'RENEW WITH ADJUSTMENTS'
                recommended_premium_change = 10.0 + (total_risk_score - 30) / 3  # 10-15%
            elif total_risk_score <= 80:
                risk_category = 'HIGH_RISK'
                renewal_action = 'MAJOR CHANGES REQUIRED'
                recommended_premium_change = 15.0 + (total_risk_score - 60) / 2  # 15-25%
            else:
                risk_category = 'EXTREME_RISK'
                renewal_action = 'CONSIDER NON-RENEWAL'
                recommended_premium_change = 25.0 + (total_risk_score - 80)  # 25%+

            return {
                'success': True,
                'total_risk_score': total_risk_score,
                'risk_category': risk_category,
                'component_scores': {
                    'mlr_risk': round(mlr_score),
                    'hcc_persistence': round(hcc_score),
                    'chronic_burden': round(chronic_score),
                    'collection_risk': round(collection_score),
                    'trend_risk': round(trend_score)
                },
                'renewal_action': renewal_action,
                'recommended_premium_change': round(recommended_premium_change, 1)
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'total_risk_score': 50,
                'risk_category': 'UNKNOWN',
                'component_scores': {},
                'renewal_action': 'INSUFFICIENT_DATA',
                'recommended_premium_change': 0
            }

    def generate_basic_recommendation(self, mlr_metrics: Dict, concentration: Dict, 
                                     financial: Dict, fraud: Dict, conditions: Dict) -> Dict:
        """Generate basic rule-based recommendation"""
        
        bv_mlr = mlr_metrics.get('bv_mlr', 0)
        concentration_type = concentration.get('type', 'UNKNOWN')
        top_5_pct = concentration.get('top_5_pct', 0)
        payment_rate = financial.get('payment_rate', 0)
        chronic_pct = conditions.get('chronic_pct', 0)
        unknown_pct = fraud.get('unknown_pct', 0)
        
        if bv_mlr is None or bv_mlr == 0:
            return {
                'action': 'INSUFFICIENT DATA',
                'premium_change_pct': 0,
                'success_probability': 0,
                'reasoning': 'No debit note data available'
            }
        
        # Check payment first
        if payment_rate < 10:
            return {
                'action': 'TERMINATE',
                'premium_change_pct': 0,
                'success_probability': 0,
                'reasoning': 'Zero payment - no leverage for utilization management'
            }
        
        # Check fraud
        if unknown_pct > 30:
            return {
                'action': 'TERMINATE or INCREASE 50%+',
                'premium_change_pct': 50,
                'success_probability': 20,
                'reasoning': 'High fraud risk (>30% unknown providers)'
            }
        
        # MLR-based logic
        if concentration_type == "EPISODIC" and top_5_pct > 40:
            if bv_mlr < 85:
                action = "KEEP PREMIUM"
                change = 0
            else:
                action = "INCREASE 5-10%"
                change = 7.5
        elif bv_mlr > 100:
            if chronic_pct > 40:
                action = "INCREASE 25%+ or TERMINATE"
                change = 25
            else:
                action = "INCREASE 20%+"
                change = 20
        elif bv_mlr > 85:
            action = "INCREASE 15-20%"
            change = 17.5
        elif bv_mlr > 75:
            action = "INCREASE 10-15%"
            change = 12.5
        else:
            action = "KEEP PREMIUM"
            change = 0
        
        # Success probability
        if bv_mlr < 75:
            prob = 90
        elif bv_mlr < 85:
            prob = 70
        elif bv_mlr < 100:
            prob = 50
        else:
            prob = 30
        
        return {
            'action': action,
            'premium_change_pct': change,
            'success_probability': prob,
            'reasoning': f"MLR {bv_mlr:.1f}%, {concentration_type} pattern"
        }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def run_comprehensive_analysis(company_name: str,
                              debit_override: Optional[float] = None,
                              cash_override: Optional[float] = None,
                              use_motherduck: bool = False) -> Dict:
    """
    Main analysis function - combines all calculations

    Args:
        company_name: Name of the company to analyze
        debit_override: Optional override for debit note amount
        cash_override: Optional override for cash received amount
        use_motherduck: If True, use MotherDuck cloud database instead of local

    Returns complete data structure for AI analysis
    """

    engine = CalculationEngine(use_motherduck=use_motherduck)
    
    try:
        # Get contract details
        contract = engine.get_company_contract_details(company_name)
        if not contract.get('success'):
            return contract
        
        groupid = contract['groupid']
        start_date = contract['startdate']
        end_date = contract['enddate']
        months_elapsed = contract['months_elapsed']
        
        # Get all data
        financial = engine.get_financial_data(company_name, start_date, end_date, debit_override, cash_override)
        claims = engine.get_claims_data(groupid, start_date, end_date)
        pa = engine.get_pa_data(company_name, groupid, start_date, end_date)
        enrollment = engine.get_enrollment_data(groupid)
        
        # Calculate metrics
        mlr = engine.calculate_mlr_metrics(financial, claims, pa, enrollment, months_elapsed)
        concentration = engine.analyze_member_concentration(groupid, start_date, end_date)
        conditions = engine.analyze_conditions(groupid, start_date, end_date)
        providers = engine.analyze_providers(groupid, start_date, end_date)
        fraud = engine.analyze_fraud(groupid, start_date, end_date)

        # Enhanced analyses
        monthly_pmpm = engine.calculate_monthly_pmpm_trend(groupid, start_date, end_date, enrollment)
        chronic_disease = engine.analyze_chronic_disease_burden(groupid, start_date, end_date)
        trend_decomp = engine.decompose_claims_trend(groupid, start_date, end_date, enrollment)

        # Calculate comprehensive risk score
        risk_score = engine.calculate_renewal_risk_score(
            mlr,
            concentration,
            chronic_disease,
            financial,
            trend_decomp
        )

        # Provider banding analysis
        provider_bands = engine.analyze_provider_bands(groupid, start_date, end_date)

        # Plan distribution analysis
        plan_analysis = engine.analyze_plan_distribution(company_name, groupid, start_date, end_date)

        # Benefit limit analysis
        benefit_analysis = engine.analyze_company_benefit_limits(company_name, groupid, start_date, end_date)

        # Generate basic recommendation
        recommendation = engine.generate_basic_recommendation(mlr, concentration, financial, fraud, conditions)

        # Return complete structure
        return {
            'success': True,
            'company_name': company_name,
            'contract': contract,
            'financial': financial,
            'claims': claims,
            'pa': pa,
            'enrollment': enrollment,
            'mlr': mlr,
            'concentration': concentration,
            'conditions': conditions,
            'providers': providers,
            'fraud': fraud,
            'recommendation': recommendation,
            # New enhanced metrics
            'monthly_pmpm': monthly_pmpm,
            'chronic_disease': chronic_disease,
            'trend_decomposition': trend_decomp,
            'risk_score': risk_score,
            # Provider banding and plan analysis
            'provider_bands': provider_bands,
            'plan_analysis': plan_analysis,
            # Benefit limit analysis
            'benefit_analysis': benefit_analysis
        }
    
    finally:
        engine.close()