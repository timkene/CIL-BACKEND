#!/usr/bin/env python3
"""
Hospital Band Analysis - Enhanced Actuarial Dashboard
======================================================

Professional hospital tariff banding with actuarial best practices:
- Quality Layer: Flag hospitals with poor outcomes
- Risk Flags: Track procedures >P90/P95 for fraud detection
- Dual Reporting: Weighted (TCOC) + Unweighted (unit price) bands
- Episode Analysis: Chronic disease total cost of care

Author: Casey's AI Assistant  
Date: November 2025
Version: 4.0-Enhanced-Actuarial
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
import io
from pathlib import Path
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Page configuration
st.set_page_config(
    page_title="Hospital Band Analysis - Enhanced",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .quality-alert {
        background: #ff4444;
        padding: 1rem;
        border-radius: 8px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .fraud-alert {
        background: #ff9800;
        padding: 1rem;
        border-radius: 8px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .metric-good {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-warning {
        background: linear-gradient(135deg, #f2994a 0%, #f2c94c 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .metric-bad {
        background: linear-gradient(135deg, #eb3349 0%, #f45c43 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
</style>
""", unsafe_allow_html=True)


class DuckDBDataLoader:
    """Load all required data from DuckDB"""
    
    def __init__(self, db_path: str = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'):
        self.db_path = db_path
        
    def get_connection(self):
        """Get DuckDB connection"""
        if not Path(self.db_path).exists():
            st.error(f"❌ DuckDB file not found: {self.db_path}")
            return None
        return duckdb.connect(self.db_path, read_only=True)
    
    @st.cache_data(ttl=300)
    def load_claims_stats(_self) -> pd.DataFrame:
        """Load claims statistics for frequency and P90/P95 calculation"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            WITH cleaned_claims AS (
                SELECT 
                    LOWER(TRIM(code)) as procedurecode,
                    chargeamount
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE code IS NOT NULL 
                    AND chargeamount > 0
                    AND chargeamount IS NOT NULL
            ),
            percentiles AS (
                SELECT 
                    procedurecode,
                    APPROX_QUANTILE(chargeamount, 0.01) as p01,
                    APPROX_QUANTILE(chargeamount, 0.25) as p25,
                    APPROX_QUANTILE(chargeamount, 0.50) as p50,
                    APPROX_QUANTILE(chargeamount, 0.75) as p75,
                    APPROX_QUANTILE(chargeamount, 0.90) as p90,
                    APPROX_QUANTILE(chargeamount, 0.95) as p95,
                    APPROX_QUANTILE(chargeamount, 0.99) as p99,
                    COUNT(*) as total_count
                FROM cleaned_claims
                GROUP BY procedurecode
            ),
            trimmed_stats AS (
                SELECT 
                    c.procedurecode,
                    p.p25, p.p50, p.p75, p.p90, p.p95,
                    AVG(c.chargeamount) as mean,
                    STDDEV(c.chargeamount) as std,
                    COUNT(*) as count
                FROM cleaned_claims c
                JOIN percentiles p ON c.procedurecode = p.procedurecode
                WHERE c.chargeamount >= p.p01 
                    AND c.chargeamount <= p.p99
                GROUP BY c.procedurecode, p.p25, p.p50, p.p75, p.p90, p.p95
            )
            SELECT * FROM trimmed_stats
            WHERE count >= 5
            ORDER BY count DESC
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            st.success(f"✅ Loaded claims statistics for {len(df):,} procedures")
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load claims data: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_quality_metrics(_self) -> pd.DataFrame:
        """Load provider quality metrics from claims data"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            WITH readmission_check AS (
                SELECT
                    nhisproviderid,
                    enrollee_id,
                    diagnosiscode,
                    datesubmitted,
                    LAG(datesubmitted) OVER (PARTITION BY nhisproviderid, enrollee_id, diagnosiscode ORDER BY datesubmitted) as prev_visit
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisproviderid IS NOT NULL
                    AND datesubmitted >= DATE '2024-01-01'
                    AND enrollee_id IS NOT NULL
                    AND diagnosiscode IS NOT NULL
            ),
            readmissions AS (
                SELECT
                    nhisproviderid,
                    COUNT(DISTINCT CASE
                        WHEN prev_visit IS NOT NULL
                        AND date_diff('day', prev_visit, datesubmitted) <= 30
                        THEN enrollee_id
                    END) as readmission_count
                FROM readmission_check
                GROUP BY nhisproviderid
            ),
            provider_outcomes AS (
                SELECT
                    c.nhisproviderid,
                    p.providername,
                    COUNT(DISTINCT c.enrollee_id) as patient_count,
                    COUNT(*) as claim_count,
                    AVG(c.chargeamount) as avg_charge,

                    -- High-cost outlier count (>P95 for procedure)
                    COUNT(CASE WHEN c.chargeamount >
                        (SELECT APPROX_QUANTILE(chargeamount, 0.95)
                         FROM "AI DRIVEN DATA"."CLAIMS DATA" c2
                         WHERE c2.code = c.code)
                    THEN 1 END) as high_cost_outlier_count,

                    -- Denial rate (proxy for quality issues)
                    SUM(CASE WHEN c.deniedamount > 0 THEN 1 ELSE 0 END)::FLOAT /
                    NULLIF(COUNT(*), 0) * 100 as denial_rate

                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
                    ON c.nhisproviderid = p.providerid
                WHERE c.nhisproviderid IS NOT NULL
                    AND c.datesubmitted >= DATE '2024-01-01'
                    AND (p.providername IS NULL OR LOWER(p.providername) NOT LIKE '%nhis%')
                GROUP BY c.nhisproviderid, p.providername
                HAVING COUNT(*) >= 50  -- Minimum claims for statistical validity
            )
            SELECT
                po.*,
                COALESCE(r.readmission_count, 0) as readmission_count,
                -- Quality score (0-100, higher is better)
                GREATEST(0, 100 -
                    (COALESCE(r.readmission_count, 0)::FLOAT / NULLIF(po.patient_count, 0) * 100 * 2) -  -- Weight readmissions heavily
                    (po.high_cost_outlier_count::FLOAT / NULLIF(po.claim_count, 0) * 100) -
                    (po.denial_rate * 0.5)  -- Denial rate already in percentage
                ) as quality_score
            FROM provider_outcomes po
            LEFT JOIN readmissions r ON po.nhisproviderid = r.nhisproviderid
            ORDER BY quality_score DESC
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            if len(df) > 0:
                st.success(f"✅ Loaded quality metrics for {len(df):,} providers")
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load quality metrics: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_episode_data(_self, chronic_conditions: List[str] = None) -> pd.DataFrame:
        """
        Load episode-level data for chronic disease analysis
        
        Parameters:
        -----------
        chronic_conditions : List[str]
            List of ICD-10 diagnosis codes for chronic conditions
            Default: Diabetes (E10-E14), Hypertension (I10-I15)
        """
        if chronic_conditions is None:
            chronic_conditions = [
                'E10', 'E11', 'E12', 'E13', 'E14',  # Diabetes
                'I10', 'I11', 'I12', 'I13', 'I15',  # Hypertension
                'J45',  # Asthma
                'N18',  # Chronic kidney disease
                'I50',  # Heart failure
            ]
        
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            # Build condition filter
            condition_filter = " OR ".join([
                f"c.diagnosiscode LIKE '{code}%'" for code in chronic_conditions
            ])
            
            query = f"""
            WITH chronic_episodes AS (
                SELECT 
                    c.enrollee_id,
                    c.nhisproviderid,
                    p.providername,
                    SUBSTR(c.diagnosiscode, 1, 3) as condition_category,
                    DATE_TRUNC('month', c.datesubmitted) as episode_month,
                    COUNT(*) as visit_count,
                    SUM(c.approvedamount) as total_cost,
                    AVG(c.approvedamount) as avg_cost_per_visit,
                    COUNT(DISTINCT c.code) as unique_procedures
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p
                    ON c.nhisproviderid = p.providerid
                WHERE ({condition_filter})
                    AND c.datesubmitted >= DATE '2024-01-01'
                    AND c.approvedamount > 0
                    AND (p.providername IS NULL OR LOWER(p.providername) NOT LIKE '%nhis%')
                GROUP BY 
                    c.enrollee_id,
                    c.nhisproviderid,
                    p.providername,
                    SUBSTR(c.diagnosiscode, 1, 3),
                    DATE_TRUNC('month', c.datesubmitted)
            )
            SELECT 
                nhisproviderid,
                providername,
                condition_category,
                COUNT(DISTINCT enrollee_id) as patient_count,
                COUNT(DISTINCT episode_month) as episode_count,
                SUM(total_cost) as total_episode_cost,
                AVG(total_cost) as avg_cost_per_episode,
                AVG(visit_count) as avg_visits_per_episode,
                SUM(visit_count) as total_visits
            FROM chronic_episodes
            GROUP BY nhisproviderid, providername, condition_category
            HAVING COUNT(DISTINCT enrollee_id) >= 5  -- Minimum patients for stability
            ORDER BY total_episode_cost DESC
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            if len(df) > 0:
                st.success(f"✅ Loaded episode data for {len(df):,} provider-condition combinations")
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load episode data: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=300)
    def load_hospital_list(_self) -> pd.DataFrame:
        """Load list of hospitals that have tariff data (providerid, providername, current band)."""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            query = """
            SELECT DISTINCT
                CAST(p.providerid AS VARCHAR) AS providerid,
                TRIM(p.providername) AS providername,
                COALESCE(NULLIF(TRIM(p.bands), ''), 'Unspecified') AS current_band
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
            INNER JOIN "AI DRIVEN DATA"."TARIFF" t ON pt.tariffid = t.tariffid
            WHERE t.tariffamount > 0
              AND t.procedurecode IS NOT NULL
            ORDER BY p.providername
            """
            df = conn.execute(query).fetchdf()
            conn.close()
            return df
        except Exception as e:
            st.warning(f"⚠️ Could not load hospital list: {e}")
            return pd.DataFrame()

    def get_hospital_tariff(self, provider_id: str) -> Optional[pd.DataFrame]:
        """
        Get current tariff from system for a hospital by provider ID.
        Returns DataFrame with columns: procedurecode, tariffamount (required by banding engine).
        """
        try:
            conn = self.get_connection()
            if conn is None:
                return None
            query = """
            SELECT
                LOWER(TRIM(t.procedurecode)) AS procedurecode,
                CAST(t.tariffamount AS DOUBLE) AS tariffamount
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt ON p.protariffid = pt.protariffid
            INNER JOIN "AI DRIVEN DATA"."TARIFF" t ON pt.tariffid = t.tariffid
            WHERE CAST(p.providerid AS VARCHAR) = ?
              AND t.tariffamount > 0
              AND t.procedurecode IS NOT NULL
            ORDER BY t.procedurecode
            """
            result = conn.execute(query, [provider_id.strip()]).fetchdf()
            conn.close()
            if result.empty:
                return None
            result['procedurecode'] = result['procedurecode'].astype(str).str.strip().str.lower().str.replace(' ', '')
            return result
        except Exception as e:
            st.warning(f"⚠️ Tariff fetch failed for provider {provider_id}: {e}")
            return None


class EnhancedBandingEngine:
    """
    Enhanced hospital banding with dual reporting and quality assessment
    """
    
    def __init__(self, 
                 standard_tariff_df: pd.DataFrame,
                 claims_stats: pd.DataFrame = None,
                 quality_metrics: pd.DataFrame = None):
        """
        Initialize enhanced banding engine
        
        Parameters:
        -----------
        standard_tariff_df : pd.DataFrame
            Standard tariff with band columns
        claims_stats : pd.DataFrame, optional
            Claims statistics with P90/P95
        quality_metrics : pd.DataFrame, optional
            Provider quality scores
        """
        self.standard_df = standard_tariff_df.copy()
        self.claims_stats = claims_stats
        self.quality_metrics = quality_metrics
        self.thresholds = None
        self.std_dict = None
        
        self._prepare_standard_tariff()
        self._calculate_thresholds()
    
    def _normalize_code(self, value) -> str:
        """Normalize procedure codes"""
        if value is None or pd.isna(value):
            return ""
        try:
            return str(value).strip().lower().replace(" ", "")
        except:
            return str(value)
    
    def _prepare_standard_tariff(self):
        """Prepare standard tariff"""
        # Normalize codes
        self.standard_df['procedurecode'] = self.standard_df['procedurecode'].apply(self._normalize_code)
        
        # Ensure numeric types
        band_cols = ['band_a', 'band_b', 'band_c', 'band_d', 'band_special']
        for col in band_cols:
            if col in self.standard_df.columns:
                self.standard_df[col] = pd.to_numeric(self.standard_df[col], errors='coerce')
        
        # Set frequency
        if 'frequency' not in self.standard_df.columns:
            self.standard_df['frequency'] = 1
        self.standard_df['effective_frequency'] = self.standard_df['frequency']
        
        # Update frequencies from claims
        if self.claims_stats is not None and not self.claims_stats.empty:
            claims_dict = self.claims_stats.set_index('procedurecode')['count'].to_dict()
            for idx, row in self.standard_df.iterrows():
                proc_code = row['procedurecode']
                if proc_code in claims_dict:
                    self.standard_df.at[idx, 'effective_frequency'] = claims_dict[proc_code]
        
        # Create lookup
        self.std_dict = {
            row['procedurecode']: row 
            for _, row in self.standard_df.iterrows()
        }
    
    def _calculate_thresholds(self):
        """Calculate band thresholds"""
        self.thresholds = {
            'D': float(self.standard_df['band_d'].mean()),
            'C': float(self.standard_df['band_c'].mean()),
            'B': float(self.standard_df['band_b'].mean()),
            'A': float(self.standard_df['band_a'].mean()),
            'Special': float(self.standard_df['band_special'].mean())
        }
    
    def _determine_procedure_band(self, price: float, std_proc: Dict) -> str:
        """Determine band for a procedure"""
        if price <= std_proc["band_d"]:
            return "D"
        elif price <= std_proc["band_c"]:
            return "C"
        elif price <= std_proc["band_b"]:
            return "B"
        elif price <= std_proc["band_a"]:
            return "A"
        elif price <= std_proc["band_special"]:
            return "Special"
        else:
            return "check"
    
    def _assign_band_from_avg(self, avg_price: float) -> str:
        """Assign band from average price"""
        if avg_price <= self.thresholds['D']:
            return 'D'
        elif avg_price <= self.thresholds['C']:
            return 'C'
        elif avg_price <= self.thresholds['B']:
            return 'B'
        elif avg_price <= self.thresholds['A']:
            return 'A'
        elif avg_price <= self.thresholds['Special']:
            return 'Special'
        else:
            return 'check'
    
    def analyze_tariff_dual(self, 
                           tariff_df: pd.DataFrame, 
                           tariff_name: str,
                           provider_id: str = None) -> Dict:
        """
        DUAL ANALYSIS: Both weighted (TCOC) and unweighted (unit price) banding
        
        Parameters:
        -----------
        tariff_df : pd.DataFrame
            Tariff data with procedurecode and tariffamount
        tariff_name : str
            Name of tariff
        provider_id : str, optional
            Provider ID for quality lookup
        
        Returns:
        --------
        Dict with dual analysis results
        """
        try:
            # Prepare data
            tariff_df = tariff_df.copy()
            tariff_df['procedurecode'] = tariff_df['procedurecode'].apply(self._normalize_code)
            tariff_df['tariffamount'] = pd.to_numeric(tariff_df['tariffamount'], errors='coerce')
            tariff_df = tariff_df.dropna(subset=['tariffamount'])
            tariff_df = tariff_df[tariff_df['tariffamount'] > 0]
            
            if len(tariff_df) == 0:
                return {'success': False, 'error': 'No valid procedures'}
            
            # Get claims dict
            claims_dict = {}
            if self.claims_stats is not None and not self.claims_stats.empty:
                claims_dict = {
                    row['procedurecode']: row
                    for _, row in self.claims_stats.iterrows()
                }
            
            # WEIGHTED ANALYSIS (TCOC - Total Cost of Care)
            weighted_results = self._calculate_weighted_band(tariff_df, claims_dict)
            
            # UNWEIGHTED ANALYSIS (Unit Price Negotiation)
            unweighted_results = self._calculate_unweighted_band(tariff_df)
            
            # QUALITY ASSESSMENT
            quality_results = self._assess_quality(provider_id)
            
            # FRAUD RISK ASSESSMENT
            fraud_results = self._assess_fraud_risk(tariff_df, claims_dict)
            
            return {
                'success': True,
                'tariff_name': tariff_name,
                'weighted': weighted_results,
                'unweighted': unweighted_results,
                'quality': quality_results,
                'fraud': fraud_results,
                'procedure_details': weighted_results['procedure_details']
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def _calculate_weighted_band(self, tariff_df: pd.DataFrame, claims_dict: Dict) -> Dict:
        """
        Calculate WEIGHTED band (TCOC approach)
        Uses frequency weighting to reflect total cost impact
        """
        total_weighted_price = 0.0
        total_log_freq = 0.0
        band_freq_weighted = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
        procedure_details = []
        matched = 0
        
        for _, row in tariff_df.iterrows():
            proc_code = row['procedurecode']
            price = row['tariffamount']
            
            if proc_code not in self.std_dict:
                continue
            
            matched += 1
            std_proc = self.std_dict[proc_code]
            freq = float(std_proc.get('effective_frequency', 1))
            
            # Log-scaled frequency weighting
            log_freq = np.log1p(freq)
            weighted_price = price * log_freq
            total_weighted_price += weighted_price
            total_log_freq += log_freq
            
            # Determine band
            band = self._determine_procedure_band(price, std_proc)
            band_freq_weighted[band] += log_freq
            
            # Store details
            procedure_details.append({
                'procedurecode': proc_code,
                'proceduredesc': std_proc.get('proceduredesc', 'N/A'),
                'price': price,
                'band': band,
                'frequency': freq,
                'log_frequency': log_freq,
                'weighted_price': weighted_price,
                'band_a': std_proc['band_a'],
                'band_b': std_proc['band_b'],
                'band_c': std_proc['band_c'],
                'band_d': std_proc['band_d']
            })
        
        if total_log_freq == 0:
            return {'band': 'ERROR', 'weighted_avg': 0, 'matched': 0}
        
        # Calculate weighted average
        weighted_avg = total_weighted_price / total_log_freq
        calculated_band = self._assign_band_from_avg(weighted_avg)
        
        # Dominant band logic
        total_freq = sum(band_freq_weighted.values())
        band_pcts = {
            band: (freq / total_freq * 100) if total_freq > 0 else 0
            for band, freq in band_freq_weighted.items()
        }
        
        valid_bands = {b: p for b, p in band_pcts.items() if b != 'check'}
        dominant_band = max(valid_bands, key=valid_bands.get) if valid_bands else calculated_band
        dominant_pct = valid_bands.get(dominant_band, 0)
        
        # Assignment logic
        if dominant_pct >= 60:
            final_band = dominant_band
            method = "Dominant Band (≥60%)"
        else:
            final_band = calculated_band
            method = "Weighted Average"
        
        # Gaming protection
        if band_pcts.get('check', 0) > 30:
            final_band = calculated_band
            method = "Gaming Protection"
        
        return {
            'band': final_band,
            'weighted_avg': weighted_avg,
            'calculated_band': calculated_band,
            'dominant_band': dominant_band,
            'dominant_pct': dominant_pct,
            'method': method,
            'band_pcts': band_pcts,
            'matched': matched,
            'coverage_pct': (matched / len(self.std_dict) * 100) if len(self.std_dict) > 0 else 0,
            'procedure_details': procedure_details
        }
    
    def _calculate_unweighted_band(self, tariff_df: pd.DataFrame) -> Dict:
        """
        Calculate UNWEIGHTED band (Unit Price approach)
        Pure price evaluation without frequency weighting
        """
        total_price = 0.0
        band_count = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
        matched = 0
        
        for _, row in tariff_df.iterrows():
            proc_code = row['procedurecode']
            price = row['tariffamount']
            
            if proc_code not in self.std_dict:
                continue
            
            matched += 1
            std_proc = self.std_dict[proc_code]
            total_price += price
            
            # Determine band
            band = self._determine_procedure_band(price, std_proc)
            band_count[band] += 1
        
        if matched == 0:
            return {'band': 'ERROR', 'avg': 0, 'matched': 0}
        
        # Simple average
        simple_avg = total_price / matched
        calculated_band = self._assign_band_from_avg(simple_avg)
        
        # Dominant band
        total_count = sum(band_count.values())
        band_pcts = {
            band: (count / total_count * 100) if total_count > 0 else 0
            for band, count in band_count.items()
        }
        
        valid_bands = {b: p for b, p in band_pcts.items() if b != 'check'}
        dominant_band = max(valid_bands, key=valid_bands.get) if valid_bands else calculated_band
        dominant_pct = valid_bands.get(dominant_band, 0)
        
        # Assignment
        if dominant_pct >= 60:
            final_band = dominant_band
            method = "Dominant Band"
        else:
            final_band = calculated_band
            method = "Simple Average"
        
        return {
            'band': final_band,
            'avg': simple_avg,
            'calculated_band': calculated_band,
            'dominant_band': dominant_band,
            'dominant_pct': dominant_pct,
            'method': method,
            'band_pcts': band_pcts,
            'matched': matched
        }
    
    def _assess_quality(self, provider_id: str = None) -> Dict:
        """Assess quality metrics"""
        if self.quality_metrics is None or self.quality_metrics.empty:
            return {'available': False}
        
        if provider_id is None:
            return {'available': False}
        
        # Find provider
        provider_quality = self.quality_metrics[
            self.quality_metrics['nhisproviderid'] == provider_id
        ]
        
        if provider_quality.empty:
            return {'available': False, 'reason': 'Provider not found in quality data'}
        
        quality_row = provider_quality.iloc[0]
        quality_score = quality_row['quality_score']
        
        # Quality classification
        if quality_score >= 80:
            quality_tier = 'Excellent'
            quality_flag = 'green'
        elif quality_score >= 60:
            quality_tier = 'Good'
            quality_flag = 'yellow'
        elif quality_score >= 40:
            quality_tier = 'Fair'
            quality_flag = 'orange'
        else:
            quality_tier = 'Poor'
            quality_flag = 'red'
        
        return {
            'available': True,
            'quality_score': quality_score,
            'quality_tier': quality_tier,
            'quality_flag': quality_flag,
            'readmission_count': quality_row['readmission_count'],
            'patient_count': quality_row['patient_count'],
            'denial_rate': quality_row['denial_rate'],
            'high_cost_outliers': quality_row['high_cost_outlier_count']
        }
    
    def _assess_fraud_risk(self, tariff_df: pd.DataFrame, claims_dict: Dict) -> Dict:
        """
        Assess fraud risk based on P90/P95 thresholds
        """
        if not claims_dict:
            return {'available': False}
        
        above_p90 = []
        above_p95 = []
        extreme_outliers = []  # >2x P95
        
        for _, row in tariff_df.iterrows():
            proc_code = row['procedurecode']
            price = row['tariffamount']
            
            if proc_code not in claims_dict:
                continue
            
            claims_data = claims_dict[proc_code]
            p90 = claims_data.get('p90')
            p95 = claims_data.get('p95')
            
            if pd.notna(p90) and price > p90:
                above_p90.append({
                    'procedurecode': proc_code,
                    'price': price,
                    'p90': p90,
                    'excess': price - p90,
                    'excess_pct': ((price - p90) / p90 * 100) if p90 > 0 else 0
                })
            
            if pd.notna(p95) and price > p95:
                above_p95.append({
                    'procedurecode': proc_code,
                    'price': price,
                    'p95': p95,
                    'excess': price - p95,
                    'excess_pct': ((price - p95) / p95 * 100) if p95 > 0 else 0
                })
                
                # Extreme outliers
                if price > (p95 * 2):
                    extreme_outliers.append({
                        'procedurecode': proc_code,
                        'price': price,
                        'p95': p95,
                        'multiplier': price / p95 if p95 > 0 else 0
                    })
        
        # Risk classification
        p90_count = len(above_p90)
        p95_count = len(above_p95)
        extreme_count = len(extreme_outliers)
        total_procs = len(tariff_df)
        
        p90_pct = (p90_count / total_procs * 100) if total_procs > 0 else 0
        p95_pct = (p95_count / total_procs * 100) if total_procs > 0 else 0
        
        # Risk scoring
        if extreme_count > 0 or p95_pct > 20:
            fraud_risk = 'HIGH'
            risk_flag = 'red'
        elif p95_pct > 10 or p90_pct > 40:
            fraud_risk = 'MEDIUM'
            risk_flag = 'orange'
        elif p90_pct > 20:
            fraud_risk = 'LOW'
            risk_flag = 'yellow'
        else:
            fraud_risk = 'MINIMAL'
            risk_flag = 'green'
        
        return {
            'available': True,
            'fraud_risk': fraud_risk,
            'risk_flag': risk_flag,
            'above_p90_count': p90_count,
            'above_p90_pct': p90_pct,
            'above_p95_count': p95_count,
            'above_p95_pct': p95_pct,
            'extreme_outliers': extreme_count,
            'above_p90_details': above_p90,
            'above_p95_details': above_p95,
            'extreme_outlier_details': extreme_outliers
        }


# ============================================================================
# STREAMLIT UI
# ============================================================================

def main():
    """Main Streamlit application"""
    
    st.markdown('<h1 class="main-header">🏥 Hospital Band Analysis - Enhanced Actuarial Dashboard</h1>', 
                unsafe_allow_html=True)
    
    st.markdown("""
    ### Professional provider banding with actuarial best practices:
    - **Quality Layer**: Flag hospitals with poor clinical outcomes
    - **Fraud Detection**: Track procedures >P90/P95 from actual claims
    - **Dual Reporting**: Weighted (TCOC) + Unweighted (Unit Price) bands  
    - **Episode Analysis**: Total cost of care for chronic conditions
    """)
    
    # Initialize data loader
    loader = DuckDBDataLoader()
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Load data options
        st.subheader("Data Loading")
        load_claims = st.checkbox("Load Claims Statistics", value=True, 
                                  help="Enable P90/P95 fraud detection")
        load_quality = st.checkbox("Load Quality Metrics", value=True,
                                   help="Enable quality assessment")
        load_episodes = st.checkbox("Load Episode Data", value=False,
                                    help="Enable chronic disease TCOC analysis")
        
        if st.button("🔄 Reload Data"):
            st.cache_data.clear()
            st.rerun()
        
        st.markdown("---")
        st.markdown("**Band Thresholds:**")
        st.markdown("- D: ₦10,289")
        st.markdown("- C: ₦13,684")
        st.markdown("- B: ₦23,948")
        st.markdown("- A: ₦47,895")
        st.markdown("- Special: ₦95,790")
    
    # Load data
    with st.spinner("📊 Loading data from DuckDB..."):
        # Load standard tariff (from REALITY_TARIFF.xlsx)
        try:
            standard_df = pd.read_excel('/Users/kenechukwuchukwuka/Downloads/REALITY TARIFF.xlsx')
            st.success(f"✅ Loaded {len(standard_df):,} procedures from REALITY_TARIFF")
        except Exception as e:
            st.error(f"❌ Could not load REALITY_TARIFF.xlsx: {e}")
            return
        
        # Load claims stats
        claims_stats = loader.load_claims_stats() if load_claims else pd.DataFrame()
        
        # Load quality metrics
        quality_metrics = loader.load_quality_metrics() if load_quality else pd.DataFrame()
        
        # Load episode data
        episode_data = loader.load_episode_data() if load_episodes else pd.DataFrame()
    
    # Initialize engine
    engine = EnhancedBandingEngine(
        standard_tariff_df=standard_df,
        claims_stats=claims_stats,
        quality_metrics=quality_metrics
    )
    
    # Main tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📤 Upload & Analyze",
        "📊 Episode Analysis",
        "🏆 Quality Dashboard",
        "🚨 Fraud Detection",
        "🏥 Analyze from System"
    ])
    
    # ========================================================================
    # TAB 1: Upload & Analyze
    # ========================================================================
    with tab1:
        st.header("Upload Hospital Tariff for Analysis")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            uploaded_file = st.file_uploader(
                "Upload hospital tariff CSV",
                type=['csv'],
                help="Must contain 'procedurecode' and 'tariffamount' columns"
            )
        
        with col2:
            provider_id = st.text_input(
                "Provider ID (optional)",
                help="For quality assessment lookup"
            )
        
        if uploaded_file is not None:
            try:
                # Load tariff
                tariff_df = pd.read_csv(uploaded_file, thousands=',')
                
                # Display preview
                st.subheader("📋 Tariff Preview")
                st.dataframe(tariff_df.head(10), use_container_width=True)
                
                # Analyze button
                if st.button("🔍 Analyze Tariff", type="primary"):
                    with st.spinner("Analyzing tariff..."):
                        # Get tariff name
                        tariff_name = uploaded_file.name.replace('.csv', '')
                        
                        # Run dual analysis
                        result = engine.analyze_tariff_dual(
                            tariff_df=tariff_df,
                            tariff_name=tariff_name,
                            provider_id=provider_id if provider_id else None
                        )
                        
                        if result['success']:
                            # Display results
                            display_dual_analysis_results(result, quality_metrics, episode_data)
                        else:
                            st.error(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")
            
            except Exception as e:
                st.error(f"❌ Error loading tariff: {e}")
    
    # ========================================================================
    # TAB 2: Episode Analysis
    # ========================================================================
    with tab2:
        if episode_data.empty:
            st.info("ℹ️ Enable 'Load Episode Data' in sidebar to view chronic disease analysis")
        else:
            display_episode_analysis(episode_data)
    
    # ========================================================================
    # TAB 3: Quality Dashboard
    # ========================================================================
    with tab3:
        if quality_metrics.empty:
            st.info("ℹ️ Enable 'Load Quality Metrics' in sidebar to view quality dashboard")
        else:
            display_quality_dashboard(quality_metrics)
    
    # ========================================================================
    # TAB 4: Fraud Detection
    # ========================================================================
    with tab4:
        if claims_stats.empty:
            st.info("ℹ️ Enable 'Load Claims Statistics' in sidebar to view fraud detection")
        else:
            st.header("🚨 Fraud Detection Dashboard")
            st.info("Upload a tariff in Tab 1 to see fraud risk assessment")

    # ========================================================================
    # TAB 5: Analyze from System (hospital list + current tariff → band)
    # ========================================================================
    with tab5:
        st.header("🏥 Analyze Bands from System Tariff")
        st.markdown("Select up to **20 hospitals**. Current tariff is loaded from the database; banding uses the same engine as CSV upload.")
        hospital_list_df = loader.load_hospital_list()
        if hospital_list_df.empty:
            st.warning("No hospitals with tariff data found in the database.")
        else:
            # Build multiselect options: "ProviderName (ID: xxx)"
            option_labels = (
                hospital_list_df["providername"].astype(str)
                + " (ID: " + hospital_list_df["providerid"].astype(str) + ")"
            ).tolist()
            label_to_row = {
                lab: row for lab, row in zip(
                    option_labels,
                    hospital_list_df.to_dict("records")
                )
            }
            selected_labels = st.multiselect(
                "Select hospitals (max 20)",
                options=option_labels,
                default=[],
                max_selections=20,
                help="Current band comes from DB; new bands from tariff analysis.",
            )
            if st.button("🔍 Analyze Selected Hospitals", type="primary", key="analyze_system_btn"):
                if not selected_labels:
                    st.info("Select at least one hospital.")
                else:
                    results_rows = []
                    progress = st.progress(0)
                    n = len(selected_labels)
                    for i, label in enumerate(selected_labels):
                        progress.progress((i + 1) / n)
                        row = label_to_row[label]
                        provider_id = str(row["providerid"])
                        provider_name = str(row["providername"])
                        current_band = str(row.get("current_band", "Unspecified"))
                        tariff_df = loader.get_hospital_tariff(provider_id)
                        if tariff_df is None or tariff_df.empty:
                            results_rows.append({
                                "Hospital": provider_name,
                                "Provider ID": provider_id,
                                "Current Band (DB)": current_band,
                                "Weighted Band (New)": "—",
                                "Unweighted Band (New)": "—",
                                "Matched Procedures": 0,
                                "Note": "No tariff or no valid procedures",
                            })
                            continue
                        try:
                            res = engine.analyze_tariff_dual(
                                tariff_df=tariff_df,
                                tariff_name=provider_name,
                                provider_id=provider_id,
                            )
                        except Exception as e:
                            results_rows.append({
                                "Hospital": provider_name,
                                "Provider ID": provider_id,
                                "Current Band (DB)": current_band,
                                "Weighted Band (New)": "—",
                                "Unweighted Band (New)": "—",
                                "Matched Procedures": 0,
                                "Note": str(e),
                            })
                            continue
                        if not res.get("success"):
                            results_rows.append({
                                "Hospital": provider_name,
                                "Provider ID": provider_id,
                                "Current Band (DB)": current_band,
                                "Weighted Band (New)": "—",
                                "Unweighted Band (New)": "—",
                                "Matched Procedures": 0,
                                "Note": res.get("error", "Analysis failed"),
                            })
                            continue
                        w = res["weighted"]
                        u = res["unweighted"]
                        results_rows.append({
                            "Hospital": provider_name,
                            "Provider ID": provider_id,
                            "Current Band (DB)": current_band,
                            "Weighted Band (New)": w["band"],
                            "Unweighted Band (New)": u["band"],
                            "Matched Procedures": w.get("matched", 0),
                            "Note": "",
                        })
                    progress.empty()
                    out_df = pd.DataFrame(results_rows)
                    st.subheader("Results: Current vs New Band")
                    st.dataframe(out_df, use_container_width=True)
                    if not out_df.empty:
                        st.download_button(
                            "Download results (CSV)",
                            data=out_df.to_csv(index=False),
                            file_name="hospital_band_analysis.csv",
                            mime="text/csv",
                            key="download_system_bands",
                        )


def display_dual_analysis_results(result: Dict, quality_metrics: pd.DataFrame, episode_data: pd.DataFrame):
    """Display comprehensive dual analysis results"""
    
    st.markdown("---")
    st.header("📊 Analysis Results")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    weighted = result['weighted']
    unweighted = result['unweighted']
    quality = result['quality']
    fraud = result['fraud']
    
    with col1:
        band_color = {
            'D': 'metric-good',
            'C': 'metric-good',
            'B': 'metric-warning',
            'A': 'metric-bad',
            'Special': 'metric-bad',
            'check': 'metric-bad'
        }.get(weighted['band'], 'metric-warning')
        
        st.markdown(f"""
        <div class="{band_color}">
            <h3>Weighted Band (TCOC)</h3>
            <h1>{weighted['band']}</h1>
            <p>{weighted['method']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        band_color = {
            'D': 'metric-good',
            'C': 'metric-good',
            'B': 'metric-warning',
            'A': 'metric-bad',
            'Special': 'metric-bad',
            'check': 'metric-bad'
        }.get(unweighted['band'], 'metric-warning')
        
        st.markdown(f"""
        <div class="{band_color}">
            <h3>Unweighted Band</h3>
            <h1>{unweighted['band']}</h1>
            <p>{unweighted['method']}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        if quality['available']:
            tier_color = {
                'Excellent': 'metric-good',
                'Good': 'metric-good',
                'Fair': 'metric-warning',
                'Poor': 'metric-bad'
            }.get(quality['quality_tier'], 'metric-warning')
            
            st.markdown(f"""
            <div class="{tier_color}">
                <h3>Quality Score</h3>
                <h1>{quality['quality_score']:.0f}</h1>
                <p>{quality['quality_tier']}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Quality data not available")
    
    with col4:
        if fraud['available']:
            risk_color = {
                'MINIMAL': 'metric-good',
                'LOW': 'metric-good',
                'MEDIUM': 'metric-warning',
                'HIGH': 'metric-bad'
            }.get(fraud['fraud_risk'], 'metric-warning')
            
            st.markdown(f"""
            <div class="{risk_color}">
                <h3>Fraud Risk</h3>
                <h1>{fraud['fraud_risk']}</h1>
                <p>{fraud['above_p95_count']} above P95</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Fraud risk data not available")
    
    # Detailed tabs
    detail_tab1, detail_tab2, detail_tab3, detail_tab4 = st.tabs([
        "📈 Band Analysis",
        "🏥 Quality Details", 
        "🚨 Fraud Details",
        "📋 Procedure List"
    ])
    
    with detail_tab1:
        display_band_details(weighted, unweighted)
    
    with detail_tab2:
        display_quality_details(quality)
    
    with detail_tab3:
        display_fraud_details(fraud)
    
    with detail_tab4:
        display_procedure_details(result['procedure_details'], fraud)


def display_band_details(weighted: Dict, unweighted: Dict):
    """Display detailed band comparison"""
    
    st.subheader("Weighted vs Unweighted Band Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🎯 Weighted Band (TCOC)")
        st.markdown(f"**Final Band:** {weighted['band']}")
        st.markdown(f"**Weighted Average:** ₦{weighted['weighted_avg']:,.2f}")
        st.markdown(f"**Method:** {weighted['method']}")
        st.markdown(f"**Coverage:** {weighted['coverage_pct']:.1f}%")
        
        # Band distribution
        st.markdown("**Band Distribution:**")
        for band, pct in sorted(weighted['band_pcts'].items()):
            st.progress(pct / 100, text=f"{band}: {pct:.1f}%")
    
    with col2:
        st.markdown("### 💰 Unweighted Band (Unit Price)")
        st.markdown(f"**Final Band:** {unweighted['band']}")
        st.markdown(f"**Simple Average:** ₦{unweighted['avg']:,.2f}")
        st.markdown(f"**Method:** {unweighted['method']}")
        
        # Band distribution
        st.markdown("**Band Distribution:**")
        for band, pct in sorted(unweighted['band_pcts'].items()):
            st.progress(pct / 100, text=f"{band}: {pct:.1f}%")
    
    # Interpretation
    st.markdown("---")
    st.markdown("### 📖 Interpretation")
    
    if weighted['band'] != unweighted['band']:
        st.warning(f"""
        **Band Mismatch Detected!**
        
        - **Weighted Band ({weighted['band']})**: Reflects total cost impact based on procedure frequency
        - **Unweighted Band ({unweighted['band']})**: Reflects pure unit pricing
        
        **Recommendation:**
        - Use **Weighted Band** for member steering and network tiering
        - Use **Unweighted Band** for contract negotiations on unit prices
        """)
    else:
        st.success(f"""
        **Bands Aligned at {weighted['band']}**
        
        Both frequency-weighted and unweighted analyses agree. This hospital is consistently
        priced in Band {weighted['band']} regardless of procedure frequency.
        """)


def display_quality_details(quality: Dict):
    """Display quality metrics details"""
    
    if not quality['available']:
        st.info("Quality metrics not available for this provider")
        return
    
    st.subheader("🏆 Quality Assessment")
    
    # Quality score breakdown
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            "Readmission Rate",
            f"{(quality['readmission_count'] / quality['patient_count'] * 100):.1f}%",
            help="Lower is better"
        )
    
    with col2:
        st.metric(
            "Denial Rate",
            f"{quality['denial_rate']:.1f}%",
            help="Lower is better"
        )
    
    with col3:
        st.metric(
            "High-Cost Outliers",
            quality['high_cost_outliers'],
            help="Procedures >P95 for that code"
        )
    
    # Quality tier explanation
    st.markdown("---")
    st.markdown("### Quality Tier Classification")
    
    if quality['quality_flag'] == 'red':
        st.markdown('<div class="quality-alert">❌ POOR QUALITY - Consider exclusion from network regardless of cost</div>', 
                   unsafe_allow_html=True)
    elif quality['quality_flag'] == 'orange':
        st.warning("⚠️ FAIR QUALITY - Requires improvement plan or increased monitoring")
    elif quality['quality_flag'] == 'yellow':
        st.info("✅ GOOD QUALITY - Acceptable for network inclusion")
    else:
        st.success("🏆 EXCELLENT QUALITY - Preferred provider for network")


def display_fraud_details(fraud: Dict):
    """Display fraud risk details"""
    
    if not fraud['available']:
        st.info("Fraud risk assessment not available (requires claims data)")
        return
    
    st.subheader("🚨 Fraud Risk Assessment")
    
    # Summary
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Above P90", fraud['above_p90_count'])
    
    with col2:
        st.metric("Above P95", fraud['above_p95_count'])
    
    with col3:
        st.metric("Extreme Outliers", fraud['extreme_outliers'])
    
    with col4:
        st.metric("P95 %", f"{fraud['above_p95_pct']:.1f}%")
    
    # Risk classification
    st.markdown("---")
    
    if fraud['fraud_risk'] == 'HIGH':
        st.markdown('<div class="fraud-alert">🚨 HIGH FRAUD RISK - Immediate audit recommended</div>',
                   unsafe_allow_html=True)
        st.error(f"""
        **Red Flags:**
        - {fraud['above_p95_count']} procedures above P95 ({fraud['above_p95_pct']:.1f}%)
        - {fraud['extreme_outliers']} extreme outliers (>2x P95)
        
        **Action:** Conduct detailed audit before contract renewal
        """)
    elif fraud['fraud_risk'] == 'MEDIUM':
        st.warning(f"""
        **⚠️ MEDIUM FRAUD RISK**
        
        - {fraud['above_p95_count']} procedures above P95
        - Requires enhanced monitoring
        """)
    else:
        st.success(f"✅ {fraud['fraud_risk']} FRAUD RISK - Pricing appears reasonable")
    
    # Detailed procedure list
    if fraud['above_p95_count'] > 0:
        st.markdown("### Procedures Above P95")
        
        p95_df = pd.DataFrame(fraud['above_p95_details'])
        p95_df = p95_df.sort_values('excess', ascending=False)
        
        st.dataframe(
            p95_df.style.format({
                'price': '₦{:,.2f}',
                'p95': '₦{:,.2f}',
                'excess': '₦{:,.2f}',
                'excess_pct': '{:.1f}%'
            }),
            use_container_width=True
        )


def display_procedure_details(procedures: List[Dict], fraud: Dict):
    """Display detailed procedure list"""
    
    st.subheader("📋 Procedure Details")
    
    if not procedures:
        st.info("No matched procedures")
        return
    
    df = pd.DataFrame(procedures)
    
    # Add fraud flags
    if fraud['available'] and fraud['above_p90_details']:
        p90_codes = [p['procedurecode'] for p in fraud['above_p90_details']]
        df['above_p90'] = df['procedurecode'].isin(p90_codes)
    else:
        df['above_p90'] = False
    
    # Sort by weighted price (highest impact first)
    df = df.sort_values('weighted_price', ascending=False)
    
    # Display
    st.dataframe(
        df.style.format({
            'price': '₦{:,.2f}',
            'frequency': '{:,.0f}',
            'log_frequency': '{:.2f}',
            'weighted_price': '₦{:,.2f}',
            'band_a': '₦{:,.2f}',
            'band_b': '₦{:,.2f}',
            'band_c': '₦{:,.2f}',
            'band_d': '₦{:,.2f}'
        }).apply(lambda x: ['background-color: #ffcccc' if v else '' 
                           for v in df['above_p90']], axis=0),
        use_container_width=True
    )
    
    st.caption("🔴 Red rows indicate procedures above P90 (potential fraud risk)")


def display_episode_analysis(episode_data: pd.DataFrame):
    """Display episode-level chronic disease analysis"""
    
    st.header("📊 Episode-Level Analysis (Chronic Diseases)")
    
    st.markdown("""
    **Total Cost of Care (TCOC) approach for chronic conditions**
    
    Measures provider efficiency in managing chronic disease episodes including:
    - Diabetes (E10-E14)
    - Hypertension (I10-I15)
    - Asthma (J45)
    - Chronic Kidney Disease (N18)
    - Heart Failure (I50)
    """)
    
    # Condition selector
    conditions = sorted(episode_data['condition_category'].unique())
    selected_condition = st.selectbox("Select Condition", conditions)
    
    # Filter data
    condition_data = episode_data[episode_data['condition_category'] == selected_condition]
    condition_data = condition_data.sort_values('total_episode_cost', ascending=False)
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Providers", len(condition_data))
    
    with col2:
        st.metric("Total Patients", condition_data['patient_count'].sum())
    
    with col3:
        st.metric("Total Cost", f"₦{condition_data['total_episode_cost'].sum():,.0f}")
    
    with col4:
        st.metric("Avg Cost/Episode", f"₦{condition_data['avg_cost_per_episode'].mean():,.0f}")
    
    # Provider ranking
    st.subheader("Provider Ranking by Total Episode Cost")
    
    # Calculate efficiency metrics
    condition_data['efficiency_score'] = (
        condition_data['avg_cost_per_episode'] / 
        condition_data['avg_cost_per_episode'].median()
    )
    
    # Color code
    def color_efficiency(val):
        if val < 0.8:
            return 'background-color: #d4edda'  # Green - efficient
        elif val < 1.2:
            return 'background-color: #fff3cd'  # Yellow - average
        else:
            return 'background-color: #f8d7da'  # Red - inefficient
    
    st.dataframe(
        condition_data.style.format({
            'patient_count': '{:,.0f}',
            'episode_count': '{:,.0f}',
            'total_episode_cost': '₦{:,.2f}',
            'avg_cost_per_episode': '₦{:,.2f}',
            'avg_visits_per_episode': '{:.1f}',
            'total_visits': '{:,.0f}',
            'efficiency_score': '{:.2f}'
        }).applymap(color_efficiency, subset=['efficiency_score']),
        use_container_width=True
    )
    
    st.caption("""
    **Efficiency Score:** <0.8 = Efficient (green) | 0.8-1.2 = Average (yellow) | >1.2 = Inefficient (red)
    """)
    
    # Visualization
    fig = px.scatter(
        condition_data.head(20),
        x='patient_count',
        y='avg_cost_per_episode',
        size='total_episode_cost',
        hover_data=['providername', 'episode_count'],
        title=f'Provider Efficiency: {selected_condition}',
        labels={
            'patient_count': 'Number of Patients',
            'avg_cost_per_episode': 'Average Cost per Episode (₦)'
        }
    )
    
    st.plotly_chart(fig, use_container_width=True)


def display_quality_dashboard(quality_metrics: pd.DataFrame):
    """Display comprehensive quality dashboard"""
    
    st.header("🏆 Provider Quality Dashboard")
    
    # Overall statistics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Providers", len(quality_metrics))
    
    with col2:
        avg_quality = quality_metrics['quality_score'].mean()
        st.metric("Average Quality Score", f"{avg_quality:.1f}")
    
    with col3:
        excellent = len(quality_metrics[quality_metrics['quality_score'] >= 80])
        st.metric("Excellent Providers", excellent)
    
    with col4:
        poor = len(quality_metrics[quality_metrics['quality_score'] < 40])
        st.metric("Poor Quality Providers", poor)
    
    # Quality distribution
    st.subheader("Quality Score Distribution")
    
    fig = px.histogram(
        quality_metrics,
        x='quality_score',
        nbins=20,
        title='Distribution of Provider Quality Scores',
        labels={'quality_score': 'Quality Score', 'count': 'Number of Providers'}
    )
    
    fig.add_vline(x=80, line_dash="dash", line_color="green", annotation_text="Excellent")
    fig.add_vline(x=60, line_dash="dash", line_color="yellow", annotation_text="Good")
    fig.add_vline(x=40, line_dash="dash", line_color="red", annotation_text="Poor")
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Provider ranking
    st.subheader("Provider Quality Ranking")
    
    quality_sorted = quality_metrics.sort_values('quality_score', ascending=False)
    
    def color_quality(val):
        if val >= 80:
            return 'background-color: #d4edda'
        elif val >= 60:
            return 'background-color: #fff3cd'
        elif val >= 40:
            return 'background-color: #f8d7da'
        else:
            return 'background-color: #f5c6cb'
    
    st.dataframe(
        quality_sorted.style.format({
            'patient_count': '{:,.0f}',
            'claim_count': '{:,.0f}',
            'avg_charge': '₦{:,.2f}',
            'readmission_count': '{:,.0f}',
            'high_cost_outlier_count': '{:,.0f}',
            'denial_rate': '{:.1f}%',
            'quality_score': '{:.1f}'
        }).applymap(color_quality, subset=['quality_score']),
        use_container_width=True
    )


if __name__ == "__main__":
    main()