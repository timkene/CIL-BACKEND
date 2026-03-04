#!/usr/bin/env python3
"""
Hospital Band Analysis - Enhanced Streamlit Dashboard
======================================================

Production-ready hospital tariff banding system with actuarial best practices.

ENHANCEMENTS:
✅ Quality Layer: Flag hospitals with poor outcomes (readmissions, complications)
✅ Risk Flags: P90/P95 fraud detection with detailed tracking
✅ Dual Reporting: Weighted (TCOC) + Unweighted (Unit Price) analysis
✅ Episode-Level Analysis: Chronic condition total cost of care

Author: Casey's AI Assistant  
Date: November 2025
Version: 4.0-Enhanced
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
    page_title="Hospital Band Analysis (Enhanced)",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
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
        background: #ff6b6b;
        padding: 1rem;
        border-radius: 8px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .fraud-alert {
        background: #ffa500;
        padding: 1rem;
        border-radius: 8px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 1rem 2rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


class DuckDBTariffLoader:
    """Load and manage tariff data from DuckDB with enhanced quality metrics"""

    def __init__(self, db_path: str = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'):
        self.db_path = db_path
        
    def get_connection(self):
        """Get DuckDB connection"""
        if not Path(self.db_path).exists():
            st.error(f"❌ DuckDB file not found: {self.db_path}")
            return None
        return duckdb.connect(self.db_path, read_only=True)
    
    @st.cache_data(ttl=300)
    def load_tariff_table(_self) -> pd.DataFrame:
        """Load only mapped tariffs (those with active provider assignments)"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            SELECT DISTINCT
                t.*,
                p.providername,
                p.providerid,
                p.bands,
                p.protariffid as provider_link_id,
                pt.tariffid as mapped_tariffid
            FROM "AI DRIVEN DATA"."TARIFF" t
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt
                ON t.tariffid = pt.tariffid
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS" p
                ON pt.protariffid = p.protariffid
            WHERE t.tariffamount > 0
                AND LOWER(p.providername) NOT LIKE '%nhis%'
            ORDER BY p.providername, t.tariffname, t.procedurecode
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            # Normalize procedure codes
            if 'procedurecode' in df.columns:
                df['procedurecode'] = df['procedurecode'].astype(str).str.strip().str.lower()
            
            return df
            
        except Exception as e:
            st.error(f"Error loading TARIFF table: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_claims_stats(_self) -> pd.DataFrame:
        """Load claims statistics for risk assessment"""
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
            
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load claims statistics: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_quality_metrics(_self) -> pd.DataFrame:
        """Load quality metrics from claims data (readmissions, complications)"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            WITH provider_claims AS (
                SELECT 
                    nhisproviderid,
                    enrollee_id,
                    datesubmitted,
                    diagnosiscode,
                    approvedamount,
                    panumber,
                    LEAD(datesubmitted) OVER (
                        PARTITION BY nhisproviderid, enrollee_id 
                        ORDER BY datesubmitted
                    ) as next_visit_date
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisproviderid IS NOT NULL
                    AND datesubmitted >= DATE '2024-01-01'
            ),
            readmissions AS (
                SELECT
                    nhisproviderid,
                    COUNT(*) as total_encounters,
                    SUM(CASE
                        WHEN next_visit_date IS NOT NULL
                        AND date_diff('day', datesubmitted, next_visit_date) <= 30
                        THEN 1 ELSE 0
                    END) as readmit_30_day
                FROM provider_claims
                GROUP BY nhisproviderid
            ),
            complications AS (
                SELECT 
                    nhisproviderid,
                    COUNT(*) as total_claims,
                    SUM(CASE 
                        WHEN diagnosiscode LIKE 'T81%' OR -- Complications of procedures
                             diagnosiscode LIKE 'T80%' OR -- Complications following infusion
                             diagnosiscode LIKE 'Y60%' OR -- Surgical mishaps
                             diagnosiscode LIKE 'Y61%' OR 
                             diagnosiscode LIKE 'Y62%'
                        THEN 1 ELSE 0 
                    END) as complication_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE datesubmitted >= DATE '2024-01-01'
                GROUP BY nhisproviderid
            )
            SELECT 
                r.nhisproviderid as providerid,
                r.total_encounters,
                r.readmit_30_day,
                ROUND(r.readmit_30_day * 100.0 / NULLIF(r.total_encounters, 0), 2) as readmit_rate_30,
                c.total_claims,
                c.complication_count,
                ROUND(c.complication_count * 100.0 / NULLIF(c.total_claims, 0), 2) as complication_rate
            FROM readmissions r
            LEFT JOIN complications c ON r.nhisproviderid = c.nhisproviderid
            WHERE r.total_encounters >= 10  -- Minimum volume for statistical validity
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load quality metrics: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def load_episode_data(_self) -> pd.DataFrame:
        """Load episode-level data for chronic conditions (diabetes, hypertension)"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            WITH chronic_conditions AS (
                SELECT 
                    nhisproviderid,
                    enrollee_id,
                    diagnosiscode,
                    approvedamount,
                    datesubmitted,
                    CASE 
                        WHEN diagnosiscode LIKE 'E10%' OR diagnosiscode LIKE 'E11%' THEN 'Diabetes'
                        WHEN diagnosiscode LIKE 'I10%' OR diagnosiscode LIKE 'I11%' THEN 'Hypertension'
                        WHEN diagnosiscode LIKE 'J45%' THEN 'Asthma'
                        WHEN diagnosiscode LIKE 'N18%' THEN 'Chronic Kidney Disease'
                        ELSE NULL
                    END as condition_category
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE datesubmitted >= DATE '2024-01-01'
                    AND (
                        diagnosiscode LIKE 'E10%' OR diagnosiscode LIKE 'E11%' OR  -- Diabetes
                        diagnosiscode LIKE 'I10%' OR diagnosiscode LIKE 'I11%' OR  -- Hypertension
                        diagnosiscode LIKE 'J45%' OR                                -- Asthma
                        diagnosiscode LIKE 'N18%'                                   -- CKD
                    )
            )
            SELECT 
                nhisproviderid as providerid,
                condition_category,
                COUNT(DISTINCT enrollee_id) as unique_patients,
                COUNT(*) as total_visits,
                SUM(approvedamount) as total_cost,
                AVG(approvedamount) as avg_cost_per_visit,
                SUM(approvedamount) / NULLIF(COUNT(DISTINCT enrollee_id), 0) as cost_per_patient
            FROM chronic_conditions
            WHERE condition_category IS NOT NULL
            GROUP BY nhisproviderid, condition_category
            HAVING COUNT(DISTINCT enrollee_id) >= 5  -- Minimum 5 patients for stability
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            return df
            
        except Exception as e:
            st.warning(f"⚠️ Could not load episode data: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def get_unique_tariffs(_self) -> List[Tuple[str, str, str]]:
        """Get list of unique tariff-provider combinations"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return []
            
            query = """
            SELECT DISTINCT
                t.tariffname,
                p.providername,
                t.tariffid,
                p.providerid
            FROM "AI DRIVEN DATA"."TARIFF" t
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt
                ON t.tariffid = pt.tariffid
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS" p
                ON pt.protariffid = p.protariffid
            WHERE t.tariffname IS NOT NULL
                AND LOWER(p.providername) NOT LIKE '%nhis%'
            ORDER BY p.providername, t.tariffname
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            return list(df.itertuples(index=False, name=None))
            
        except Exception as e:
            st.error(f"Error getting tariff names: {e}")
            return []


class HospitalBandingEngine:
    """
    Enhanced hospital banding engine with:
    - Dual reporting (weighted + unweighted)
    - Quality layer
    - Risk flags
    - Episode-level analysis
    """
    
    def __init__(self, standard_tariff_df: pd.DataFrame, claims_stats: pd.DataFrame = None, 
                 quality_metrics: pd.DataFrame = None, episode_data: pd.DataFrame = None):
        """
        Initialize the banding engine
        
        Parameters:
        -----------
        standard_tariff_df : pd.DataFrame
            Standard tariff with band columns
        claims_stats : pd.DataFrame, optional
            Claims statistics for risk assessment
        quality_metrics : pd.DataFrame, optional
            Quality metrics (readmissions, complications)
        episode_data : pd.DataFrame, optional
            Episode-level chronic condition data
        """
        self.standard_df = standard_tariff_df.copy()
        self.claims_stats = claims_stats
        self.quality_metrics = quality_metrics
        self.episode_data = episode_data
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
        """Prepare standard tariff for analysis"""
        try:
            # Normalize codes
            self.standard_df['procedurecode'] = self.standard_df['procedurecode'].apply(self._normalize_code)
            
            # Ensure numeric types for band columns
            band_cols = ['band_a', 'band_b', 'band_c', 'band_d', 'band_special']
            for col in band_cols:
                if col not in self.standard_df.columns:
                    st.error(f"❌ Missing required column: {col}")
                    return
                self.standard_df[col] = pd.to_numeric(self.standard_df[col], errors='coerce')
            
            # Set frequency
            if 'frequency' not in self.standard_df.columns:
                self.standard_df['frequency'] = 1
            self.standard_df['effective_frequency'] = self.standard_df['frequency']
            
            # Update frequencies from claims if available
            if self.claims_stats is not None and not self.claims_stats.empty:
                claims_dict = self.claims_stats.set_index('procedurecode')['count'].to_dict()
                for idx, row in self.standard_df.iterrows():
                    proc_code = row['procedurecode']
                    if proc_code in claims_dict:
                        self.standard_df.at[idx, 'effective_frequency'] = claims_dict[proc_code]
            
            # Create lookup dictionary
            self.std_dict = {
                row['procedurecode']: row 
                for _, row in self.standard_df.iterrows()
            }
            
        except Exception as e:
            st.error(f"Error preparing standard tariff: {e}")
    
    def _calculate_thresholds(self):
        """Calculate band thresholds using simple mean"""
        self.thresholds = {
            'D': float(self.standard_df['band_d'].mean()),
            'C': float(self.standard_df['band_c'].mean()),
            'B': float(self.standard_df['band_b'].mean()),
            'A': float(self.standard_df['band_a'].mean()),
            'Special': float(self.standard_df['band_special'].mean())
        }
    
    def _determine_procedure_band(self, price: float, std_proc: Dict) -> str:
        """Determine which band a price falls into"""
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
    
    def _assign_band_from_weighted_avg(self, weighted_avg: float) -> str:
        """Assign band based on weighted average"""
        if weighted_avg <= self.thresholds['D']:
            return 'D'
        elif weighted_avg <= self.thresholds['C']:
            return 'C'
        elif weighted_avg <= self.thresholds['B']:
            return 'B'
        elif weighted_avg <= self.thresholds['A']:
            return 'A'
        elif weighted_avg <= self.thresholds['Special']:
            return 'Special'
        else:
            return 'check'
    
    def _check_quality_flags(self, providerid: str) -> Dict:
        """Check quality metrics for this provider"""
        quality_flags = {
            'has_quality_issues': False,
            'readmit_rate': None,
            'complication_rate': None,
            'quality_alert': None
        }
        
        if self.quality_metrics is None or self.quality_metrics.empty:
            return quality_flags
        
        provider_quality = self.quality_metrics[
            self.quality_metrics['providerid'] == providerid
        ]
        
        if provider_quality.empty:
            return quality_flags
        
        readmit_rate = provider_quality['readmit_rate_30'].iloc[0]
        complication_rate = provider_quality['complication_rate'].iloc[0]
        
        quality_flags['readmit_rate'] = readmit_rate
        quality_flags['complication_rate'] = complication_rate
        
        # Flag if rates exceed industry benchmarks
        # Industry benchmarks: 30-day readmit ~15%, complications ~2%
        alerts = []
        if pd.notna(readmit_rate) and readmit_rate > 20:  # >20% is concerning
            alerts.append(f"High readmission rate: {readmit_rate:.1f}%")
            quality_flags['has_quality_issues'] = True
        
        if pd.notna(complication_rate) and complication_rate > 3:  # >3% is concerning
            alerts.append(f"High complication rate: {complication_rate:.1f}%")
            quality_flags['has_quality_issues'] = True
        
        if alerts:
            quality_flags['quality_alert'] = " | ".join(alerts)
        
        return quality_flags
    
    def analyze_tariff(self, tariff_df: pd.DataFrame, tariff_name: str, 
                      providerid: str = None) -> Dict:
        """
        Analyze a tariff using DUAL methodology (weighted + unweighted)
        
        Parameters:
        -----------
        tariff_df : pd.DataFrame
            Tariff data with procedurecode and tariffamount columns
        tariff_name : str
            Name of the tariff
        providerid : str, optional
            Provider ID for quality checks
        
        Returns:
        --------
        Dict with comprehensive analysis results including:
        - Weighted band (TCOC - Total Cost of Care)
        - Unweighted band (Unit Price Negotiation)
        - Quality flags
        - Risk flags (P90/P95)
        """
        try:
            # Prepare tariff data
            tariff_df = tariff_df.copy()
            tariff_df['procedurecode'] = tariff_df['procedurecode'].apply(self._normalize_code)
            tariff_df['tariffamount'] = pd.to_numeric(tariff_df['tariffamount'], errors='coerce')
            tariff_df = tariff_df.dropna(subset=['tariffamount'])
            tariff_df = tariff_df[tariff_df['tariffamount'] > 0]
            
            if len(tariff_df) == 0:
                return {
                    'success': False,
                    'error': 'No valid procedures found'
                }
            
            # Check quality metrics first
            quality_flags = self._check_quality_flags(providerid) if providerid else {}
            
            # Get claims stats dict if available
            claims_dict = {}
            if self.claims_stats is not None and not self.claims_stats.empty:
                claims_dict = {
                    row['procedurecode']: row
                    for _, row in self.claims_stats.iterrows()
                }
            
            # Initialize accumulators for WEIGHTED analysis (TCOC)
            weighted_total_price = 0.0
            weighted_total_log_freq = 0.0
            weighted_band_freq = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
            
            # Initialize accumulators for UNWEIGHTED analysis (Unit Price)
            unweighted_total_price = 0.0
            unweighted_count = 0
            unweighted_band_freq = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
            
            # Risk tracking
            procedure_details = []
            matched = 0
            above_p90_count = 0
            above_p90_weight = 0.0
            above_p95_count = 0
            fraud_risk_procedures = []
            
            # Process each procedure
            for _, row in tariff_df.iterrows():
                proc_code = row['procedurecode']
                price = row['tariffamount']
                
                if proc_code not in self.std_dict:
                    continue
                
                matched += 1
                std_proc = self.std_dict[proc_code]
                freq = float(std_proc.get('effective_frequency', 1))
                
                # WEIGHTED ANALYSIS (TCOC)
                log_freq = np.log1p(freq)
                weighted_price = price * log_freq
                weighted_total_price += weighted_price
                weighted_total_log_freq += log_freq
                
                # UNWEIGHTED ANALYSIS (Unit Price)
                unweighted_total_price += price
                unweighted_count += 1
                
                # Determine band
                band = self._determine_procedure_band(price, std_proc)
                
                # Risk flagging with claims data
                flag_above_p90 = False
                flag_above_p95 = False
                claims_p90 = None
                claims_p95 = None
                fraud_risk_score = 0
                
                if proc_code in claims_dict:
                    claims_data = claims_dict[proc_code]
                    claims_p90 = claims_data.get('p90')
                    claims_p95 = claims_data.get('p95')
                    claims_p50 = claims_data.get('p50')
                    
                    # P90 flag
                    if pd.notna(claims_p90) and price > claims_p90:
                        flag_above_p90 = True
                        above_p90_count += 1
                        above_p90_weight += log_freq
                    
                    # P95 flag (fraud risk)
                    if pd.notna(claims_p95) and price > claims_p95:
                        flag_above_p95 = True
                        above_p95_count += 1
                        
                        # Calculate fraud risk score
                        if pd.notna(claims_p50) and claims_p50 > 0:
                            price_ratio = price / claims_p50
                            if price_ratio > 3:  # >300% of median
                                fraud_risk_score = min(100, int((price_ratio - 3) * 20))
                                fraud_risk_procedures.append({
                                    'procedurecode': proc_code,
                                    'proceduredesc': std_proc.get('proceduredesc', 'N/A'),
                                    'price': price,
                                    'market_median': claims_p50,
                                    'price_ratio': price_ratio,
                                    'fraud_risk_score': fraud_risk_score
                                })
                    
                    # Auto-flag to 'check' if extreme outlier
                    if pd.notna(claims_p95) and price > max(claims_p95, std_proc['band_a'] * 1.25):
                        band = "check"
                elif price > std_proc['band_a'] * 1.25:
                    band = "check"
                
                # Update distributions
                weighted_band_freq[band] += log_freq
                unweighted_band_freq[band] += 1
                
                # Store details
                procedure_details.append({
                    'procedurecode': proc_code,
                    'proceduredesc': std_proc.get('proceduredesc', 'N/A'),
                    'price': price,
                    'band': band,
                    'band_a': std_proc['band_a'],
                    'band_b': std_proc['band_b'],
                    'band_c': std_proc['band_c'],
                    'band_d': std_proc['band_d'],
                    'frequency': log_freq,
                    'flag_above_p90': flag_above_p90,
                    'flag_above_p95': flag_above_p95,
                    'claims_p90': claims_p90,
                    'claims_p95': claims_p95,
                    'fraud_risk_score': fraud_risk_score
                })
            
            if weighted_total_log_freq == 0 or unweighted_count == 0:
                return {
                    'success': False,
                    'error': f'No matching procedures found. Hospital has {len(tariff_df)} procedures.'
                }
            
            # === WEIGHTED ANALYSIS (TCOC - Primary Metric) ===
            weighted_avg = weighted_total_price / weighted_total_log_freq
            weighted_calculated_band = self._assign_band_from_weighted_avg(weighted_avg)
            
            # Find dominant band (weighted)
            weighted_total_freq = sum(weighted_band_freq.values())
            weighted_band_pcts = {
                band: (freq / weighted_total_freq * 100) if weighted_total_freq > 0 else 0 
                for band, freq in weighted_band_freq.items()
            }
            
            weighted_valid_bands = {b: p for b, p in weighted_band_pcts.items() if b != 'check'}
            weighted_dominant_band = max(weighted_valid_bands, key=weighted_valid_bands.get) if weighted_valid_bands else weighted_calculated_band
            weighted_dominant_pct = weighted_valid_bands.get(weighted_dominant_band, 0)
            
            # Apply dominant band logic
            if weighted_dominant_pct >= 60:
                weighted_final_band = weighted_dominant_band
                weighted_method = "Dominant Band"
            else:
                weighted_final_band = weighted_calculated_band
                weighted_method = "Weighted Average"
            
            # Gaming protection
            if weighted_band_pcts.get('check', 0) > 30:
                weighted_final_band = weighted_calculated_band
                weighted_method = "Gaming Protection"
            
            # === UNWEIGHTED ANALYSIS (Unit Price - Secondary Metric) ===
            unweighted_avg = unweighted_total_price / unweighted_count
            unweighted_calculated_band = self._assign_band_from_weighted_avg(unweighted_avg)
            
            # Find dominant band (unweighted)
            unweighted_band_pcts = {
                band: (count / unweighted_count * 100) if unweighted_count > 0 else 0 
                for band, count in unweighted_band_freq.items()
            }
            
            unweighted_valid_bands = {b: p for b, p in unweighted_band_pcts.items() if b != 'check'}
            unweighted_dominant_band = max(unweighted_valid_bands, key=unweighted_valid_bands.get) if unweighted_valid_bands else unweighted_calculated_band
            unweighted_dominant_pct = unweighted_valid_bands.get(unweighted_dominant_band, 0)
            
            # Apply dominant band logic (unweighted)
            if unweighted_dominant_pct >= 60:
                unweighted_final_band = unweighted_dominant_band
                unweighted_method = "Dominant Band"
            else:
                unweighted_final_band = unweighted_calculated_band
                unweighted_method = "Simple Average"
            
            # Gaming protection (unweighted)
            if unweighted_band_pcts.get('check', 0) > 30:
                unweighted_final_band = unweighted_calculated_band
                unweighted_method = "Gaming Protection"
            
            # === CONFIDENCE SCORES ===
            volume_conf = min(1.0, np.log1p(weighted_total_log_freq) / np.log1p(10000))
            coverage_conf = min(1.0, matched / len(self.std_dict))
            confidence = 0.60 * volume_conf + 0.40 * coverage_conf
            
            coverage_pct = (matched / len(self.std_dict) * 100) if len(self.std_dict) > 0 else 0
            
            # === RISK METRICS ===
            pct_above_p90 = (above_p90_weight / weighted_total_log_freq * 100) if weighted_total_log_freq > 0 else 0
            pct_above_p95 = (above_p95_count / matched * 100) if matched > 0 else 0
            
            # Sort fraud risks by score
            fraud_risk_procedures.sort(key=lambda x: x['fraud_risk_score'], reverse=True)
            
            return {
                'success': True,
                'tariff_name': tariff_name,
                
                # WEIGHTED (TCOC - Primary)
                'weighted_avg': weighted_avg,
                'weighted_final_band': weighted_final_band,
                'weighted_calculated_band': weighted_calculated_band,
                'weighted_dominant_band': weighted_dominant_band,
                'weighted_dominant_pct': weighted_dominant_pct,
                'weighted_method': weighted_method,
                'weighted_band_pcts': weighted_band_pcts,
                
                # UNWEIGHTED (Unit Price - Secondary)
                'unweighted_avg': unweighted_avg,
                'unweighted_final_band': unweighted_final_band,
                'unweighted_calculated_band': unweighted_calculated_band,
                'unweighted_dominant_band': unweighted_dominant_band,
                'unweighted_dominant_pct': unweighted_dominant_pct,
                'unweighted_method': unweighted_method,
                'unweighted_band_pcts': unweighted_band_pcts,
                
                # Common metrics
                'confidence': confidence,
                'coverage_pct': coverage_pct,
                'matched_procedures': matched,
                'total_procedures': len(tariff_df),
                'procedure_details': procedure_details,
                
                # Risk metrics
                'above_p90_count': above_p90_count,
                'pct_above_p90': pct_above_p90,
                'above_p95_count': above_p95_count,
                'pct_above_p95': pct_above_p95,
                'fraud_risk_procedures': fraud_risk_procedures[:10],  # Top 10
                'has_claims_data': bool(claims_dict),
                
                # Quality metrics
                **quality_flags
            }
            
        except Exception as e:
            import traceback
            return {
                'success': False,
                'error': f"{str(e)}\n{traceback.format_exc()}"
            }


def create_standard_tariff() -> pd.DataFrame:
    """Create standard tariff from REALITY_TARIFF.xlsx"""
    try:
        df = pd.read_excel('/Users/kenechukwuchukwuka/Downloads/REALITY TARIFF.xlsx')
        
        required_cols = ['procedurecode', 'band_a', 'band_b', 'band_c', 'band_d', 'band_special']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            st.error(f"❌ Missing columns in REALITY_TARIFF.xlsx: {missing_cols}")
            return pd.DataFrame()
        
        return df
        
    except Exception as e:
        st.error(f"❌ Error loading REALITY_TARIFF.xlsx: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=600)
def analyze_all_tariffs(_loader: DuckDBTariffLoader, _engine: HospitalBandingEngine):
    """Analyze all tariffs and return results dataframe (cached to prevent recomputation)"""

    # Get unique tariff-provider combinations
    tariff_provider_combos = _loader.get_unique_tariffs()

    if not tariff_provider_combos:
        return pd.DataFrame()

    # Load complete tariff table
    tariff_df = _loader.load_tariff_table()

    if tariff_df.empty:
        return pd.DataFrame()

    # Analyze each tariff-provider combination
    results = []
    for i, (tariffname, providername, tariffid, providerid) in enumerate(tariff_provider_combos):
        # Get tariff data
        tariff_data = tariff_df[
            (tariff_df['tariffname'] == tariffname) &
            (tariff_df['providername'] == providername)
        ].copy()

        if tariff_data.empty:
            continue

        # Analyze with provider ID for quality checks
        result = _engine.analyze_tariff(tariff_data, f"{providername} - {tariffname}", providerid=providerid)

        if result['success']:
            provider_assigned_band = tariff_data['bands'].iloc[0] if 'bands' in tariff_data.columns else "N/A"

            results.append({
                'Provider Name': providername,
                'Tariff Name': tariffname,
                'Provider ID': providerid,
                'Provider Band': provider_assigned_band,

                # PRIMARY: Weighted (TCOC)
                'Weighted Band': result['weighted_final_band'],
                'Weighted Avg': f"₦{result['weighted_avg']:,.0f}",
                'Weighted Method': result['weighted_method'],

                # SECONDARY: Unweighted (Unit Price)
                'Unweighted Band': result['unweighted_final_band'],
                'Unweighted Avg': f"₦{result['unweighted_avg']:,.0f}",
                'Unweighted Method': result['unweighted_method'],

                # Band Comparison
                'Bands Match': '✅' if result['weighted_final_band'] == result['unweighted_final_band'] else '⚠️',

                # Quality Flags
                'Quality Alert': '🚨' if result.get('has_quality_issues') else '✅',
                'Readmit Rate': f"{result.get('readmit_rate', 0):.1f}%" if pd.notna(result.get('readmit_rate')) else "—",
                'Complication Rate': f"{result.get('complication_rate', 0):.1f}%" if pd.notna(result.get('complication_rate')) else "—",

                # Risk Metrics
                'Above P90': f"{result['pct_above_p90']:.0f}%" if result['has_claims_data'] else "—",
                'Above P95': f"{result['pct_above_p95']:.0f}%" if result['has_claims_data'] else "—",
                'Fraud Risk': '🚨' if len(result.get('fraud_risk_procedures', [])) > 0 else '✅',

                # Other metrics
                'Confidence': f"{result['confidence']*100:.0f}%",
                'Coverage': f"{result['coverage_pct']:.1f}%",
                'Matched Procedures': result['matched_procedures'],

                # Store full result for drill-down
                '_full_result': result
            })

    return pd.DataFrame(results)


def display_all_tariffs_with_bands(loader: DuckDBTariffLoader, engine: HospitalBandingEngine):
    """Display all mapped tariffs with DUAL REPORTING"""

    st.markdown("### 🏥 Mapped Hospital Tariffs - Dual Band Analysis")
    st.info("ℹ️ **Dual Reporting**: Weighted Band (TCOC Impact) + Unweighted Band (Unit Price Negotiation)")

    # Analyze all tariffs (cached - won't recompute on filter changes)
    with st.spinner("Analyzing all tariffs... (this will be cached for future visits)"):
        results_df = analyze_all_tariffs(loader, engine)

    if results_df.empty:
        st.warning("⚠️ No mapped tariffs found or analysis failed")
        return

    st.success(f"✅ Analyzed {len(results_df)} tariff-provider combinations")

    # === FILTERS (moved BEFORE summary to prevent recomputation) ===
    st.markdown("### 🔍 Filter & Search")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        provider_filter = st.multiselect(
            "Filter by Provider",
            options=sorted(results_df['Provider Name'].unique()),
            default=[]
        )

    with col2:
        weighted_band_filter = st.multiselect(
            "Filter by Weighted Band",
            options=['A', 'B', 'C', 'D', 'Special', 'check'],
            default=[]
        )

    with col3:
        quality_filter = st.selectbox(
            "Quality Filter",
            options=["All", "🚨 Quality Issues Only", "✅ No Issues"],
            index=0
        )

    with col4:
        fraud_filter = st.selectbox(
            "Fraud Risk Filter",
            options=["All", "🚨 Fraud Risk Only", "✅ No Risk"],
            index=0
        )

    # Apply filters
    filtered_df = results_df.copy()

    if provider_filter:
        filtered_df = filtered_df[filtered_df['Provider Name'].isin(provider_filter)]

    if weighted_band_filter:
        filtered_df = filtered_df[filtered_df['Weighted Band'].isin(weighted_band_filter)]

    if quality_filter == "🚨 Quality Issues Only":
        filtered_df = filtered_df[filtered_df['Quality Alert'] == '🚨']
    elif quality_filter == "✅ No Issues":
        filtered_df = filtered_df[filtered_df['Quality Alert'] == '✅']

    if fraud_filter == "🚨 Fraud Risk Only":
        filtered_df = filtered_df[filtered_df['Fraud Risk'] == '🚨']
    elif fraud_filter == "✅ No Risk":
        filtered_df = filtered_df[filtered_df['Fraud Risk'] == '✅']

    st.markdown("---")

    # === SUMMARY METRICS (based on filtered data) ===
    st.markdown("### 📊 Executive Summary")
    st.caption(f"Showing {len(filtered_df)} of {len(results_df)} tariff-provider combinations")

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("Filtered Tariffs", len(filtered_df))

    with col2:
        unique_providers = filtered_df['Provider Name'].nunique()
        st.metric("Unique Providers", unique_providers)

    with col3:
        # Dual band agreement
        dual_match = len(filtered_df[filtered_df['Bands Match'] == '✅'])
        dual_match_pct = (dual_match / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric("Weighted/Unweighted Match", f"{dual_match} ({dual_match_pct:.0f}%)")

    with col4:
        quality_issues = len(filtered_df[filtered_df['Quality Alert'] == '🚨'])
        st.metric("Quality Alerts", quality_issues, delta=None if quality_issues == 0 else "⚠️")

    with col5:
        high_risk = len(filtered_df[
            filtered_df['Above P90'].str.replace('%', '').str.replace('—', '0').astype(float) > 40
        ])
        st.metric("High P90 Risk (>40%)", high_risk)

    with col6:
        fraud_alerts = len(filtered_df[filtered_df['Fraud Risk'] == '🚨'])
        st.metric("Fraud Risk Alerts", fraud_alerts, delta=None if fraud_alerts == 0 else "⚠️")
    
    st.markdown("---")
    
    # === QUALITY ALERTS (using filtered data) ===
    quality_issues_df = filtered_df[filtered_df['Quality Alert'] == '🚨']
    if not quality_issues_df.empty:
        st.markdown("### 🚨 QUALITY ALERTS")
        st.markdown(
            '<div class="quality-alert">⚠️ The following providers have quality concerns (high readmissions or complications)</div>',
            unsafe_allow_html=True
        )
        
        quality_display = quality_issues_df[[
            'Provider Name', 'Tariff Name', 'Weighted Band', 
            'Readmit Rate', 'Complication Rate'
        ]].copy()
        
        st.dataframe(quality_display, use_container_width=True)
        st.markdown("---")
    
    # === FRAUD RISK ALERTS (using filtered data) ===
    fraud_df = filtered_df[filtered_df['Fraud Risk'] == '🚨']
    if not fraud_df.empty:
        st.markdown("### 🔍 FRAUD RISK ALERTS")
        st.markdown(
            '<div class="fraud-alert">⚠️ The following providers have procedures >300% of market median</div>',
            unsafe_allow_html=True
        )
        
        # Show top fraud risks
        for _, row in fraud_df.head(5).iterrows():
            with st.expander(f"🚨 {row['Provider Name']} - {row['Tariff Name']}"):
                fraud_procs = row['_full_result'].get('fraud_risk_procedures', [])
                if fraud_procs:
                    fraud_df_display = pd.DataFrame(fraud_procs)
                    fraud_df_display['price'] = fraud_df_display['price'].apply(lambda x: f"₦{x:,.0f}")
                    fraud_df_display['market_median'] = fraud_df_display['market_median'].apply(lambda x: f"₦{x:,.0f}")
                    fraud_df_display['price_ratio'] = fraud_df_display['price_ratio'].apply(lambda x: f"{x:.1f}x")
                    
                    st.dataframe(fraud_df_display[[
                        'proceduredesc', 'price', 'market_median', 'price_ratio', 'fraud_risk_score'
                    ]], use_container_width=True)
        
        st.markdown("---")

    # === MAIN RESULTS TABLE ===
    st.markdown("### 📋 Detailed Results (Dual Reporting)")
    
    # Drop internal columns for display
    display_df = filtered_df.drop(columns=['_full_result'])
    
    # Color coding
    def highlight_bands(row):
        styles = [''] * len(row)
        
        # Quality alert
        if row['Quality Alert'] == '🚨':
            styles = ['background-color: #ffe6e6'] * len(row)
        
        # Fraud alert (override quality if both)
        if row['Fraud Risk'] == '🚨':
            styles = ['background-color: #fff4e6'] * len(row)
        
        return styles
    
    st.dataframe(
        display_df.style.apply(highlight_bands, axis=1),
        use_container_width=True,
        height=600
    )
    
    # Download button
    csv = display_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Results as CSV",
        data=csv,
        file_name=f"dual_band_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )
    
    # === VISUALIZATIONS ===
    st.markdown("---")
    st.markdown("### 📊 Band Distribution Analysis")
    
    tab1, tab2, tab3 = st.tabs(["Weighted vs Unweighted", "Quality Metrics", "Risk Analysis"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Weighted band distribution (using filtered data)
            weighted_dist = filtered_df['Weighted Band'].value_counts().reset_index()
            weighted_dist.columns = ['Band', 'Count']

            fig = px.bar(
                weighted_dist,
                x='Band',
                y='Count',
                color='Band',
                color_discrete_map={
                    'A': '#90EE90', 'B': '#87CEEB', 'C': '#FFD700',
                    'D': '#FFA500', 'Special': '#DDA0DD', 'check': '#FF6B6B'
                },
                title="Weighted Band Distribution (TCOC Impact)"
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Unweighted band distribution (using filtered data)
            unweighted_dist = filtered_df['Unweighted Band'].value_counts().reset_index()
            unweighted_dist.columns = ['Band', 'Count']

            fig2 = px.bar(
                unweighted_dist,
                x='Band',
                y='Count',
                color='Band',
                color_discrete_map={
                    'A': '#90EE90', 'B': '#87CEEB', 'C': '#FFD700',
                    'D': '#FFA500', 'Special': '#DDA0DD', 'check': '#FF6B6B'
                },
                title="Unweighted Band Distribution (Unit Price)"
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        if not quality_issues_df.empty:
            # Quality metrics scatter (using filtered data)
            quality_plot_df = filtered_df.copy()
            quality_plot_df['Readmit'] = pd.to_numeric(
                quality_plot_df['Readmit Rate'].str.replace('%', '').str.replace('—', 'nan'),
                errors='coerce'
            )
            quality_plot_df['Complication'] = pd.to_numeric(
                quality_plot_df['Complication Rate'].str.replace('%', '').str.replace('—', 'nan'),
                errors='coerce'
            )
            quality_plot_df = quality_plot_df.dropna(subset=['Readmit', 'Complication'])
            
            if not quality_plot_df.empty:
                fig_quality = px.scatter(
                    quality_plot_df,
                    x='Readmit',
                    y='Complication',
                    color='Weighted Band',
                    size='Matched Procedures',
                    hover_data=['Provider Name', 'Tariff Name'],
                    title="Quality Metrics: Readmission vs Complication Rates",
                    labels={'Readmit': '30-Day Readmission Rate (%)', 'Complication': 'Complication Rate (%)'}
                )
                
                # Add benchmark lines
                fig_quality.add_hline(y=3, line_dash="dash", line_color="red", annotation_text="3% Complication Benchmark")
                fig_quality.add_vline(x=20, line_dash="dash", line_color="red", annotation_text="20% Readmit Benchmark")
                
                st.plotly_chart(fig_quality, use_container_width=True)
        else:
            st.info("✅ No quality issues detected across providers")
    
    with tab3:
        # Risk scatter plot (using filtered data)
        risk_plot_df = filtered_df.copy()
        risk_plot_df['P90_pct'] = pd.to_numeric(
            risk_plot_df['Above P90'].str.replace('%', '').str.replace('—', '0'),
            errors='coerce'
        )
        risk_plot_df['P95_pct'] = pd.to_numeric(
            risk_plot_df['Above P95'].str.replace('%', '').str.replace('—', '0'),
            errors='coerce'
        )
        
        fig_risk = px.scatter(
            risk_plot_df,
            x='P90_pct',
            y='P95_pct',
            color='Weighted Band',
            size='Matched Procedures',
            hover_data=['Provider Name', 'Tariff Name'],
            title="Risk Analysis: P90 vs P95 Violations",
            labels={'P90_pct': '% Procedures Above P90', 'P95_pct': '% Procedures Above P95'}
        )
        
        # Add risk threshold lines
        fig_risk.add_hline(y=10, line_dash="dash", line_color="orange", annotation_text="10% P95 Alert")
        fig_risk.add_vline(x=40, line_dash="dash", line_color="red", annotation_text="40% P90 High Risk")
        
        st.plotly_chart(fig_risk, use_container_width=True)


def display_single_tariff_analysis(result: Dict):
    """Display analysis for a single uploaded tariff with dual reporting"""
    
    if not result['success']:
        st.error(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")
        return
    
    st.markdown(f"## Analysis Results: {result['tariff_name']}")
    
    # === QUALITY ALERTS ===
    if result.get('has_quality_issues'):
        st.markdown(
            f'<div class="quality-alert">🚨 QUALITY ALERT: {result.get("quality_alert", "Quality issues detected")}</div>',
            unsafe_allow_html=True
        )
    
    # === FRAUD ALERTS ===
    if len(result.get('fraud_risk_procedures', [])) > 0:
        st.markdown(
            f'<div class="fraud-alert">🚨 FRAUD RISK: {len(result["fraud_risk_procedures"])} procedures exceed 300% of market median</div>',
            unsafe_allow_html=True
        )
    
    st.markdown("---")
    
    # === DUAL REPORTING METRICS ===
    st.markdown("### 📊 Dual Band Analysis")
    st.info("**Primary Metric**: Weighted Band (Total Cost of Care Impact) | **Secondary Metric**: Unweighted Band (Unit Price Negotiation)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("#### PRIMARY: Weighted (TCOC)")
        st.metric("Weighted Band", result['weighted_final_band'])
        st.metric("Weighted Average", f"₦{result['weighted_avg']:,.0f}")
        st.caption(f"Method: {result['weighted_method']}")
    
    with col2:
        st.markdown("#### SECONDARY: Unweighted")
        st.metric("Unweighted Band", result['unweighted_final_band'])
        st.metric("Simple Average", f"₦{result['unweighted_avg']:,.0f}")
        st.caption(f"Method: {result['unweighted_method']}")
    
    with col3:
        st.markdown("#### Quality Metrics")
        if result.get('has_quality_issues'):
            st.metric("Quality Status", "🚨 Alert")
            if pd.notna(result.get('readmit_rate')):
                st.metric("30-Day Readmit", f"{result['readmit_rate']:.1f}%")
            if pd.notna(result.get('complication_rate')):
                st.metric("Complications", f"{result['complication_rate']:.1f}%")
        else:
            st.metric("Quality Status", "✅ Good")
    
    with col4:
        st.markdown("#### Risk Metrics")
        st.metric("Above P90", f"{result['pct_above_p90']:.0f}%")
        st.metric("Above P95", f"{result['pct_above_p95']:.0f}%")
        if len(result.get('fraud_risk_procedures', [])) > 0:
            st.metric("Fraud Risks", f"{len(result['fraud_risk_procedures'])}")
    
    st.markdown("---")
    
    # === BAND COMPARISON ===
    st.markdown("### ⚖️ Band Comparison & Recommendation")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Comparison table
        comparison_df = pd.DataFrame({
            'Metric': ['Band Assignment', 'Average Price', 'Dominant Band', 'Dominant %', 'Assignment Method'],
            'Weighted (TCOC)': [
                result['weighted_final_band'],
                f"₦{result['weighted_avg']:,.0f}",
                result['weighted_dominant_band'],
                f"{result['weighted_dominant_pct']:.1f}%",
                result['weighted_method']
            ],
            'Unweighted (Unit Price)': [
                result['unweighted_final_band'],
                f"₦{result['unweighted_avg']:,.0f}",
                result['unweighted_dominant_band'],
                f"{result['unweighted_dominant_pct']:.1f}%",
                result['unweighted_method']
            ]
        })
        
        st.dataframe(comparison_df, use_container_width=True, hide_index=True)
    
    with col2:
        # Recommendation
        st.markdown("#### 💡 Recommendations")
        
        if result['weighted_final_band'] == result['unweighted_final_band']:
            st.success(f"✅ **Consistent Pricing**: Both methodologies agree on Band {result['weighted_final_band']}")
            st.info(f"Use **Band {result['weighted_final_band']}** for both network steerage and contract negotiations")
        else:
            st.warning(f"⚠️ **Split Assessment**: Weighted={result['weighted_final_band']}, Unweighted={result['unweighted_final_band']}")
            st.info(
                f"**Network Steerage**: Use **{result['weighted_final_band']}** (focuses on high-frequency procedures)\n\n"
                f"**Contract Negotiation**: Target **{result['unweighted_final_band']}** procedures (high unit prices)"
            )
        
        if result.get('has_quality_issues'):
            st.error("🚨 **Quality Override**: Consider downgrading band or excluding from preferred network regardless of cost")
        
        if len(result.get('fraud_risk_procedures', [])) > 0:
            st.error("🚨 **Fraud Risk**: Require detailed audit before contract renewal")
    
    st.markdown("---")
    
    # === BAND DISTRIBUTION CHARTS ===
    st.markdown("### 📊 Band Distribution Comparison")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Weighted band distribution
        weighted_data = pd.DataFrame({
            'Band': list(result['weighted_band_pcts'].keys()),
            'Percentage': list(result['weighted_band_pcts'].values())
        })
        
        fig1 = px.pie(
            weighted_data,
            values='Percentage',
            names='Band',
            title="Weighted Band Distribution (TCOC)",
            color='Band',
            color_discrete_map={
                'A': '#90EE90', 'B': '#87CEEB', 'C': '#FFD700',
                'D': '#FFA500', 'Special': '#DDA0DD', 'check': '#FF6B6B'
            }
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Unweighted band distribution
        unweighted_data = pd.DataFrame({
            'Band': list(result['unweighted_band_pcts'].keys()),
            'Percentage': list(result['unweighted_band_pcts'].values())
        })
        
        fig2 = px.pie(
            unweighted_data,
            values='Percentage',
            names='Band',
            title="Unweighted Band Distribution (Unit Price)",
            color='Band',
            color_discrete_map={
                'A': '#90EE90', 'B': '#87CEEB', 'C': '#FFD700',
                'D': '#FFA500', 'Special': '#DDA0DD', 'check': '#FF6B6B'
            }
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    st.markdown("---")
    
    # === FRAUD RISK DETAILS ===
    if len(result.get('fraud_risk_procedures', [])) > 0:
        st.markdown("### 🔍 Fraud Risk Analysis")
        
        fraud_df = pd.DataFrame(result['fraud_risk_procedures'])
        fraud_df['price'] = fraud_df['price'].apply(lambda x: f"₦{x:,.0f}")
        fraud_df['market_median'] = fraud_df['market_median'].apply(lambda x: f"₦{x:,.0f}")
        fraud_df['price_ratio'] = fraud_df['price_ratio'].apply(lambda x: f"{x:.1f}x")
        
        st.dataframe(
            fraud_df[[
                'proceduredesc', 'price', 'market_median', 'price_ratio', 'fraud_risk_score'
            ]].rename(columns={
                'proceduredesc': 'Procedure',
                'price': 'Hospital Price',
                'market_median': 'Market Median',
                'price_ratio': 'Price Ratio',
                'fraud_risk_score': 'Risk Score (0-100)'
            }),
            use_container_width=True
        )
    
    st.markdown("---")
    
    # === PROCEDURE DETAILS TABLE ===
    st.markdown("### 📋 Detailed Procedure Analysis")
    
    if result['procedure_details']:
        proc_df = pd.DataFrame(result['procedure_details'])
        proc_df = proc_df.sort_values('price', ascending=False)
        
        # Format for display
        display_df = proc_df.copy()
        display_df['Price'] = display_df['price'].apply(lambda x: f"₦{x:,.0f}")
        display_df['Band A'] = display_df['band_a'].apply(lambda x: f"₦{x:,.0f}")
        display_df['Band B'] = display_df['band_b'].apply(lambda x: f"₦{x:,.0f}")
        display_df['Band C'] = display_df['band_c'].apply(lambda x: f"₦{x:,.0f}")
        display_df['Band D'] = display_df['band_d'].apply(lambda x: f"₦{x:,.0f}")
        display_df['P90 Flag'] = display_df['flag_above_p90'].apply(lambda x: '🚨' if x else '')
        display_df['P95 Flag'] = display_df['flag_above_p95'].apply(lambda x: '🚨' if x else '')
        display_df['Fraud Risk'] = display_df['fraud_risk_score'].apply(
            lambda x: f"{x}" if x > 0 else ""
        )
        
        if result['has_claims_data']:
            display_df['Claims P90'] = display_df['claims_p90'].apply(
                lambda x: f"₦{x:,.0f}" if pd.notna(x) else "—"
            )
            display_df['Claims P95'] = display_df['claims_p95'].apply(
                lambda x: f"₦{x:,.0f}" if pd.notna(x) else "—"
            )
            
            export_cols = [
                'procedurecode', 'proceduredesc', 'Price', 'band', 
                'Band A', 'Band B', 'Band C', 'Band D',
                'P90 Flag', 'P95 Flag', 'Fraud Risk', 
                'Claims P90', 'Claims P95'
            ]
        else:
            export_cols = [
                'procedurecode', 'proceduredesc', 'Price', 'band',
                'Band A', 'Band B', 'Band C', 'Band D'
            ]
        
        display_df = display_df[export_cols]
        display_df.columns = [col.replace('_', ' ').title() for col in export_cols]
        
        # Color coding
        def highlight_risks(row):
            styles = [''] * len(row)
            
            if '🚨' in str(row.get('P95 Flag', '')):
                styles = ['background-color: #ffe6e6'] * len(row)
            elif '🚨' in str(row.get('P90 Flag', '')):
                styles = ['background-color: #fff4e6'] * len(row)
            
            return styles
        
        st.dataframe(
            display_df.style.apply(highlight_risks, axis=1),
            use_container_width=True,
            height=500
        )
        
        # Download button
        csv = display_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Procedure Details as CSV",
            data=csv,
            file_name=f"{result['tariff_name']}_procedures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )


def main():
    """Main Streamlit application with enhanced features"""

    try:
        # Header
        st.markdown('<h1 class="main-header">🏥 Hospital Band Analysis Dashboard (Enhanced)</h1>', unsafe_allow_html=True)
        st.markdown(
            "**New Features**: Quality Layer | Fraud Detection | Dual Reporting (TCOC + Unit Price) | Episode-Level Analysis",
            unsafe_allow_html=True
        )

        # Initialize components
        loader = DuckDBTariffLoader()

        # Load standard tariff
        with st.spinner("Loading standard tariff..."):
            standard_df = create_standard_tariff()

        if standard_df.empty:
            st.error("❌ Cannot proceed without standard tariff. Please check REALITY_TARIFF.xlsx")
            return

        st.success(f"✅ Loaded {len(standard_df)} standard procedures")

        # Load claims statistics
        with st.spinner("Loading claims statistics for risk assessment..."):
            claims_stats = loader.load_claims_stats()

        if not claims_stats.empty:
            st.success(f"✅ Loaded claims stats for {len(claims_stats)} procedures")
        
        # Load quality metrics
        with st.spinner("Loading quality metrics (readmissions, complications)..."):
            quality_metrics = loader.load_quality_metrics()
        
        if not quality_metrics.empty:
            st.success(f"✅ Loaded quality metrics for {len(quality_metrics)} providers")
        
        # Load episode data
        with st.spinner("Loading episode-level chronic condition data..."):
            episode_data = loader.load_episode_data()
        
        if not episode_data.empty:
            st.success(f"✅ Loaded episode data for {len(episode_data)} provider-condition combinations")

        # Initialize enhanced engine
        with st.spinner("Initializing enhanced banding engine..."):
            engine = HospitalBandingEngine(
                standard_df, 
                claims_stats=claims_stats,
                quality_metrics=quality_metrics,
                episode_data=episode_data
            )

        st.success("✅ Enhanced banding engine ready with quality & risk assessment")

    except Exception as e:
        st.error(f"❌ Error initializing application: {e}")
        import traceback
        st.code(traceback.format_exc())
        return
    
    # Display thresholds in sidebar
    st.sidebar.markdown("### 📊 Band Thresholds")
    for band, threshold in engine.thresholds.items():
        st.sidebar.metric(f"Band {band}", f"₦{threshold:,.0f}")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### ℹ️ About (Enhanced)")
    st.sidebar.info(
        "**Enhanced Dashboard with Actuarial Best Practices**\n\n"
        "✅ **Quality Layer**: Flags hospitals with poor outcomes\n\n"
        "✅ **Fraud Detection**: P90/P95 risk tracking\n\n"
        "✅ **Dual Reporting**: Weighted (TCOC) + Unweighted (Unit Price)\n\n"
        "✅ **Episode Analysis**: Chronic condition total cost\n\n"
        "Based on industry standards from Massachusetts GIC, Symmetry ETG, and actuarial research."
    )
    
    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 All Tariffs (Dual Report)", 
        "📤 Upload Tariff", 
        "🏥 Episode Analysis",
        "⚙️ Settings"
    ])
    
    with tab1:
        st.markdown("## Current Bands for Mapped Tariffs (Dual Reporting)")
        st.info("ℹ️ Dual methodology: **Weighted** (TCOC impact) + **Unweighted** (unit price negotiation)")
        
        # Recalculate button
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("🔄 Reload & Recalculate All", type="primary"):
                st.cache_data.clear()
                st.rerun()
        
        # Display all tariffs with dual reporting
        display_all_tariffs_with_bands(loader, engine)
    
    with tab2:
        st.markdown("## Upload Hospital Tariff for Dual Analysis")
        
        st.info(
            "📋 Upload a CSV file with at least these columns: "
            "`procedurecode` (or `proedurecode`), `tariffamount`. "
            "Optionally include `tariffname` and `proceduredesc`."
        )
        
        # File uploader
        uploaded_file = st.file_uploader(
            "Choose a CSV file",
            type=['csv'],
            help="Upload hospital tariff CSV file"
        )
        
        if uploaded_file is not None:
            try:
                # Read uploaded file
                hospital_df = pd.read_csv(uploaded_file, thousands=',')
                
                # Handle common typos
                if 'proedurecode' in hospital_df.columns and 'procedurecode' not in hospital_df.columns:
                    hospital_df['procedurecode'] = hospital_df['proedurecode']
                
                st.success(f"✅ File uploaded: {uploaded_file.name}")
                
                # Preview
                with st.expander("📄 Preview uploaded data"):
                    st.dataframe(hospital_df.head(10), use_container_width=True)
                
                # Get tariff name
                tariff_name_input = st.text_input(
                    "Tariff Name (optional)",
                    value=hospital_df.get('tariffname', [uploaded_file.name])[0] if 'tariffname' in hospital_df.columns else uploaded_file.name,
                    help="Enter a name for this tariff"
                )
                
                # Analyze button
                if st.button("🔍 Analyze Tariff (Dual Report)", type="primary"):
                    with st.spinner(f"Analyzing {tariff_name_input} with dual methodology..."):
                        result = engine.analyze_tariff(hospital_df, tariff_name_input)
                        
                        if result['success']:
                            st.success("✅ Dual analysis complete!")
                            display_single_tariff_analysis(result)
                        else:
                            st.error(f"❌ Analysis failed: {result.get('error', 'Unknown error')}")
                
            except Exception as e:
                st.error(f"❌ Error reading file: {e}")
                import traceback
                st.code(traceback.format_exc())
    
    with tab3:
        st.markdown("## 🏥 Episode-Level Analysis (Chronic Conditions)")
        
        if episode_data.empty:
            st.warning("⚠️ No episode data available")
        else:
            st.info(
                "**Episode-Level Total Cost of Care (TCOC)** for chronic conditions. "
                "Shows average cost per patient for managing chronic diseases like diabetes, hypertension, etc."
            )
            
            # Provider selector
            unique_providers = sorted(episode_data['providerid'].unique())
            selected_provider = st.selectbox(
                "Select Provider",
                options=unique_providers,
                format_func=lambda x: x if pd.notna(x) else "Unknown"
            )
            
            if selected_provider:
                provider_episodes = episode_data[episode_data['providerid'] == selected_provider]
                
                # Summary metrics
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    total_patients = int(provider_episodes['unique_patients'].sum())
                    st.metric("Total Chronic Patients", total_patients)
                
                with col2:
                    total_visits = int(provider_episodes['total_visits'].sum())
                    st.metric("Total Visits", total_visits)
                
                with col3:
                    total_cost = float(provider_episodes['total_cost'].sum())
                    st.metric("Total Cost", f"₦{total_cost:,.0f}")
                
                with col4:
                    avg_cost_per_patient = total_cost / total_patients if total_patients > 0 else 0
                    st.metric("Avg Cost/Patient", f"₦{avg_cost_per_patient:,.0f}")
                
                st.markdown("---")
                
                # Episode details by condition
                st.markdown("### Episode Details by Condition")
                
                episode_display = provider_episodes.copy()
                episode_display['Total Cost'] = episode_display['total_cost'].apply(lambda x: f"₦{x:,.0f}")
                episode_display['Avg Cost/Visit'] = episode_display['avg_cost_per_visit'].apply(lambda x: f"₦{x:,.0f}")
                episode_display['Cost/Patient'] = episode_display['cost_per_patient'].apply(lambda x: f"₦{x:,.0f}")
                
                st.dataframe(
                    episode_display[[
                        'condition_category', 'unique_patients', 'total_visits',
                        'Total Cost', 'Avg Cost/Visit', 'Cost/Patient'
                    ]].rename(columns={
                        'condition_category': 'Condition',
                        'unique_patients': 'Patients',
                        'total_visits': 'Visits'
                    }),
                    use_container_width=True
                )
                
                # Visualization
                fig = px.bar(
                    provider_episodes,
                    x='condition_category',
                    y='cost_per_patient',
                    color='condition_category',
                    title=f"Cost per Patient by Chronic Condition - {selected_provider}",
                    labels={'cost_per_patient': 'Cost per Patient (₦)', 'condition_category': 'Condition'}
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.markdown("## ⚙️ Settings & Configuration")
        
        st.markdown("### Database Configuration")
        st.info(f"📁 **DuckDB Path:** `ai_driven_data.duckdb`")
        st.info(f"📊 **Standard Tariff:** REALITY_TARIFF.xlsx (from project files)")
        st.success(f"✅ **Processing:** Only mapped tariffs (via providers_tariff table)")
        
        st.markdown("---")
        st.markdown("### Data Statistics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Standard Procedures", len(standard_df))
            mapped_tariffs = len(loader.get_unique_tariffs())
            st.metric("Mapped Tariff-Provider Combos", mapped_tariffs)
            
            if not claims_stats.empty:
                st.metric("Procedures with Claims Data", len(claims_stats))
        
        with col2:
            if not quality_metrics.empty:
                st.metric("Providers with Quality Metrics", len(quality_metrics))
            
            if not episode_data.empty:
                st.metric("Episode-Condition Combinations", len(episode_data))
        
        st.markdown("---")
        st.markdown("### Cache Management")
        
        if st.button("🗑️ Clear All Cached Data"):
            st.cache_data.clear()
            st.success("✅ Cache cleared! Reload to fetch fresh data.")
        
        st.markdown("---")
        st.markdown("### Enhanced Methodology")
        
        with st.expander("📖 How Enhanced Band Assignment Works"):
            st.markdown("""
            **DUAL REPORTING METHODOLOGY (Industry Best Practice)**
            
            #### PRIMARY METRIC: Weighted Band (Total Cost of Care)
            
            **Use Case:** Network steerage, identifying high-impact cost drivers
            
            **Calculation:**
            1. Each procedure weighted by log(frequency+1) from actual claims
            2. Weighted Average = Σ(price × log_freq) / Σ(log_freq)
            3. Band assignment via dominant band logic (≥60%) or weighted average
            
            **Why Weighted?** High-frequency procedures drive total costs. A hospital 
            charging ₦1,500 for a procedure used 100 times has more impact (₦150,000) 
            than one charging ₦100,000 for a procedure used once.
            
            #### SECONDARY METRIC: Unweighted Band (Unit Price)
            
            **Use Case:** Contract negotiations, identifying overpriced procedures
            
            **Calculation:**
            1. Equal weight to all procedures
            2. Simple Average = Σ(price) / count
            3. Band assignment via dominant band logic or simple average
            
            **Why Unweighted?** Identifies procedures with excessive unit costs 
            regardless of frequency - important for negotiations.
            
            ---
            
            #### QUALITY LAYER (New)
            
            **Quality Flags:**
            - 30-day readmission rate >20% (industry benchmark: 15%)
            - Complication rate >3% (industry benchmark: 2%)
            
            **Quality Override:** Poor quality providers may be excluded from 
            preferred networks **regardless of cost**.
            
            ---
            
            #### RISK FLAGS (Enhanced)
            
            **P90 Flag:** Price exceeds 90th percentile from real claims
            - Indicator: Pricing above market norm
            - Action: Review for potential overpricing
            
            **P95 Flag:** Price exceeds 95th percentile from real claims
            - Indicator: Extreme outlier (>3x median = fraud risk)
            - Action: Detailed audit required
            
            **Fraud Risk Score:** 0-100 scale based on price ratio to market median
            
            ---
            
            #### EPISODE-LEVEL ANALYSIS (New)
            
            **Chronic Conditions:** Diabetes, Hypertension, Asthma, CKD
            
            **Metrics:**
            - Total cost per patient (full episode)
            - Cost per visit
            - Patient volume
            
            **Use Case:** Identify providers with efficient chronic disease management 
            (lower total cost of care per patient).
            
            ---
            
            **Based on actuarial research from:**
            - Massachusetts Group Insurance Commission (GIC)
            - Symmetry Episode Treatment Groups (ETG)
            - Blue Cross Blue Shield Massachusetts
            - Axene Health Partners
            """)


if __name__ == "__main__":
    main()