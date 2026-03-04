#!/usr/bin/env python3
"""
Hospital Band Analysis - REALITY-ADJUSTED Tariff
=================================================

This script analyzes hospital bands using ACTUAL BILLING BEHAVIOR from claims
rather than relying solely on official published tariffs.

CONCEPT:
--------
Hospitals often charge differently from their published tariff:
- Some charge MORE (upcoding, price creep)
- Some charge LESS (discounts, negotiations)

This script creates a HYBRID TARIFF that shows reality:
- For procedures WITH claims: Use AVG(approvedamount) from last N months
- For procedures WITHOUT claims: Use official tariff price

BUSINESS VALUE:
---------------
✓ Detect hospitals whose actual charges differ from published rates
✓ Catch systematic overcharging (fraud indicator)
✓ Identify generous discounters (reward them)
✓ See if behavior changes band classification
✓ Compare "Official Band" vs "Reality Band"

Author: Casey's AI Assistant
Date: November 2025
Version: 5.0-Reality-Adjusted
"""

import streamlit as st
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Page config
st.set_page_config(
    page_title="Reality-Adjusted Hospital Banding",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .reality-alert {
        background: linear-gradient(135deg, #ff6b6b 0%, #ffa500 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .discount-alert {
        background: linear-gradient(135deg, #4ecdc4 0%, #45b7d1 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        font-weight: bold;
        margin: 1rem 0;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
</style>
""", unsafe_allow_html=True)


class RealityAdjustedTariffBuilder:
    """Build hybrid tariff from claims reality + official tariff fallback"""
    
    def __init__(self, db_path: str = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'):
        self.db_path = db_path
    
    def get_connection(self):
        """Get DuckDB connection"""
        if not Path(self.db_path).exists():
            st.error(f"❌ DuckDB not found: {self.db_path}")
            return None
        return duckdb.connect(self.db_path, read_only=True)
    
    def _normalize_code(self, value) -> str:
        """Normalize procedure codes"""
        if value is None or pd.isna(value):
            return ""
        return str(value).strip().lower().replace(" ", "")
    
    @st.cache_data(ttl=300)
    def get_providers_with_tariffs(_self) -> pd.DataFrame:
        """Get all providers with their tariff assignments"""
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            query = """
            SELECT DISTINCT
                p.providerid,
                p.providername,
                p.bands as official_band,
                pt.tariffid,
                t.tariffname
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt
                ON p.protariffid = pt.protariffid
            INNER JOIN "AI DRIVEN DATA"."TARIFF" t
                ON pt.tariffid = t.tariffid
            WHERE LOWER(p.providername) NOT LIKE '%nhis%'
                AND p.providerid IS NOT NULL
            ORDER BY p.providername
            """
            
            df = conn.execute(query).fetchdf()
            conn.close()
            
            return df
            
        except Exception as e:
            st.error(f"Error loading providers: {e}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)
    def build_reality_adjusted_tariff(
        _self, 
        providerid: str, 
        standard_procedures: pd.DataFrame,
        lookback_months: int = 6
    ) -> pd.DataFrame:
        """
        Build hybrid tariff for a provider using:
        1. Claims avg (last N months) for used procedures
        2. Official tariff for unused procedures
        
        Parameters:
        -----------
        providerid : str
            Provider TIN
        standard_procedures : pd.DataFrame
            REALITY_TARIFF.xlsx procedures to analyze
        lookback_months : int
            How many months back to look at claims
        
        Returns:
        --------
        DataFrame with columns:
        - procedurecode
        - proceduredesc
        - reality_price (hybrid)
        - price_source ('CLAIMS' or 'TARIFF')
        - claims_count (if from claims)
        - official_tariff_price
        - price_difference
        - price_difference_pct
        """
        try:
            conn = _self.get_connection()
            if conn is None:
                return pd.DataFrame()
            
            # Calculate cutoff date
            cutoff_date = datetime.now() - timedelta(days=lookback_months * 30)
            cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')
            
            # Get claims-based pricing (actual behavior)
            claims_query = f"""
            SELECT 
                LOWER(TRIM(c.code)) as procedurecode,
                COUNT(*) as claims_count,
                AVG(c.approvedamount) as avg_approved,
                STDDEV(c.approvedamount) as std_approved,
                MIN(c.approvedamount) as min_approved,
                MAX(c.approvedamount) as max_approved,
                APPROX_QUANTILE(c.approvedamount, 0.50) as median_approved
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            WHERE c.nhisproviderid = '{providerid}'
                AND c.datesubmitted >= DATE '{cutoff_date_str}'
                AND c.code IS NOT NULL
                AND c.approvedamount > 0
            GROUP BY LOWER(TRIM(c.code))
            HAVING COUNT(*) >= 1  -- At least 1 claim
            """
            
            claims_pricing = conn.execute(claims_query).fetchdf()
            
            # Get official tariff pricing (published rates)
            # Find this provider's tariff
            provider_tariff_query = f"""
            SELECT DISTINCT pt.tariffid
            FROM "AI DRIVEN DATA"."PROVIDERS" p
            INNER JOIN "AI DRIVEN DATA"."PROVIDERS_TARIFF" pt
                ON p.protariffid = pt.protariffid
            WHERE p.providerid = '{providerid}'
            LIMIT 1
            """
            
            tariff_result = conn.execute(provider_tariff_query).fetchdf()
            
            if tariff_result.empty:
                conn.close()
                return pd.DataFrame()
            
            tariffid = tariff_result['tariffid'].iloc[0]
            
            # Get official tariff prices
            tariff_query = f"""
            SELECT
                LOWER(TRIM(t.procedurecode)) as procedurecode,
                t.tariffamount as official_price
            FROM "AI DRIVEN DATA"."TARIFF" t
            WHERE t.tariffid = {tariffid}
                AND t.tariffamount > 0
            """
            
            official_pricing = conn.execute(tariff_query).fetchdf()
            conn.close()
            
            # Normalize standard procedures
            standard_procedures = standard_procedures.copy()
            standard_procedures['procedurecode'] = standard_procedures['procedurecode'].apply(_self._normalize_code)
            
            # Build hybrid tariff
            hybrid_tariff = []
            
            for _, std_proc in standard_procedures.iterrows():
                proc_code = std_proc['procedurecode']
                proc_desc = std_proc.get('proceduredesc', 'N/A')
                
                # Check if we have claims data
                claims_match = claims_pricing[claims_pricing['procedurecode'] == proc_code]
                
                if not claims_match.empty:
                    # USE CLAIMS DATA (reality)
                    claims_avg = float(claims_match['avg_approved'].iloc[0])
                    claims_count = int(claims_match['claims_count'].iloc[0])
                    
                    # Get official price for comparison
                    tariff_match = official_pricing[official_pricing['procedurecode'] == proc_code]
                    official_price = float(tariff_match['official_price'].iloc[0]) if not tariff_match.empty else None
                    
                    # Calculate difference
                    if official_price and official_price > 0:
                        price_diff = claims_avg - official_price
                        price_diff_pct = (price_diff / official_price) * 100
                    else:
                        price_diff = None
                        price_diff_pct = None
                    
                    hybrid_tariff.append({
                        'procedurecode': proc_code,
                        'proceduredesc': proc_desc if proc_desc != 'N/A' else (
                            claims_match['proceduredesc'].iloc[0] if 'proceduredesc' in claims_match.columns else 'N/A'
                        ),
                        'reality_price': claims_avg,
                        'price_source': 'CLAIMS',
                        'claims_count': claims_count,
                        'official_tariff_price': official_price,
                        'price_difference': price_diff,
                        'price_difference_pct': price_diff_pct
                    })
                    
                else:
                    # USE OFFICIAL TARIFF (no claims data)
                    tariff_match = official_pricing[official_pricing['procedurecode'] == proc_code]

                    if not tariff_match.empty:
                        official_price = float(tariff_match['official_price'].iloc[0])

                        hybrid_tariff.append({
                            'procedurecode': proc_code,
                            'proceduredesc': proc_desc,
                            'reality_price': official_price,
                            'price_source': 'TARIFF',
                            'claims_count': 0,
                            'official_tariff_price': official_price,
                            'price_difference': 0.0,
                            'price_difference_pct': 0.0
                        })
            
            if not hybrid_tariff:
                return pd.DataFrame()
            
            return pd.DataFrame(hybrid_tariff)
            
        except Exception as e:
            st.error(f"Error building reality-adjusted tariff: {e}")
            import traceback
            st.code(traceback.format_exc())
            return pd.DataFrame()


class RealityBandingEngine:
    """Band analysis using reality-adjusted tariffs"""
    
    def __init__(self, standard_tariff_df: pd.DataFrame, claims_stats: pd.DataFrame = None):
        self.standard_df = standard_tariff_df.copy()
        self.claims_stats = claims_stats
        self.thresholds = {}
        self.std_dict = {}
        
        self._prepare_standard_tariff()
        self._calculate_thresholds()
    
    def _normalize_code(self, value) -> str:
        if value is None or pd.isna(value):
            return ""
        return str(value).strip().lower().replace(" ", "")
    
    def _prepare_standard_tariff(self):
        """Prepare standard tariff"""
        self.standard_df['procedurecode'] = self.standard_df['procedurecode'].apply(self._normalize_code)
        
        for col in ['band_a', 'band_b', 'band_c', 'band_d', 'band_special']:
            if col in self.standard_df.columns:
                self.standard_df[col] = pd.to_numeric(self.standard_df[col], errors='coerce')
        
        if 'frequency' not in self.standard_df.columns:
            self.standard_df['frequency'] = 1
        self.standard_df['effective_frequency'] = self.standard_df['frequency']
        
        # Update with claims frequencies
        if self.claims_stats is not None and not self.claims_stats.empty:
            claims_dict = self.claims_stats.set_index('procedurecode')['count'].to_dict()
            for idx, row in self.standard_df.iterrows():
                proc_code = row['procedurecode']
                if proc_code in claims_dict:
                    self.standard_df.at[idx, 'effective_frequency'] = claims_dict[proc_code]
        
        self.std_dict = {row['procedurecode']: row for _, row in self.standard_df.iterrows()}
    
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
        """Determine band for a price"""
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
    
    def analyze_reality_tariff(self, reality_tariff_df: pd.DataFrame, provider_name: str) -> Dict:
        """
        Analyze reality-adjusted tariff with DUAL methodology
        
        Returns both weighted and unweighted bands for reality-adjusted prices
        """
        try:
            if reality_tariff_df.empty:
                return {'success': False, 'error': 'Empty tariff'}
            
            # Initialize accumulators - WEIGHTED
            weighted_total_price = 0.0
            weighted_total_freq = 0.0
            weighted_band_freq = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
            
            # Initialize accumulators - UNWEIGHTED
            unweighted_total_price = 0.0
            unweighted_count = 0
            unweighted_band_count = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
            
            # Track pricing behavior
            procedure_details = []
            matched = 0
            claims_based_count = 0
            tariff_based_count = 0
            overcharging_count = 0
            undercharging_count = 0
            total_overcharge_amount = 0.0
            total_undercharge_amount = 0.0
            
            # Process each procedure
            for _, row in reality_tariff_df.iterrows():
                proc_code = row['procedurecode']
                reality_price = row['reality_price']
                price_source = row['price_source']
                official_price = row.get('official_tariff_price')
                price_diff = row.get('price_difference', 0)
                price_diff_pct = row.get('price_difference_pct', 0)
                
                if proc_code not in self.std_dict:
                    continue
                
                matched += 1
                std_proc = self.std_dict[proc_code]
                
                # Track source
                if price_source == 'CLAIMS':
                    claims_based_count += 1
                    
                    # Track over/undercharging
                    if pd.notna(price_diff) and price_diff != 0:
                        if price_diff > 0:  # Overcharging
                            overcharging_count += 1
                            total_overcharge_amount += price_diff
                        else:  # Undercharging (discount)
                            undercharging_count += 1
                            total_undercharge_amount += abs(price_diff)
                else:
                    tariff_based_count += 1
                
                # WEIGHTED
                freq = float(std_proc.get('effective_frequency', 1))
                log_freq = np.log1p(freq)
                weighted_price = reality_price * log_freq
                weighted_total_price += weighted_price
                weighted_total_freq += log_freq
                
                # UNWEIGHTED
                unweighted_total_price += reality_price
                unweighted_count += 1
                
                # Determine band
                band = self._determine_procedure_band(reality_price, std_proc)
                
                # Update distributions
                weighted_band_freq[band] += log_freq
                unweighted_band_count[band] += 1
                
                # Store details
                procedure_details.append({
                    'procedurecode': proc_code,
                    'proceduredesc': row.get('proceduredesc', 'N/A'),
                    'reality_price': reality_price,
                    'price_source': price_source,
                    'official_price': official_price,
                    'price_difference': price_diff,
                    'price_difference_pct': price_diff_pct,
                    'band': band,
                    'band_a': std_proc['band_a'],
                    'band_b': std_proc['band_b'],
                    'band_c': std_proc['band_c'],
                    'band_d': std_proc['band_d'],
                    'frequency': log_freq,
                    'claims_count': row.get('claims_count', 0)
                })
            
            if weighted_total_freq == 0 or unweighted_count == 0:
                return {'success': False, 'error': 'No matching procedures'}
            
            # === WEIGHTED ANALYSIS ===
            weighted_avg = weighted_total_price / weighted_total_freq
            weighted_calculated_band = self._assign_band_from_avg(weighted_avg)
            
            weighted_total = sum(weighted_band_freq.values())
            weighted_band_pcts = {
                band: (freq / weighted_total * 100) if weighted_total > 0 else 0 
                for band, freq in weighted_band_freq.items()
            }
            
            weighted_valid_bands = {b: p for b, p in weighted_band_pcts.items() if b != 'check'}
            weighted_dominant_band = max(weighted_valid_bands, key=weighted_valid_bands.get) if weighted_valid_bands else weighted_calculated_band
            weighted_dominant_pct = weighted_valid_bands.get(weighted_dominant_band, 0)
            
            if weighted_dominant_pct >= 60:
                weighted_final_band = weighted_dominant_band
                weighted_method = "Dominant Band"
            else:
                weighted_final_band = weighted_calculated_band
                weighted_method = "Weighted Average"
            
            if weighted_band_pcts.get('check', 0) > 30:
                weighted_final_band = weighted_calculated_band
                weighted_method = "Gaming Protection"
            
            # === UNWEIGHTED ANALYSIS ===
            unweighted_avg = unweighted_total_price / unweighted_count
            unweighted_calculated_band = self._assign_band_from_avg(unweighted_avg)
            
            unweighted_band_pcts = {
                band: (count / unweighted_count * 100) if unweighted_count > 0 else 0 
                for band, count in unweighted_band_count.items()
            }
            
            unweighted_valid_bands = {b: p for b, p in unweighted_band_pcts.items() if b != 'check'}
            unweighted_dominant_band = max(unweighted_valid_bands, key=unweighted_valid_bands.get) if unweighted_valid_bands else unweighted_calculated_band
            unweighted_dominant_pct = unweighted_valid_bands.get(unweighted_dominant_band, 0)
            
            if unweighted_dominant_pct >= 60:
                unweighted_final_band = unweighted_dominant_band
                unweighted_method = "Dominant Band"
            else:
                unweighted_final_band = unweighted_calculated_band
                unweighted_method = "Simple Average"
            
            if unweighted_band_pcts.get('check', 0) > 30:
                unweighted_final_band = unweighted_calculated_band
                unweighted_method = "Gaming Protection"
            
            # Calculate confidence
            volume_conf = min(1.0, np.log1p(weighted_total_freq) / np.log1p(10000))
            coverage_conf = min(1.0, matched / len(self.std_dict))
            confidence = 0.60 * volume_conf + 0.40 * coverage_conf
            
            # Calculate pricing behavior metrics
            claims_pct = (claims_based_count / matched * 100) if matched > 0 else 0
            overcharge_pct = (overcharging_count / claims_based_count * 100) if claims_based_count > 0 else 0
            undercharge_pct = (undercharging_count / claims_based_count * 100) if claims_based_count > 0 else 0
            
            # Determine pricing behavior flag
            if overcharge_pct > 50:
                behavior_flag = "SYSTEMATIC_OVERCHARGING"
            elif undercharge_pct > 50:
                behavior_flag = "GENEROUS_DISCOUNTER"
            elif overcharge_pct > 25:
                behavior_flag = "MODERATE_OVERCHARGING"
            elif undercharge_pct > 25:
                behavior_flag = "MODERATE_DISCOUNTING"
            else:
                behavior_flag = "ALIGNED_WITH_TARIFF"
            
            return {
                'success': True,
                'provider_name': provider_name,
                
                # WEIGHTED
                'weighted_avg': weighted_avg,
                'weighted_final_band': weighted_final_band,
                'weighted_calculated_band': weighted_calculated_band,
                'weighted_dominant_band': weighted_dominant_band,
                'weighted_dominant_pct': weighted_dominant_pct,
                'weighted_method': weighted_method,
                'weighted_band_pcts': weighted_band_pcts,
                
                # UNWEIGHTED
                'unweighted_avg': unweighted_avg,
                'unweighted_final_band': unweighted_final_band,
                'unweighted_calculated_band': unweighted_calculated_band,
                'unweighted_dominant_band': unweighted_dominant_band,
                'unweighted_dominant_pct': unweighted_dominant_pct,
                'unweighted_method': unweighted_method,
                'unweighted_band_pcts': unweighted_band_pcts,
                
                # METADATA
                'confidence': confidence,
                'coverage_pct': (matched / len(self.std_dict) * 100) if len(self.std_dict) > 0 else 0,
                'matched_procedures': matched,
                'procedure_details': procedure_details,
                
                # PRICING BEHAVIOR
                'claims_based_count': claims_based_count,
                'tariff_based_count': tariff_based_count,
                'claims_pct': claims_pct,
                'overcharging_count': overcharging_count,
                'undercharging_count': undercharging_count,
                'overcharge_pct': overcharge_pct,
                'undercharge_pct': undercharge_pct,
                'total_overcharge_amount': total_overcharge_amount,
                'total_undercharge_amount': total_undercharge_amount,
                'behavior_flag': behavior_flag
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}


def load_standard_tariff() -> pd.DataFrame:
    """Load REALITY_TARIFF.xlsx"""
    try:
        reality_path = '/Users/kenechukwuchukwuka/Downloads/REALITY TARIFF.xlsx'
        if Path(reality_path).exists():
            df = pd.read_excel(reality_path)
        else:
            st.error("❌ REALITY_TARIFF.xlsx not found")
            return pd.DataFrame()
        
        required_cols = ['procedurecode', 'band_a', 'band_b', 'band_c', 'band_d', 'band_special']
        missing = [c for c in required_cols if c not in df.columns]
        
        if missing:
            st.error(f"❌ Missing columns: {missing}")
            return pd.DataFrame()
        
        return df
    except Exception as e:
        st.error(f"❌ Error loading standard tariff: {e}")
        return pd.DataFrame()


def load_claims_stats(db_path: str) -> pd.DataFrame:
    """Load claims statistics"""
    try:
        if not Path(db_path).exists():
            return pd.DataFrame()
        
        conn = duckdb.connect(db_path, read_only=True)
        
        query = """
        WITH cleaned_claims AS (
            SELECT 
                LOWER(TRIM(code)) as procedurecode,
                chargeamount
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE code IS NOT NULL 
                AND chargeamount > 0
        ),
        stats AS (
            SELECT 
                procedurecode,
                COUNT(*) as count,
                AVG(chargeamount) as mean,
                STDDEV(chargeamount) as std
            FROM cleaned_claims
            GROUP BY procedurecode
        )
        SELECT * FROM stats
        WHERE count >= 5
        ORDER BY count DESC
        """
        
        df = conn.execute(query).fetchdf()
        conn.close()
        
        return df
    except:
        return pd.DataFrame()


def display_reality_analysis(result: Dict, reality_tariff: pd.DataFrame):
    """Display comprehensive reality-adjusted analysis"""
    
    st.markdown(f"# Reality-Adjusted Analysis: {result['provider_name']}")
    
    # Behavior alert
    behavior = result['behavior_flag']
    
    if behavior == "SYSTEMATIC_OVERCHARGING":
        st.markdown(f"""
        <div class="reality-alert">
        🚨 SYSTEMATIC OVERCHARGING DETECTED<br>
        {result['overcharging_count']} procedures ({result['overcharge_pct']:.1f}%) charged above official tariff<br>
        Total excess charges: ₦{result['total_overcharge_amount']:,.0f}
        </div>
        """, unsafe_allow_html=True)
    elif behavior == "GENEROUS_DISCOUNTER":
        st.markdown(f"""
        <div class="discount-alert">
        ✅ GENEROUS DISCOUNTER<br>
        {result['undercharging_count']} procedures ({result['undercharge_pct']:.1f}%) charged below official tariff<br>
        Total savings: ₦{result['total_undercharge_amount']:,.0f}
        </div>
        """, unsafe_allow_html=True)
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Band Comparison",
        "💰 Pricing Behavior",
        "📋 Procedure Details",
        "📈 Visualizations"
    ])
    
    with tab1:
        st.markdown("## 🎯 Reality Band vs Official Band")
        
        st.info("""
        **Reality Band** = Based on actual approved amounts from claims (last N months)
        
        **Official Band** = Based on published tariff rates
        """)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Reality Band (Actual Behavior)")
            
            metric_col1, metric_col2 = st.columns(2)
            metric_col1.metric("Weighted Band", result['weighted_final_band'])
            metric_col2.metric("Unweighted Band", result['unweighted_final_band'])
            
            st.metric("Weighted Avg", f"₦{result['weighted_avg']:,.0f}")
            st.metric("Unweighted Avg", f"₦{result['unweighted_avg']:,.0f}")
            
            st.caption(f"Method: {result['weighted_method']}")
            st.caption(f"{result['claims_based_count']} procedures from claims, {result['tariff_based_count']} from tariff")
        
        with col2:
            st.markdown("### Data Composition")
            
            source_data = pd.DataFrame({
                'Source': ['Claims (Actual)', 'Tariff (Fallback)'],
                'Count': [result['claims_based_count'], result['tariff_based_count']],
                'Percentage': [result['claims_pct'], 100 - result['claims_pct']]
            })
            
            fig = px.pie(
                source_data,
                values='Count',
                names='Source',
                title='Price Source Distribution',
                color='Source',
                color_discrete_map={'Claims (Actual)': '#ff6b6b', 'Tariff (Fallback)': '#4ecdc4'}
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown("## 💰 Pricing Behavior Analysis")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Overcharging", f"{result['overcharging_count']}")
            st.caption(f"{result['overcharge_pct']:.1f}% of claims")
        
        with col2:
            st.metric("Undercharging", f"{result['undercharging_count']}")
            st.caption(f"{result['undercharge_pct']:.1f}% of claims")
        
        with col3:
            st.metric("Total Excess", f"₦{result['total_overcharge_amount']:,.0f}")
            st.caption("Amount above tariff")
        
        with col4:
            st.metric("Total Savings", f"₦{result['total_undercharge_amount']:,.0f}")
            st.caption("Amount below tariff")
        
        # Behavior interpretation
        st.markdown("### 💡 Interpretation")
        
        if behavior == "SYSTEMATIC_OVERCHARGING":
            st.error("""
            **⚠️ RED FLAG:** This provider consistently charges above their published tariff.
            
            **Possible Explanations:**
            - Systematic upcoding
            - Tariff not updated (stale)
            - Price creep over time
            
            **Recommended Actions:**
            1. Immediate audit of high-variance procedures
            2. Request tariff update or justification
            3. Consider contract renegotiation
            4. Increase pre-authorization scrutiny
            """)
        
        elif behavior == "GENEROUS_DISCOUNTER":
            st.success("""
            **✅ POSITIVE SIGNAL:** This provider charges less than their published tariff.
            
            **Possible Explanations:**
            - Negotiated discounts honored
            - Conservative tariff pricing
            - Member-friendly provider
            
            **Recommended Actions:**
            1. Recognize and reward this behavior
            2. Promote to members (preferred provider)
            3. Ensure discounts are sustained
            4. Use as benchmark for negotiations
            """)
        
        elif behavior == "MODERATE_OVERCHARGING":
            st.warning("""
            **⚠️ MONITOR:** Some procedures charged above tariff, but not systematic.
            
            **Recommended Actions:**
            1. Review high-variance procedures
            2. Monitor trend over time
            3. Discuss with provider
            """)
        
        elif behavior == "MODERATE_DISCOUNTING":
            st.info("""
            **✅ GOOD:** Some procedures charged below tariff.
            
            **Recommended Actions:**
            1. Acknowledge positive behavior
            2. Monitor consistency
            """)
        
        else:
            st.info("""
            **✅ ALIGNED:** Actual charges closely match published tariff.
            
            **Interpretation:** Provider is consistent and predictable.
            """)
    
    with tab3:
        st.markdown("## 📋 Procedure-Level Details")
        
        if not result['procedure_details']:
            st.warning("No procedure details")
        else:
            proc_df = pd.DataFrame(result['procedure_details'])
            
            # Filter options
            col1, col2 = st.columns(2)
            
            with col1:
                source_filter = st.multiselect(
                    "Filter by Source",
                    options=['CLAIMS', 'TARIFF'],
                    default=['CLAIMS', 'TARIFF']
                )
            
            with col2:
                variance_filter = st.selectbox(
                    "Variance Filter",
                    options=['All', 'Overcharged Only', 'Undercharged Only', 'Exact Match']
                )
            
            # Apply filters
            filtered_df = proc_df[proc_df['price_source'].isin(source_filter)].copy()
            
            if variance_filter == 'Overcharged Only':
                filtered_df = filtered_df[filtered_df['price_difference'] > 0]
            elif variance_filter == 'Undercharged Only':
                filtered_df = filtered_df[filtered_df['price_difference'] < 0]
            elif variance_filter == 'Exact Match':
                filtered_df = filtered_df[filtered_df['price_difference'] == 0]
            
            # Sort by price difference
            filtered_df = filtered_df.sort_values('price_difference', ascending=False)
            
            # Format for display
            display_df = filtered_df[[
                'procedurecode', 'proceduredesc', 'reality_price', 'official_price',
                'price_difference', 'price_difference_pct', 'price_source', 'band'
            ]].copy()
            
            display_df['reality_price'] = display_df['reality_price'].apply(lambda x: f"₦{x:,.0f}")
            display_df['official_price'] = display_df['official_price'].apply(
                lambda x: f"₦{x:,.0f}" if pd.notna(x) else "—"
            )
            display_df['price_difference'] = display_df['price_difference'].apply(
                lambda x: f"₦{x:,.0f}" if pd.notna(x) else "—"
            )
            display_df['price_difference_pct'] = display_df['price_difference_pct'].apply(
                lambda x: f"{x:+.1f}%" if pd.notna(x) else "—"
            )
            
            display_df.columns = [
                'Code', 'Description', 'Reality Price', 'Official Price',
                'Difference', 'Difference %', 'Source', 'Band'
            ]
            
            # Color code by variance
            def highlight_variance(row):
                diff_str = row['Difference %']
                if diff_str == "—":
                    return [''] * len(row)
                
                try:
                    diff_val = float(diff_str.replace('%', '').replace('+', ''))
                    if diff_val > 20:
                        return ['background-color: #ffe6e6'] * len(row)
                    elif diff_val < -20:
                        return ['background-color: #e6ffe6'] * len(row)
                except:
                    pass
                
                return [''] * len(row)
            
            st.dataframe(
                display_df.style.apply(highlight_variance, axis=1),
                use_container_width=True,
                height=500
            )
            
            # Download
            csv = display_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Procedure Details",
                data=csv,
                file_name=f"reality_adjusted_{result['provider_name']}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
    
    with tab4:
        st.markdown("## 📈 Visualizations")
        
        # Price variance distribution
        proc_df = pd.DataFrame(result['procedure_details'])
        claims_only = proc_df[proc_df['price_source'] == 'CLAIMS'].copy()
        
        if not claims_only.empty:
            # Histogram of price differences
            fig1 = px.histogram(
                claims_only,
                x='price_difference_pct',
                nbins=30,
                title='Distribution of Price Variance (% from Official Tariff)',
                labels={'price_difference_pct': 'Variance %', 'count': 'Number of Procedures'}
            )
            
            fig1.add_vline(x=0, line_dash="dash", line_color="green", annotation_text="Exact Match")
            fig1.add_vline(x=20, line_dash="dash", line_color="red", annotation_text="+20% Overcharge")
            fig1.add_vline(x=-20, line_dash="dash", line_color="blue", annotation_text="-20% Discount")
            
            st.plotly_chart(fig1, use_container_width=True)
            
            # Scatter: Official vs Reality
            fig2 = px.scatter(
                claims_only,
                x='official_price',
                y='reality_price',
                color='price_difference_pct',
                size='claims_count',
                hover_data=['procedurecode', 'proceduredesc'],
                title='Official Tariff vs Reality (Claims-Based)',
                labels={'official_price': 'Official Tariff (₦)', 'reality_price': 'Reality Price (₦)'},
                color_continuous_scale='RdYlGn_r'
            )
            
            # Add diagonal line (perfect match)
            max_price = max(claims_only['official_price'].max(), claims_only['reality_price'].max())
            fig2.add_trace(
                go.Scatter(
                    x=[0, max_price],
                    y=[0, max_price],
                    mode='lines',
                    name='Perfect Match',
                    line=dict(dash='dash', color='gray')
                )
            )
            
            st.plotly_chart(fig2, use_container_width=True)


def main():
    """Main application"""
    
    st.markdown('<h1 class="main-header">🔍 Reality-Adjusted Hospital Banding</h1>', unsafe_allow_html=True)
    
    st.info("""
    **What This Does:** Analyzes hospital bands using ACTUAL BILLING BEHAVIOR from claims, 
    not just published tariff rates.
    
    **Key Insight:** Hospitals often charge differently from their official tariff. 
    This tool reveals their true pricing patterns.
    """)
    
    # Initialize
    try:
        db_path = '/Users/kenechukwuchukwuka/Downloads/DLT/ai_driven_data.duckdb'
        
        # Load components
        with st.spinner("Loading standard tariff..."):
            standard_df = load_standard_tariff()
        
        if standard_df.empty:
            st.error("❌ Cannot proceed without standard tariff")
            return
        
        st.success(f"✅ Loaded {len(standard_df)} standard procedures")
        
        with st.spinner("Loading claims statistics..."):
            claims_stats = load_claims_stats(db_path)
        
        if not claims_stats.empty:
            st.success(f"✅ Loaded claims stats for {len(claims_stats)} procedures")
        
        # Initialize builders
        tariff_builder = RealityAdjustedTariffBuilder(db_path)
        engine = RealityBandingEngine(standard_df, claims_stats)
        
        st.success("✅ Reality-adjusted engine ready")
        
    except Exception as e:
        st.error(f"❌ Initialization error: {e}")
        import traceback
        st.code(traceback.format_exc())
        return
    
    # Sidebar
    st.sidebar.markdown("### ⚙️ Settings")
    
    lookback_months = st.sidebar.selectbox(
        "Claims Lookback Period",
        options=[3, 6, 9, 12, 18, 24],
        index=1,  # Default to 6 months
        help="How many months of claims data to analyze"
    )
    
    st.sidebar.info(f"""
    **Current Settings:**
    - Lookback: {lookback_months} months
    - Cutoff Date: {(datetime.now() - timedelta(days=lookback_months * 30)).strftime('%Y-%m-%d')}
    """)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Band Thresholds")
    for band, threshold in engine.thresholds.items():
        st.sidebar.metric(f"Band {band}", f"₦{threshold:,.0f}")
    
    # Main interface
    st.markdown("## 🏥 Select Provider for Analysis")
    
    # Get providers
    with st.spinner("Loading providers..."):
        providers_df = tariff_builder.get_providers_with_tariffs()
    
    if providers_df.empty:
        st.error("❌ No providers found")
        return
    
    st.success(f"✅ Found {len(providers_df)} providers with tariffs")
    
    # Provider selector
    provider_options = providers_df['providername'].unique().tolist()
    selected_provider_name = st.selectbox(
        "Choose Provider",
        options=provider_options,
        help="Select a provider to analyze their reality-adjusted pricing"
    )
    
    # Get provider details
    provider_info = providers_df[providers_df['providername'] == selected_provider_name].iloc[0]
    providerid = provider_info['providerid']
    official_band = provider_info['official_band']
    
    # Display provider info
    col1, col2, col3 = st.columns(3)
    col1.metric("Provider", selected_provider_name)
    col2.metric("Provider ID", providerid)
    col3.metric("Official Band", official_band)
    
    # Analyze button
    if st.button("🔍 Analyze Reality-Adjusted Band", type="primary"):
        with st.spinner(f"Building reality-adjusted tariff for {selected_provider_name}..."):
            
            # Build reality tariff
            reality_tariff = tariff_builder.build_reality_adjusted_tariff(
                providerid,
                standard_df,
                lookback_months
            )
            
            if reality_tariff.empty:
                st.error(f"❌ No data found for {selected_provider_name}")
                st.info("This could mean: No claims in the lookback period, or provider not in TARIFF table")
                return
            
            st.success(f"✅ Built reality tariff with {len(reality_tariff)} procedures")
            
            # Show composition
            claims_count = len(reality_tariff[reality_tariff['price_source'] == 'CLAIMS'])
            tariff_count = len(reality_tariff[reality_tariff['price_source'] == 'TARIFF'])
            
            st.info(f"""
            **Reality Tariff Composition:**
            - {claims_count} procedures from CLAIMS (actual behavior)
            - {tariff_count} procedures from TARIFF (fallback)
            - Total: {len(reality_tariff)} procedures
            """)
            
            # Analyze
            result = engine.analyze_reality_tariff(
                reality_tariff,
                selected_provider_name
            )
            
            if result['success']:
                st.success("✅ Analysis complete!")
                
                # Show band comparison upfront
                st.markdown("---")
                st.markdown("### 🎯 Band Comparison")
                
                comp_col1, comp_col2, comp_col3 = st.columns(3)
                
                with comp_col1:
                    st.metric("Official Band", official_band)
                    st.caption("From provider table")
                
                with comp_col2:
                    st.metric("Reality Weighted Band", result['weighted_final_band'])
                    st.caption("Claims-adjusted, frequency-weighted")
                
                with comp_col3:
                    band_change = result['weighted_final_band'] != official_band
                    if band_change:
                        st.warning(f"⚠️ BAND CHANGE DETECTED!")
                        st.caption(f"{official_band} → {result['weighted_final_band']}")
                    else:
                        st.success("✅ Band Confirmed")
                        st.caption("Reality matches official")
                
                st.markdown("---")
                
                # Full analysis
                display_reality_analysis(result, reality_tariff)
            else:
                st.error(f"❌ Analysis failed: {result.get('error')}")


if __name__ == "__main__":
    main()