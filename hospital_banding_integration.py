"""
HOSPITAL BANDING INTEGRATION
=============================

Connects provider analysis to hospital_banding.py system.
Automatically checks if top providers are charging correct bands.
"""

import sys
from pathlib import Path
import pandas as pd
import tempfile
import os

# Import the hospital banding engine
sys.path.insert(0, '/Users/kenechukwuchukwuka/Downloads/DLT')
from hospital_banding import HospitalBandingEngine


class ProviderBandingAnalyzer:
    """Analyzes provider tariffs against standard banding"""
    
    def __init__(self, standard_tariff_path: str = '/Users/kenechukwuchukwuka/Downloads/DLT/REALITY TARIFF_Sheet1.csv'):
        self.standard_tariff_path = standard_tariff_path
        self.engine = None
        
        # Try to initialize engine
        try:
            # Convert Excel to CSV if needed
            if standard_tariff_path.endswith('.xlsx'):
                df = pd.read_excel(standard_tariff_path)
                # Save to temp CSV
                self.temp_csv = tempfile.mktemp(suffix='.csv')
                df.to_csv(self.temp_csv, index=False)
                self.engine = HospitalBandingEngine(self.temp_csv, use_claims=True)
            else:
                self.engine = HospitalBandingEngine(standard_tariff_path, use_claims=True)
        except Exception as e:
            print(f"Warning: Could not initialize HospitalBandingEngine: {e}")
            self.engine = None
    
    def analyze_provider_tariff(self, provider_id: str, provider_name: str, db_conn) -> dict:
        """
        Analyze a single provider's tariff against standard bands
        
        Parameters:
        -----------
        provider_id : str
            Provider TIN/ID
        provider_name : str
            Provider name
        db_conn : duckdb.Connection
            Database connection
        
        Returns:
        --------
        dict with banding analysis results
        """
        
        if not self.engine:
            return {
                'success': False,
                'error': 'Banding engine not initialized'
            }
        
        try:
            # Get provider tariff from database
            query = """
            SELECT 
                procedurecode,
                tariffamount
            FROM "AI DRIVEN DATA"."PROVIDERS_TARIFF"
            WHERE providerid = ?
                AND tariffamount > 0
            """
            
            tariff_df = db_conn.execute(query, [provider_id]).fetchdf()
            
            if tariff_df.empty:
                return {
                    'success': False,
                    'error': 'No tariff data found for provider'
                }
            
            # Save to temporary CSV for banding analysis
            temp_hospital_csv = tempfile.mktemp(suffix='.csv')
            tariff_df.to_csv(temp_hospital_csv, index=False)
            
            # Run banding analysis
            result = self.engine.analyze_hospital(temp_hospital_csv, provider_name)
            
            # Clean up temp file
            os.remove(temp_hospital_csv)
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def analyze_top_providers(self, provider_list: list, db_conn, top_n: int = 5) -> dict:
        """
        Analyze top N providers from provider analysis
        
        Parameters:
        -----------
        provider_list : list
            List of provider dicts from analyze_providers()
        db_conn : duckdb.Connection
            Database connection
        top_n : int
            Number of top providers to analyze (default 5)
        
        Returns:
        --------
        dict with results for each provider
        """
        
        if not self.engine:
            return {
                'success': False,
                'error': 'Banding engine not initialized'
            }
        
        results = []
        
        # Take top N providers
        for provider in provider_list[:top_n]:
            provider_id = provider.get('nhisproviderid')
            provider_name = provider.get('providername', 'Unknown')
            
            if not provider_id or pd.isna(provider_id) or provider_id == '':
                continue
            
            # Analyze this provider
            analysis = self.analyze_provider_tariff(provider_id, provider_name, db_conn)
            
            if analysis.get('success'):
                # Calculate overcharging metrics
                weighted_avg = analysis.get('weighted_avg', 0)
                final_band = analysis.get('final_band', 'Unknown')
                
                # Get band thresholds
                thresholds = self.engine.thresholds if self.engine else {}
                correct_band_threshold = thresholds.get(final_band, 0)
                
                # Calculate overcharge percentage
                if correct_band_threshold > 0:
                    overcharge_pct = ((weighted_avg - correct_band_threshold) / correct_band_threshold * 100)
                else:
                    overcharge_pct = 0
                
                # Estimate annual impact (based on provider's total cost)
                provider_cost = provider.get('total_cost', 0)
                annual_impact = provider_cost * (overcharge_pct / 100) if overcharge_pct > 0 else 0
                
                results.append({
                    'provider_id': provider_id,
                    'provider_name': provider_name,
                    'provider_cost': provider_cost,
                    'assigned_band': final_band,
                    'weighted_avg': weighted_avg,
                    'correct_band_threshold': correct_band_threshold,
                    'overcharge_pct': overcharge_pct,
                    'annual_impact': annual_impact,
                    'confidence': analysis.get('confidence', 0),
                    'method': analysis.get('assignment_method', 'Unknown')
                })
            else:
                # Failed analysis
                results.append({
                    'provider_id': provider_id,
                    'provider_name': provider_name,
                    'error': analysis.get('error', 'Unknown error'),
                    'success': False
                })
        
        return {
            'success': True,
            'results': results,
            'analyzed_count': len([r for r in results if not r.get('error')]),
            'failed_count': len([r for r in results if r.get('error')])
        }
    
    def close(self):
        """Clean up temporary files"""
        if hasattr(self, 'temp_csv') and os.path.exists(self.temp_csv):
            os.remove(self.temp_csv)


def run_banding_analysis_for_company(analysis_data: dict) -> dict:
    """
    Main function to run banding analysis for a company
    
    Parameters:
    -----------
    analysis_data : dict
        Complete analysis data from run_comprehensive_analysis()
    
    Returns:
    --------
    dict with banding results
    """
    
    try:
        from complete_calculation_engine import CalculationEngine
        
        # Get top providers
        providers = analysis_data.get('providers', {})
        top_providers = providers.get('top_providers', [])
        
        if not top_providers:
            return {
                'success': False,
                'error': 'No provider data available'
            }
        
        # Initialize banding analyzer
        analyzer = ProviderBandingAnalyzer()
        
        # Get database connection
        engine = CalculationEngine()
        db_conn = engine.connect()
        
        # Analyze top 5 providers
        results = analyzer.analyze_top_providers(top_providers, db_conn, top_n=5)
        
        # Close connections
        analyzer.close()
        engine.close()
        
        return results
        
    except Exception as e:
        return {
            'success': False,
            'error': f'Banding analysis failed: {str(e)}'
        }


# Convenience function for Streamlit integration
def add_banding_button(analysis_data: dict):
    """
    Add a button to trigger banding analysis
    Returns banding results if button clicked
    """
    import streamlit as st
    
    if st.button("🏥 Run Hospital Banding Analysis", help="Check if top 5 providers are charging correct bands"):
        with st.spinner("Analyzing hospital bands..."):
            results = run_banding_analysis_for_company(analysis_data)
            
            if results.get('success'):
                st.success(f"✅ Analyzed {results.get('analyzed_count', 0)} providers")
            else:
                st.error(f"❌ {results.get('error', 'Unknown error')}")
            
            return results
    
    return None