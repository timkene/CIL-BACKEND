#!/usr/bin/env python3
"""
Hospital Band Analysis Tool - PATH 1 (Enhanced with DuckDB Claims)
==================================================================

Production-ready system with DuckDB claims integration for:
- Log-scaled frequency weighting
- Dominant band logic
- Claims-based risk flagging (using P90/P95 from actual claims)
- Gaming prevention mechanisms

Author: Casey's AI Assistant
Date: November 2025
Version: 2.1-DuckDB

Usage:
    python hospital_banding_duckdb.py --standard STANDARD.csv --hospitals FOLDER/ --output report.xlsx
    python hospital_banding_duckdb.py --standard STANDARD.csv --hospitals h1.csv h2.csv --use-claims
"""

import pandas as pd
import numpy as np
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import warnings
warnings.filterwarnings('ignore')

# Try to import duckdb
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("WARNING: duckdb not available. Claims data features will be limited.")
    print("Install with: pip install duckdb")

# Try to import rich for beautiful output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
    from rich.panel import Panel
    from rich import box
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None
    print("Note: Install 'rich' for beautiful output: pip install rich")


class DuckDBClaimsLoader:
    """Load claims statistics from DuckDB"""
    
    @staticmethod
    def load_claims_stats() -> pd.DataFrame:
        """
        Query DuckDB for claims statistics per procedure code
        
        Returns:
        --------
        DataFrame with columns: procedurecode, p25, p50, p75, p90, p95, mean, std, count
        """
        if not DUCKDB_AVAILABLE:
            return pd.DataFrame()
        
        try:
            # Connect to the actual DuckDB database file
            db_path = 'ai_driven_data.duckdb'
            if not Path(db_path).exists():
                if RICH_AVAILABLE:
                    console.print(f"[yellow]âš [/yellow] DuckDB file not found: {db_path}")
                else:
                    print(f"âš  DuckDB file not found: {db_path}")
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
                    AVG(chargeamount) as raw_mean,
                    STDDEV(chargeamount) as raw_std,
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
            
            if RICH_AVAILABLE:
                console.print(f"[green]âœ“[/green] Loaded claims statistics for {len(df)} procedures from DuckDB")
            else:
                print(f"âœ“ Loaded claims statistics for {len(df)} procedures from DuckDB")
            
            return df
            
        except Exception as e:
            if RICH_AVAILABLE:
                console.print(f"[yellow]âš [/yellow] Could not load claims from DuckDB: {e}")
            else:
                print(f"âš  Could not load claims from DuckDB: {e}")
            return pd.DataFrame()


class HospitalBandingEngine:
    """
    Enhanced hospital banding engine with DuckDB claims integration
    """
    
    def __init__(self, standard_tariff_path: str, use_claims: bool = False):
        """
        Initialize the banding engine
        
        Parameters:
        -----------
        standard_tariff_path : str
            Path to standard tariff CSV
        use_claims : bool
            Whether to load claims data from DuckDB
        """
        self.standard_tariff_path = standard_tariff_path
        self.use_claims = use_claims
        self.standard_df = None
        self.claims_stats = None
        self.thresholds = None
        self.std_dict = None
        
        self._load_standard_tariff()
        if use_claims:
            self._load_claims_from_duckdb()
        self._calculate_thresholds()
    
    def _normalize_code(self, value) -> str:
        """Normalize procedure codes"""
        if value is None:
            return ""
        try:
            return str(value).strip().lower().replace(" ", "")
        except:
            return str(value)
    
    def _load_standard_tariff(self):
        """Load and validate standard tariff"""
        try:
            self.standard_df = pd.read_csv(self.standard_tariff_path, thousands=',')
            
            # Normalize codes
            self.standard_df['procedurecode'] = self.standard_df['procedurecode'].astype(str).apply(self._normalize_code)
            
            # Convert band columns to numeric
            for col in ['band_a', 'band_b', 'band_c', 'band_d', 'band_special']:
                if col not in self.standard_df.columns:
                    raise ValueError(f"Missing required column: {col}")
                self.standard_df[col] = pd.to_numeric(self.standard_df[col], errors='coerce')
            
            # Set uniform frequency (fallback when no claims)
            if 'frequency' not in self.standard_df.columns:
                self.standard_df['frequency'] = 1
            self.standard_df['effective_frequency'] = self.standard_df['frequency']
            
            # Create lookup dictionary
            self.std_dict = {
                row['procedurecode']: row 
                for _, row in self.standard_df.iterrows()
            }
            
            if RICH_AVAILABLE:
                console.print(f"[green]âœ“[/green] Loaded {len(self.standard_df)} procedures from standard tariff")
            else:
                print(f"âœ“ Loaded {len(self.standard_df)} procedures from standard tariff")
                
        except Exception as e:
            print(f"ERROR: Failed to load standard tariff: {e}")
            sys.exit(1)
    
    def _load_claims_from_duckdb(self):
        """Load claims statistics from DuckDB"""
        self.claims_stats = DuckDBClaimsLoader.load_claims_stats()
        
        # Update effective frequencies if we have claims data
        if not self.claims_stats.empty:
            claims_dict = self.claims_stats.set_index('procedurecode')['count'].to_dict()
            
            for idx, row in self.standard_df.iterrows():
                proc_code = row['procedurecode']
                if proc_code in claims_dict:
                    self.standard_df.at[idx, 'effective_frequency'] = claims_dict[proc_code]
            
            if RICH_AVAILABLE:
                console.print(f"[green]âœ“[/green] Updated frequencies for procedures with claims data")
            else:
                print(f"âœ“ Updated frequencies for procedures with claims data")
    
    def _calculate_thresholds(self):
        """Calculate band thresholds using SIMPLE MEAN (universal thresholds)"""
        self.thresholds = {
            'D': float(self.standard_df['band_d'].mean()),
            'C': float(self.standard_df['band_c'].mean()),
            'B': float(self.standard_df['band_b'].mean()),
            'A': float(self.standard_df['band_a'].mean()),
            'Special': float(self.standard_df['band_special'].mean())
        }
        
        if RICH_AVAILABLE:
            console.print("\n[bold]Band Thresholds (Simple Mean):[/bold]")
            for band, threshold in self.thresholds.items():
                console.print(f"  Band {band}: â‚¦{threshold:,.2f}")
        else:
            print("\nBand Thresholds (Simple Mean):")
            for band, threshold in self.thresholds.items():
                print(f"  Band {band}: â‚¦{threshold:,.2f}")
    
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
    
    def _calculate_confidence(self, total_freq: float, matched: int) -> float:
        """Calculate confidence score"""
        volume_conf = min(1.0, np.log1p(total_freq) / np.log1p(10000))
        coverage_conf = min(1.0, matched / len(self.std_dict))
        return 0.60 * volume_conf + 0.40 * coverage_conf
    
    def analyze_hospital(self, hospital_path: str, hospital_name: Optional[str] = None) -> Dict:
        """
        Analyze a single hospital using dominant band methodology with claims data
        
        Parameters:
        -----------
        hospital_path : str
            Path to hospital tariff CSV
        hospital_name : str, optional
            Override hospital name
        
        Returns:
        --------
        Dict with analysis results
        """
        # Load hospital data
        try:
            hospital_df = pd.read_csv(hospital_path, thousands=',')
            
            # Handle common typos in column names
            if 'proedurecode' in hospital_df.columns and 'procedurecode' not in hospital_df.columns:
                hospital_df['procedurecode'] = hospital_df['proedurecode']
                if RICH_AVAILABLE:
                    console.print(f"[yellow]âš [/yellow] Fixed typo: 'proedurecode' -> 'procedurecode'")
                else:
                    print(f"âš  Fixed typo: 'proedurecode' -> 'procedurecode'")
            
            # Check required columns
            if 'procedurecode' not in hospital_df.columns:
                raise ValueError(f"Missing 'procedurecode' column. Found columns: {list(hospital_df.columns)}")
            if 'tariffamount' not in hospital_df.columns:
                raise ValueError(f"Missing 'tariffamount' column. Found columns: {list(hospital_df.columns)}")
            
            hospital_df['procedurecode'] = hospital_df['procedurecode'].astype(str).apply(self._normalize_code)
            hospital_df['tariffamount'] = pd.to_numeric(hospital_df['tariffamount'], errors='coerce')
            hospital_df = hospital_df.dropna(subset=['tariffamount'])
            hospital_df = hospital_df[hospital_df['tariffamount'] > 0]
            
            if len(hospital_df) == 0:
                raise ValueError("No valid procedures found after filtering (all tariffamount values are invalid or zero)")
            
            # Get hospital name
            if hospital_name is None:
                if 'tariffname' in hospital_df.columns:
                    hospital_name = hospital_df['tariffname'].iloc[0]
                else:
                    hospital_name = Path(hospital_path).stem
            
        except Exception as e:
            error_msg = str(e)
            if RICH_AVAILABLE:
                console.print(f"[red]âŒ[/red] Failed to analyze {hospital_name or Path(hospital_path).stem}: {error_msg}")
            else:
                print(f"âŒ Failed to analyze {hospital_name or Path(hospital_path).stem}: {error_msg}")
            return {
                'success': False,
                'hospital_name': hospital_name or Path(hospital_path).stem,
                'error': error_msg
            }
        
        # Get claims stats dict if available
        claims_dict = {}
        if self.claims_stats is not None and not self.claims_stats.empty:
            claims_dict = {
                row['procedurecode']: row
                for _, row in self.claims_stats.iterrows()
            }
        
        # Initialize accumulators
        total_weighted_price = 0.0
        total_log_freq = 0.0
        band_freq_weighted = {'A': 0, 'B': 0, 'C': 0, 'D': 0, 'Special': 0, 'check': 0}
        procedure_details = []
        matched = 0
        above_p90_count = 0
        above_p90_weight = 0.0
        
        # Process each procedure
        for _, row in hospital_df.iterrows():
            proc_code = row['procedurecode']
            price = row['tariffamount']
            
            if proc_code not in self.std_dict:
                continue
            
            matched += 1
            std_proc = self.std_dict[proc_code]
            freq = float(std_proc.get('effective_frequency', 1))
            
            # Log-scaled weighting
            log_freq = np.log1p(freq)
            weighted_price = price * log_freq
            total_weighted_price += weighted_price
            total_log_freq += log_freq
            
            # Determine band
            band = self._determine_procedure_band(price, std_proc)
            
            # Enhanced risk flagging with claims data
            flag_above_p90 = False
            claims_p90 = None
            claims_p95 = None
            
            if proc_code in claims_dict:
                claims_data = claims_dict[proc_code]
                claims_p90 = claims_data.get('p90')
                claims_p95 = claims_data.get('p95')
                
                # Flag if above P90
                if pd.notna(claims_p90) and price > claims_p90:
                    flag_above_p90 = True
                    above_p90_count += 1
                    above_p90_weight += log_freq
                
                # Auto-flag to 'check' if above P95 or >125% of band_a
                if pd.notna(claims_p95) and price > max(claims_p95, std_proc['band_a'] * 1.25):
                    band = "check"
            elif price > std_proc['band_a'] * 1.25:
                band = "check"
            
            # Update distributions
            band_freq_weighted[band] += log_freq
            
            # Store details
            procedure_details.append({
                'procedurecode': proc_code,
                'proceduredesc': std_proc.get('proceduredesc', 'N/A'),
                'price': price,
                'band': band,
                'band_a': std_proc['band_a'],
                'band_b': std_proc['band_b'],
                'band_c': std_proc['band_c'],
                'frequency': log_freq,
                'flag_above_p90': flag_above_p90,
                'claims_p90': claims_p90,
                'claims_p95': claims_p95
            })
        
        if total_log_freq == 0:
            # Provide diagnostic information
            hospital_proc_count = len(hospital_df)
            std_proc_count = len(self.std_dict)
            sample_hospital_codes = list(hospital_df['procedurecode'].head(5))
            sample_std_codes = list(self.std_dict.keys())[:5]
            
            error_msg = (
                f'No matching procedures found. '
                f'Hospital has {hospital_proc_count} procedures, '
                f'standard tariff has {std_proc_count} procedures. '
                f'Sample hospital codes: {sample_hospital_codes[:3]}, '
                f'Sample standard codes: {sample_std_codes[:3]}'
            )
            
            if RICH_AVAILABLE:
                console.print(f"[red]âŒ[/red] {hospital_name}: {error_msg}")
            else:
                print(f"âŒ {hospital_name}: {error_msg}")
            
            return {
                'success': False,
                'hospital_name': hospital_name,
                'error': error_msg
            }
        
        # Calculate weighted average
        weighted_avg = total_weighted_price / total_log_freq
        
        # Calculate band from weighted average
        calculated_band = self._assign_band_from_weighted_avg(weighted_avg)
        
        # Find dominant band (excluding 'check')
        total_freq = sum(band_freq_weighted.values())
        band_pcts = {band: (freq / total_freq * 100) if total_freq > 0 else 0 
                     for band, freq in band_freq_weighted.items()}
        
        valid_bands = {b: p for b, p in band_pcts.items() if b != 'check'}
        dominant_band = max(valid_bands, key=valid_bands.get) if valid_bands else calculated_band
        dominant_pct = valid_bands.get(dominant_band, 0)
        
        # Apply dominant band logic
        if dominant_pct >= 60:
            final_band = dominant_band
            method = "Dominant Band"
        else:
            final_band = calculated_band
            method = "Weighted Average"
        
        # Gaming protection: If >30% in 'check', use calculated band
        if band_pcts.get('check', 0) > 30:
            final_band = calculated_band
            method = "Gaming Protection"
        
        # Calculate confidence
        confidence = self._calculate_confidence(total_log_freq, matched)
        
        # Calculate coverage
        coverage_pct = (matched / len(self.std_dict) * 100) if len(self.std_dict) > 0 else 0
        
        # Claims-based risk metrics
        pct_above_p90 = (above_p90_weight / total_log_freq * 100) if total_log_freq > 0 else 0
        
        return {
            'success': True,
            'hospital_name': hospital_name,
            'weighted_avg': weighted_avg,
            'final_band': final_band,
            'calculated_band': calculated_band,
            'dominant_band': dominant_band,
            'dominant_pct': dominant_pct,
            'assignment_method': method,
            'confidence': confidence,
            'coverage_pct': coverage_pct,
            'matched_procedures': matched,
            'band_freq_weighted': band_freq_weighted,
            'band_pcts': band_pcts,
            'procedure_details': procedure_details,
            'above_p90_count': above_p90_count,
            'pct_above_p90': pct_above_p90,
            'has_claims_data': bool(claims_dict)
        }
    
    def analyze_multiple_hospitals(self, hospital_paths: List[str]) -> List[Dict]:
        """Analyze multiple hospitals"""
        results = []
        
        if RICH_AVAILABLE:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console
            ) as progress:
                task = progress.add_task("[cyan]Analyzing hospitals...", total=len(hospital_paths))
                
                for hospital_path in hospital_paths:
                    result = self.analyze_hospital(hospital_path)
                    results.append(result)
                    progress.update(task, advance=1)
        else:
            for i, hospital_path in enumerate(hospital_paths, 1):
                print(f"Processing {i}/{len(hospital_paths)}: {Path(hospital_path).name}")
                result = self.analyze_hospital(hospital_path)
                results.append(result)
        
        return results


def display_results_console(results: List[Dict]):
    """Display results in console"""
    successful_results = [r for r in results if r.get('success', False)]
    failed_results = [r for r in results if not r.get('success', False)]
    
    # Show errors first
    if failed_results:
        if RICH_AVAILABLE:
            console.print("\n[bold red]âš  Failed Analyses:[/bold red]")
            for result in failed_results:
                console.print(f"  [red]âŒ[/red] {result.get('hospital_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
        else:
            print("\nâš  Failed Analyses:")
            for result in failed_results:
                print(f"  âŒ {result.get('hospital_name', 'Unknown')}: {result.get('error', 'Unknown error')}")
    
    if not successful_results:
        if RICH_AVAILABLE:
            console.print("\n[red]No successful results to display[/red]")
        else:
            print("No successful results to display")
        return
    
    if RICH_AVAILABLE:
        console.print("\n")
        console.print(Panel.fit(
            "[bold]HOSPITAL BAND ANALYSIS RESULTS (with DuckDB Claims)[/bold]",
            border_style="cyan"
        ))
        
        # Main results table
        table = Table(show_header=True, header_style="bold cyan", box=box.ROUNDED)
        table.add_column("Hospital", style="cyan", width=25)
        table.add_column("Weighted\nAverage", justify="right")
        table.add_column("Band", justify="center")
        table.add_column("Method", style="dim", width=15)
        table.add_column("Conf %", justify="right")
        table.add_column("Above\nP90", justify="center")
        
        for result in successful_results:
            # Risk indicator based on P90
            if result.get('pct_above_p90', 0) > 40:
                p90_indicator = f"[red]{result['pct_above_p90']:.0f}%[/red]"
            elif result.get('pct_above_p90', 0) > 20:
                p90_indicator = f"[yellow]{result['pct_above_p90']:.0f}%[/yellow]"
            else:
                p90_indicator = f"[green]{result['pct_above_p90']:.0f}%[/green]" if result.get('has_claims_data') else "[dim]â€”[/dim]"
            
            table.add_row(
                result['hospital_name'][:25],
                f"â‚¦{result['weighted_avg']:,.0f}",
                result['final_band'],
                result['assignment_method'][:15],
                f"{result['confidence']*100:.0f}%",
                p90_indicator
            )
        
        console.print(table)
        
        # Summary statistics
        if any(r.get('has_claims_data') for r in successful_results):
            high_risk = sum(1 for r in successful_results if r.get('pct_above_p90', 0) > 40)
            if high_risk > 0:
                console.print(f"\n[bold red]âš ï¸  {high_risk} hospital(s) with >40% procedures above market P90[/bold red]")
    
    else:
        # Simple text output
        print("\n" + "="*90)
        print("HOSPITAL BAND ANALYSIS RESULTS (with DuckDB Claims)")
        print("="*90)
        print(f"{'Hospital':<30} {'Weighted Avg':>15} {'Band':>8} {'Method':<20} {'Conf':>6} {'P90%':>6}")
        print("-"*90)
        
        for result in successful_results:
            p90_str = f"{result.get('pct_above_p90', 0):.0f}%" if result.get('has_claims_data') else "â€”"
            
            print(f"{result['hospital_name'][:30]:<30} "
                  f"â‚¦{result['weighted_avg']:>14,.0f} "
                  f"{result['final_band']:>8} "
                  f"{result['assignment_method'][:20]:<20} "
                  f"{result['confidence']*100:>5.0f}% "
                  f"{p90_str:>6}")


def create_excel_report(results: List[Dict], output_path: str, engine):
    """Create comprehensive Excel report"""
    successful_results = [r for r in results if r.get('success', False)]
    
    if not successful_results:
        print("No successful results to export")
        return
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = []
        for result in successful_results:
            total_freq = sum(result['band_freq_weighted'].values())
            
            summary_data.append({
                'Hospital': result['hospital_name'],
                'Weighted Average': result['weighted_avg'],
                'Final Band': result['final_band'],
                'Calculated Band': result['calculated_band'],
                'Dominant Band': result['dominant_band'],
                'Dominant %': result['dominant_pct'],
                'Assignment Method': result['assignment_method'],
                'Confidence %': result['confidence'] * 100,
                'Coverage %': result['coverage_pct'],
                'Matched Procedures': result['matched_procedures'],
                'Above P90 Count': result.get('above_p90_count', 0),
                'Above P90 %': result.get('pct_above_p90', 0),
                '% Band A': (result['band_freq_weighted']['A'] / total_freq * 100) if total_freq > 0 else 0,
                '% Band B': (result['band_freq_weighted']['B'] / total_freq * 100) if total_freq > 0 else 0,
                '% Band C': (result['band_freq_weighted']['C'] / total_freq * 100) if total_freq > 0 else 0,
                '% Band D': (result['band_freq_weighted']['D'] / total_freq * 100) if total_freq > 0 else 0,
                '% Special': (result['band_freq_weighted']['Special'] / total_freq * 100) if total_freq > 0 else 0,
                '% Check': (result['band_freq_weighted']['check'] / total_freq * 100) if total_freq > 0 else 0
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Band thresholds sheet
        thresholds_df = pd.DataFrame([
            {'Band': band, 'Threshold': f"â‚¦{value:,.2f}"}
            for band, value in engine.thresholds.items()
        ])
        thresholds_df.to_excel(writer, sheet_name='Thresholds', index=False)
        
        # Individual hospital sheets (first 10)
        for i, result in enumerate(successful_results[:10], 1):
            if not result['procedure_details']:
                continue
            
            proc_df = pd.DataFrame(result['procedure_details'])
            proc_df = proc_df.sort_values('price', ascending=False)
            
            # Format for export
            proc_df['Price'] = proc_df['price'].apply(lambda x: f"â‚¦{x:,.2f}")
            proc_df['Band A'] = proc_df['band_a'].apply(lambda x: f"â‚¦{x:,.2f}")
            proc_df['Band B'] = proc_df['band_b'].apply(lambda x: f"â‚¦{x:,.2f}")
            proc_df['Band C'] = proc_df['band_c'].apply(lambda x: f"â‚¦{x:,.2f}")
            proc_df['Above P90'] = proc_df['flag_above_p90'].apply(lambda x: 'YES' if x else '')
            
            export_cols = ['procedurecode', 'proceduredesc', 'Price', 'band', 'Band A', 'Band B', 'Band C', 'Above P90']
            if 'claims_p90' in proc_df.columns:
                proc_df['Claims P90'] = proc_df['claims_p90'].apply(lambda x: f"â‚¦{x:,.2f}" if pd.notna(x) else '')
                export_cols.append('Claims P90')
            
            proc_df = proc_df[export_cols]
            proc_df.columns = ['Code', 'Description', 'Price', 'Band', 'Band A', 'Band B', 'Band C', 'Above P90'] + (['Claims P90'] if 'Claims P90' in export_cols else [])
            
            sheet_name = result['hospital_name'][:31]
            proc_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    if RICH_AVAILABLE:
        console.print(f"\n[green]âœ“[/green] Excel report saved: {output_path}")
    else:
        print(f"\nâœ“ Excel report saved: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description='Hospital Band Analysis Tool - PATH 1 with DuckDB Claims Integration',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic analysis (no claims)
  python hospital_banding_duckdb.py --standard reality.csv --hospitals isalu.csv
  
  # With DuckDB claims data
  python hospital_banding_duckdb.py --standard reality.csv --hospitals hospitals/ --use-claims --output report.xlsx
  
  # Multiple hospitals with claims
  python hospital_banding_duckdb.py --standard reality.csv --hospitals h1.csv h2.csv h3.csv --use-claims
        """
    )
    
    parser.add_argument('--standard', '-s', required=True,
                       help='Path to standard tariff CSV file')
    parser.add_argument('--hospitals', '-H', nargs='+', required=True,
                       help='Path(s) to hospital tariff CSV file(s) or folder')
    parser.add_argument('--output', '-o', default=None,
                       help='Output Excel file path (default: auto-generated)')
    parser.add_argument('--use-claims', action='store_true',
                       help='Load claims data from DuckDB for risk assessment')
    parser.add_argument('--no-console', action='store_true',
                       help='Skip console output, only generate Excel')
    
    args = parser.parse_args()
    
    # Check DuckDB availability if claims requested
    if args.use_claims and not DUCKDB_AVAILABLE:
        print("ERROR: --use-claims requires duckdb to be installed")
        print("Install with: pip install duckdb")
        sys.exit(1)
    
    # Banner
    if RICH_AVAILABLE and not args.no_console:
        console.print(Panel.fit(
            "[bold cyan]Hospital Band Analysis Tool v2.1[/bold cyan]\n"
            "[dim]Dominant Band Methodology with DuckDB Claims Integration[/dim]",
            border_style="cyan"
        ))
    else:
        print("\n" + "="*60)
        print("Hospital Band Analysis Tool v2.1")
        print("DuckDB Claims Integration")
        print("="*60 + "\n")
    
    # Resolve hospital paths
    hospital_paths = []
    for path_arg in args.hospitals:
        path = Path(path_arg)
        if path.is_dir():
            csv_files = list(path.glob('*.csv'))
            hospital_paths.extend([str(f) for f in csv_files])
        elif path.exists():
            hospital_paths.append(str(path))
        else:
            print(f"Warning: Path not found: {path}")
    
    if not hospital_paths:
        print("ERROR: No valid hospital files found")
        sys.exit(1)
    
    if RICH_AVAILABLE and not args.no_console:
        console.print(f"[dim]Found {len(hospital_paths)} hospital file(s)[/dim]\n")
    else:
        print(f"Found {len(hospital_paths)} hospital file(s)\n")
    
    # Initialize engine
    engine = HospitalBandingEngine(args.standard, use_claims=args.use_claims)
    
    # Analyze hospitals
    results = engine.analyze_multiple_hospitals(hospital_paths)
    
    # Display results
    if not args.no_console:
        display_results_console(results)
    
    # Generate Excel report
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_path = f'hospital_band_analysis_{timestamp}.xlsx'
    
    create_excel_report(results, output_path, engine)
    
    # Final summary
    successful = len([r for r in results if r.get('success', False)])
    failed = len(results) - successful
    
    if RICH_AVAILABLE:
        if failed > 0:
            console.print(f"\n[yellow]âš [/yellow] {failed} hospital(s) failed analysis")
        console.print(f"[bold green]âœ“ Analysis complete![/bold green] {successful} hospitals analyzed")
    else:
        if failed > 0:
            print(f"\nâš  {failed} hospital(s) failed analysis")
        print(f"âœ“ Analysis complete! {successful} hospitals analyzed")


if __name__ == "__main__":
    main()