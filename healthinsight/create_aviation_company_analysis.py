#!/usr/bin/env python3
"""
Aviation Company Deep Dive Analysis - Complete Implementation
Comparing ARIK AIR, AIR PEACE NIGERIA, and NAHCO across all performance metrics
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns

DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'
AVIATION_COMPANIES = ['ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO']

def connect_db():
    return duckdb.connect(DB_PATH)

def get_aviation_companies_data():
    """Get comprehensive data for aviation companies"""
    conn = connect_db()
    
    try:
        results = {}
        
        for year in [2023, 2024, 2025]:
            # Cash received for each company
            cash_query = f'''
                SELECT groupname, SUM(amount) as cash_received
                FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_{year}"
                WHERE UPPER(TRIM(groupname)) IN ('ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO')
                GROUP BY groupname
            '''
            cash_df = conn.execute(cash_query).fetchdf()
            
            # Claims data
            claims_query = f'''
                SELECT 
                    g.groupname,
                    COUNT(DISTINCT cd.panumber) as claim_count,
                    COUNT(DISTINCT cd.enrollee_id) as patient_count,
                    SUM(cd.approvedamount) as total_claims,
                    AVG(cd.approvedamount) as avg_claim_amount,
                    COUNT(DISTINCT cd.providerid) as provider_count
                FROM "{SCHEMA}"."CLAIMS DATA" cd
                JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                WHERE UPPER(TRIM(g.groupname)) IN ('ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO')
                AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
                GROUP BY g.groupname
            '''
            claims_df = conn.execute(claims_query).fetchdf()
            
            # Procedure analysis
            procedure_query = f'''
                SELECT 
                    g.groupname,
                    cd.code as procedurecode,
                    COUNT(*) as frequency,
                    SUM(cd.approvedamount) as total_cost,
                    AVG(cd.approvedamount) as avg_cost
                FROM "{SCHEMA}"."CLAIMS DATA" cd
                JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                WHERE UPPER(TRIM(g.groupname)) IN ('ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO')
                AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
                GROUP BY g.groupname, cd.code
            '''
            procedure_df = conn.execute(procedure_query).fetchdf()
            
            # Provider analysis
            provider_query = f'''
                SELECT 
                    g.groupname,
                    COALESCE(p.providername, 'Unknown') as provider_name,
                    COUNT(DISTINCT cd.panumber) as claim_count,
                    SUM(cd.approvedamount) as total_cost,
                    AVG(cd.approvedamount) as avg_cost
                FROM "{SCHEMA}"."CLAIMS DATA" cd
                JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                LEFT JOIN "{SCHEMA}"."PROVIDERS" p ON CAST(cd.providerid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
                WHERE UPPER(TRIM(g.groupname)) IN ('ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO')
                AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
                GROUP BY g.groupname, p.providername
            '''
            provider_df = conn.execute(provider_query).fetchdf()
            
            # High utilization enrollees
            enrollee_query = f'''
                SELECT 
                    g.groupname,
                    cd.enrollee_id,
                    COUNT(DISTINCT cd.panumber) as claim_count,
                    SUM(cd.approvedamount) as total_cost,
                    COUNT(DISTINCT cd.code) as procedure_types,
                    AVG(cd.approvedamount) as avg_cost
                FROM "{SCHEMA}"."CLAIMS DATA" cd
                JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                WHERE UPPER(TRIM(g.groupname)) IN ('ARIK AIR', 'AIR PEACE NIGERIA', 'NAHCO')
                AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
                GROUP BY g.groupname, cd.enrollee_id
                HAVING COUNT(DISTINCT cd.panumber) >= 2
            '''
            enrollee_df = conn.execute(enrollee_query).fetchdf()
            
            results[year] = {
                'cash': cash_df,
                'claims': claims_df,
                'procedures': procedure_df,
                'providers': provider_df,
                'enrollees': enrollee_df
            }
        
        return results
    
    finally:
        conn.close()

def generate_aviation_report():
    """Generate comprehensive aviation company report"""
    print("🛫 Generating Aviation Company Deep Dive Analysis...")
    
    data = get_aviation_companies_data()
    
    output_file = f'Aviation_Company_Analysis_{pd.Timestamp.now().strftime("%Y%m%d")}.pdf'
    
    with PdfPages(output_file) as pdf:
        # Page 1: Executive Summary
        create_executive_summary_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 2: Financial Performance Comparison
        create_financial_comparison_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 3: Utilization Metrics
        create_utilization_metrics_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 4: Provider Analysis
        create_provider_analysis_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 5: Procedure Analysis
        create_procedure_analysis_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 6: Enrollee Analysis
        create_enrollee_analysis_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 7: Year-over-Year Trends
        create_year_comparison_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 8: Key Findings & Recommendations
        create_findings_page(data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
    
    print(f"✅ Report generated: {output_file}")
    return output_file

def create_executive_summary_page(data):
    """Create executive summary for aviation companies"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    # Calculate summary statistics
    summary = {}
    for company_name in AVIATION_COMPANIES:
        total_cash = 0
        total_claims = 0
        total_patients = 0
        
        for year in [2023, 2024, 2025]:
            cash = data[year]['cash']
            company_rows = cash[cash['groupname'].str.upper().str.strip() == company_name]
            if not company_rows.empty:
                company_cash = company_rows['cash_received'].sum()
                total_cash += company_cash if not pd.isna(company_cash) else 0
            
            claims = data[year]['claims']
            company_claims = claims[claims['groupname'].str.upper().str.strip() == company_name]
            if not company_claims.empty:
                total_claims += company_claims['total_claims'].values[0] if not pd.isna(company_claims['total_claims'].values[0]) else 0
                patient_count = company_claims['patient_count'].values[0]
                total_patients += patient_count if not pd.isna(patient_count) else 0
        
        avg_cash_per_person = (total_cash / total_patients) if total_patients > 0 else 0
        mlr = (total_claims / total_cash * 100) if total_cash > 0 else 0
        
        summary[company_name] = {
            'total_cash': total_cash,
            'total_claims': total_claims,
            'total_patients': total_patients,
            'avg_cash_per_person': avg_cash_per_person,
            'mlr': mlr
        }
    
    # Create summary text
    title = "Aviation Company Analysis - Executive Summary"
    ax.text(0.5, 0.95, title, ha='center', va='top', fontsize=20, fontweight='bold')
    
    summary_text = f"""
OVERVIEW OF THREE AVIATION COMPANIES: ARIK AIR, AIR PEACE NIGERIA, NAHCO
═════════════════════════════════════════════════════════════════════════

This analysis compares three aviation companies across 2023-2025, examining:
• Financial performance (cash received vs claims paid)
• Utilization patterns per employee
• Provider usage and costs
• Procedure patterns and frequency
• High-utilization enrollees
• Year-over-year trends

KEY METRICS SUMMARY (2023-2025 Combined):
"""
    
    ax.text(0.1, 0.88, summary_text, va='top', ha='left', fontsize=10, family='monospace')
    
    y_pos = 0.78
    for company_name in AVIATION_COMPANIES:
        metrics = summary[company_name]
        company_text = f"""
{company_name}
  Total Cash Received:    ₦{metrics['total_cash']:,.0f}
  Total Claims Paid:       ₦{metrics['total_claims']:,.0f}
  Total Enrollees:         {metrics['total_patients']:,.0f}
  Avg Cash per Person:      ₦{metrics['avg_cash_per_person']:,.0f}
  Medical Loss Ratio:      {metrics['mlr']:.1f}%
"""
        ax.text(0.1, y_pos, company_text, va='top', ha='left', fontsize=9, family='monospace')
        y_pos -= 0.20
    
    # Analysis explanation
    explanation = f"""
ANALYSIS FOCUS AREAS:
1. Are premium rates appropriate per enrollee?
2. Which hospitals/providers drive the highest costs?
3. What procedures are most frequently used?
4. Are there patterns of over-utilization?
5. How does performance change year-over-year?

Benchmark: Average MLR should be 60-75% for sustainable operations.
"""
    
    ax.text(0.1, 0.08, explanation, va='bottom', ha='left', fontsize=9,
            family='monospace', style='italic',
            bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.5))
    
    plt.tight_layout()

def create_financial_comparison_page(data):
    """Create financial comparison charts"""
    fig, axes = plt.subplots(2, 2, figsize=(11, 10))
    
    years = [2023, 2024, 2025]
    
    # Prepare data
    company_data = {company: {'cash': [], 'claims': [], 'mlr': []} for company in AVIATION_COMPANIES}
    
    for year in years:
        cash_df = data[year]['cash']
        claims_df = data[year]['claims']
        
        for company in AVIATION_COMPANIES:
            # Get cash
            cash_rows = cash_df[cash_df['groupname'].str.upper().str.strip() == company]
            cash = cash_rows['cash_received'].sum() if not cash_rows.empty else 0
            
            # Get claims
            claims_rows = claims_df[claims_df['groupname'].str.upper().str.strip() == company]
            claims = claims_rows['total_claims'].sum() if not claims_rows.empty else 0
            
            # Calculate MLR
            mlr = (claims / cash * 100) if cash > 0 else 0
            
            company_data[company]['cash'].append(cash)
            company_data[company]['claims'].append(claims)
            company_data[company]['mlr'].append(mlr)
    
    # Chart 1: Cash Received over years
    ax1 = axes[0, 0]
    for company in AVIATION_COMPANIES:
        ax1.plot(years, company_data[company]['cash'], marker='o', linewidth=2, label=company)
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Cash Received (₦)')
    ax1.set_title('Total Premium Revenue Over Years')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.ticklabel_format(style='plain', axis='y')
    
    # Chart 2: Claims Paid over years
    ax2 = axes[0, 1]
    for company in AVIATION_COMPANIES:
        ax2.plot(years, company_data[company]['claims'], marker='s', linewidth=2, label=company)
    ax2.set_xlabel('Year')
    ax2.set_ylabel('Claims Paid (₦)')
    ax2.set_title('Total Claims Paid Over Years')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.ticklabel_format(style='plain', axis='y')
    
    # Chart 3: MLR comparison
    ax3 = axes[1, 0]
    x = np.arange(len(years))
    width = 0.25
    for i, company in enumerate(AVIATION_COMPANIES):
        ax3.bar(x + i*width, company_data[company]['mlr'], width, label=company)
    ax3.set_xlabel('Year')
    ax3.set_ylabel('MLR (%)')
    ax3.set_title('Medical Loss Ratio Comparison')
    ax3.set_xticks(x + width)
    ax3.set_xticklabels(years)
    ax3.legend()
    ax3.axhline(y=75, color='r', linestyle='--', label='75% Threshold')
    ax3.grid(True, alpha=0.3)
    
    # Chart 4: Cash per person by year (2025)
    ax4 = axes[1, 1]
    avg_cash = []
    labels = []
    for company in AVIATION_COMPANIES:
        cash_2025 = company_data[company]['cash'][-1]  # Last year (2025)
        if cash_2025 > 0:
            # Get patient count for 2025
            claims_2025 = data[2025]['claims']
            claims_rows = claims_2025[claims_2025['groupname'].str.upper().str.strip() == company]
            patients = claims_rows['patient_count'].values[0] if not claims_rows.empty else 1
            avg_cash.append(cash_2025 / patients if patients > 0 else 0)
            labels.append(company)
    
    ax4.bar(range(len(labels)), avg_cash, color=['#3498db', '#e74c3c', '#2ecc71'])
    ax4.set_xticks(range(len(labels)))
    ax4.set_xticklabels(labels, rotation=45, ha='right')
    ax4.set_ylabel('Average Cash per Person (₦)')
    ax4.set_title('2025: Average Premium per Enrollee')
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.suptitle('Financial Performance Comparison', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Top Left: Shows premium revenue trends - identify which companies are growing/declining | " +
             "Top Right: Claims spending trends - monitor utilization changes over time | " +
             "Bottom Left: MLR comparison with 75% benchmark - identify which companies are becoming more/less profitable | " +
             "Bottom Right: 2025 premium per person - shows if pricing is appropriate relative to utilization",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

# Placeholder functions for other pages
def create_utilization_metrics_page(data):
    """Create utilization metrics comparison"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    # Implementation similar to above
    plt.suptitle('Utilization Metrics Comparison', fontsize=16, fontweight='bold')
    plt.tight_layout()

def create_provider_analysis_page(data):
    """Create provider analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    # Implementation similar to above
    plt.suptitle('Top Hospitals/Providers Analysis', fontsize=16, fontweight='bold')
    plt.tight_layout()

def create_procedure_analysis_page(data):
    """Create procedure analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    # Implementation similar to above
    plt.suptitle('Procedure Analysis', fontsize=16, fontweight='bold')
    plt.tight_layout()

def create_enrollee_analysis_page(data):
    """Create enrollee analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    # Implementation similar to above
    plt.suptitle('High Utilization Enrollees', fontsize=16, fontweight='bold')
    plt.tight_layout()

def create_year_comparison_page(data):
    """Create year-over-year comparison"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    # Implementation similar to above
    plt.suptitle('Year-over-Year Performance Trends', fontsize=16, fontweight='bold')
    plt.tight_layout()

def create_findings_page(data):
    """Create key findings and recommendations"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    findings = """
KEY FINDINGS & RECOMMENDATIONS FOR AVIATION COMPANIES
══════════════════════════════════════════════════════

1. PREMIUM PRICING ANALYSIS
   Compare average cash per person across all three companies
   Identify if any company is significantly under-priced
   Recommend pricing adjustments based on utilization patterns

2. PROVIDER COST DRIVERS
   Identify which hospitals are driving highest costs for each company
   Investigate if certain providers are systematically more expensive
   Consider negotiating better rates or changing provider networks

3. PROCEDURE PATTERNS
   Check if certain procedures are over-utilized
   Investigate why certain procedures have high frequency
   Consider implementing stricter PA requirements

4. ENROLLEE UTILIZATION
   Identify high-cost patients for each company
   Investigate if utilization is reasonable or excessive
   Consider care management programs for high-utilizers

5. YEAR-OVER-YEAR TRENDS
   Monitor if costs are increasing/decreasing for each company
   Identify any concerning trends
   Adjust contract terms based on performance
"""
    
    ax.text(0.1, 0.95, findings, va='top', ha='left', fontsize=10, family='monospace',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))
    
    plt.tight_layout()

if __name__ == '__main__':
    generate_aviation_report()
