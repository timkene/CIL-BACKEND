#!/usr/bin/env python3
"""
Aviation Company Deep Dive Analysis - Complete Implementation
Analyzing ARIK AIR, AIR PEACE NIGERIA, and NAHCO with comprehensive metrics
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages

DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'
AVIATION_COMPANIES = {
    'ARIK AIR': 'ARIK AIR LIMITED',
    'AIR PEACE': 'AIR PEACE NIGERIA',
    'NAHCO': 'NAHCO NIGERIAN AVIATION HANDLING COMPANY PLC'
}

def connect_db():
    return duckdb.connect(DB_PATH)

def get_company_data(company_name, year):
    """Get all data for a specific company in a specific year"""
    conn = connect_db()
    
    try:
        # Cash received
        cash_query = f'''
            SELECT SUM(amount) as cash_received
            FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_{year}"
            WHERE UPPER(TRIM(groupname)) = '{company_name}'
        '''
        cash_result = conn.execute(cash_query).fetchone()
        cash_received = cash_result[0] if cash_result and cash_result[0] else 0
        
        # Claims metrics - FIXED to use correct filter
        claims_query = f'''
            SELECT 
                COUNT(DISTINCT cd.panumber) as claim_count,
                COUNT(DISTINCT cd.enrollee_id) as patient_count,
                SUM(cd.approvedamount) as total_claims,
                AVG(cd.approvedamount) as avg_claim_amount,
                COUNT(DISTINCT cd.providerid) as provider_count
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            WHERE UPPER(TRIM(g.groupname)) = '{company_name}'
            AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
        '''
        claims_result = conn.execute(claims_query).fetchone()
        
        if claims_result:
            claim_count, patient_count, total_claims, avg_claim_amount, provider_count = claims_result
        else:
            claim_count, patient_count, total_claims, avg_claim_amount, provider_count = 0, 0, 0, 0, 0
        
        # Top providers - include provider_id for Unknown hospitals
        providers_query = f'''
            SELECT 
                cd.providerid,
                COALESCE(p.providername, CONCAT('Unknown (ID: ', cd.providerid, ')')) as provider_name,
                COUNT(*) as claim_count,
                SUM(cd.approvedamount) as total_cost,
                AVG(cd.approvedamount) as avg_cost
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            LEFT JOIN "{SCHEMA}"."PROVIDERS" p ON CAST(cd.providerid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
            WHERE UPPER(TRIM(g.groupname)) = '{company_name}'
            AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
            GROUP BY cd.providerid, p.providername
            ORDER BY total_cost DESC
            LIMIT 10
        '''
        top_providers = conn.execute(providers_query).fetchdf()
        
        # Top procedures
        procedures_query = f'''
            SELECT 
                cd.code as procedure_code,
                COUNT(*) as frequency,
                SUM(cd.approvedamount) as total_cost,
                AVG(cd.approvedamount) as avg_cost
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            WHERE UPPER(TRIM(g.groupname)) = '{company_name}'
            AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
            GROUP BY cd.code
            ORDER BY total_cost DESC
            LIMIT 10
        '''
        top_procedures = conn.execute(procedures_query).fetchdf()
        
        # High utilization enrollees
        enrollees_query = f'''
            SELECT 
                cd.enrollee_id,
                COUNT(DISTINCT cd.panumber) as claim_count,
                SUM(cd.approvedamount) as total_cost
            FROM "{SCHEMA}"."CLAIMS DATA" cd
            JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
            WHERE UPPER(TRIM(g.groupname)) = '{company_name}'
            AND EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
            GROUP BY cd.enrollee_id
            HAVING COUNT(DISTINCT cd.panumber) >= 3
            ORDER BY total_cost DESC
            LIMIT 10
        '''
        top_enrollees = conn.execute(enrollees_query).fetchdf()
        
        return {
            'cash_received': cash_received,
            'claim_count': claim_count if claim_count else 0,
            'patient_count': patient_count if patient_count else 0,
            'total_claims': total_claims if total_claims else 0,
            'avg_claim_amount': avg_claim_amount if avg_claim_amount else 0,
            'provider_count': provider_count if provider_count else 0,
            'top_providers': top_providers,
            'top_procedures': top_procedures,
            'top_enrollees': top_enrollees
        }
    
    finally:
        conn.close()

def generate_complete_report():
    """Generate complete aviation company comparison report"""
    print("🛫 Generating Complete Aviation Company Analysis...")
    
        # Collect all data
    all_data = {}
    for company_short, company_full in AVIATION_COMPANIES.items():
        all_data[company_short] = {}
        for year in [2023, 2024, 2025]:
            all_data[company_short][year] = get_company_data(company_full, year)
    
    output_file = f'Aviation_Complete_Analysis_{pd.Timestamp.now().strftime("%Y%m%d")}.pdf'
    
    with PdfPages(output_file) as pdf:
        # Page 1: Executive Summary
        create_summary_page(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 2: Financial Comparison
        create_financial_charts(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 3: Utilization Analysis
        create_utilization_analysis(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 4: Provider Deep Dive
        create_provider_deep_dive(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 5: Procedure Analysis
        create_procedure_deep_dive(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 6: Enrollee Analysis
        create_enrollee_deep_dive(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 7: Year-over-Year Trends
        create_year_trends(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 8: Key Findings & Action Plan
        create_findings_and_actions(all_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
    
    print(f"✅ Complete report generated: {output_file}")
    return output_file

def create_summary_page(all_data):
    """Create executive summary"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    title = "AVIATION COMPANY COMPARATIVE ANALYSIS"
    subtitle = "ARIK AIR | AIR PEACE NIGERIA | NAHCO"
    
    ax.text(0.5, 0.96, title, ha='center', va='top', fontsize=20, fontweight='bold')
    ax.text(0.5, 0.93, subtitle, ha='center', va='top', fontsize=14, style='italic')
    
    # Calculate totals for each company
    summary_text = "COMPREHENSIVE PERFORMANCE SUMMARY (2023-2025)\n" + "="*70 + "\n\n"
    
    for company in AVIATION_COMPANIES:
        total_cash = sum(all_data[company][y]['cash_received'] for y in [2023, 2024, 2025])
        total_claims = sum(all_data[company][y]['total_claims'] for y in [2023, 2024, 2025])
        total_patients = sum(all_data[company][y]['patient_count'] for y in [2023, 2024, 2025])
        
        avg_cash_person = total_cash / total_patients if total_patients > 0 else 0
        mlr = (total_claims / total_cash * 100) if total_cash > 0 else 0
        
        summary_text += f"""
{company}:
  Total Cash Received:     ₦{total_cash:,.0f}
  Total Claims Paid:        ₦{total_claims:,.0f}
  Total Unique Patients:    {total_patients:,.0f}
  Average Cash per Person:  ₦{avg_cash_person:,.0f}
  Medical Loss Ratio (MLR): {mlr:.1f}%
  Status: {'✅ EXCELLENT' if mlr < 60 else '⚠️ WARNING' if mlr > 75 else '✅ GOOD'}
"""
    
    ax.text(0.1, 0.88, summary_text, va='top', ha='left', fontsize=9, family='monospace')
    
    # Explanation box
    explanation = """
ANALYSIS PURPOSE:
This report provides a deep-dive analysis of three aviation companies to:
• Compare premium pricing (avg cash per person)
• Identify problematic hospitals/providers driving high costs
• Detect procedure over-utilization patterns
• Analyze enrollee utilization patterns
• Track year-over-year performance trends
• Provide actionable recommendations for each company

BENCHMARKS:
• Optimal MLR: 60% or less
• Warning Threshold: 75% 
• High Risk: 80%+
• Average premium per person should align with industry standards
"""
    
    ax.text(0.1, 0.05, explanation, va='bottom', ha='left', fontsize=8.5,
            family='monospace', style='italic',
            bbox=dict(boxstyle='round', facecolor='lightcyan', alpha=0.5))
    
    plt.tight_layout()

def create_financial_charts(all_data):
    """Create financial comparison charts"""
    fig, axes = plt.subplots(2, 2, figsize=(11, 10))
    
    years = [2023, 2024, 2025]
    
    # Prepare data arrays
    cash_by_company = {c: [] for c in AVIATION_COMPANIES}
    claims_by_company = {c: [] for c in AVIATION_COMPANIES}
    mlr_by_company = {c: [] for c in AVIATION_COMPANIES}
    
    for year in years:
        for company in AVIATION_COMPANIES:
            cash = all_data[company][year]['cash_received']
            claims = all_data[company][year]['total_claims']
            mlr = (claims / cash * 100) if cash > 0 else 0
            
            cash_by_company[company].append(cash)
            claims_by_company[company].append(claims)
            mlr_by_company[company].append(mlr)
    
    # Chart 1: Premium Revenue Trends
    ax1 = axes[0, 0]
    for company in AVIATION_COMPANIES:
        ax1.plot(years, cash_by_company[company], marker='o', linewidth=2.5, label=company, markersize=8)
    ax1.set_xlabel('Year', fontsize=11)
    ax1.set_ylabel('Cash Received (₦)', fontsize=11)
    ax1.set_title('Premium Revenue Trends', fontsize=12, fontweight='bold')
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.ticklabel_format(style='plain', axis='y')
    
    # Chart 2: Claims Trends
    ax2 = axes[0, 1]
    for company in AVIATION_COMPANIES:
        ax2.plot(years, claims_by_company[company], marker='s', linewidth=2.5, label=company, markersize=8)
    ax2.set_xlabel('Year', fontsize=11)
    ax2.set_ylabel('Claims Paid (₦)', fontsize=11)
    ax2.set_title('Claims Spending Trends', fontsize=12, fontweight='bold')
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)
    ax2.ticklabel_format(style='plain', axis='y')
    
    # Chart 3: MLR Comparison
    ax3 = axes[1, 0]
    x = np.arange(len(years))
    width = 0.25
    colors = ['#3498db', '#e74c3c', '#2ecc71']
    for i, company in enumerate(AVIATION_COMPANIES):
        ax3.bar(x + i*width, mlr_by_company[company], width, label=company, color=colors[i])
    ax3.set_xlabel('Year', fontsize=11)
    ax3.set_ylabel('MLR (%)', fontsize=11)
    ax3.set_title('Medical Loss Ratio Comparison', fontsize=12, fontweight='bold')
    ax3.set_xticks(x + width)
    ax3.set_xticklabels(years)
    ax3.legend(fontsize=9)
    ax3.axhline(y=75, color='r', linestyle='--', linewidth=2, label='75% Threshold')
    ax3.axhline(y=60, color='g', linestyle='--', linewidth=1, alpha=0.5, label='Optimal 60%')
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Chart 4: Average Cash per Person (2025)
    ax4 = axes[1, 1]
    avg_cash_2025 = []
    companies_short = []
    for company in AVIATION_COMPANIES:
        data = all_data[company][2025]
        if data['patient_count'] > 0:
            avg = data['cash_received'] / data['patient_count']
            avg_cash_2025.append(avg)
            companies_short.append(company)
    
    bars = ax4.bar(range(len(companies_short)), avg_cash_2025, color=colors[:len(companies_short)])
    ax4.set_xticks(range(len(companies_short)))
    ax4.set_xticklabels(companies_short, rotation=45, ha='right')
    ax4.set_ylabel('Average Premium per Person (₦)', fontsize=11)
    ax4.set_title('2025: Premium Pricing per Enrollee', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars, avg_cash_2025):
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'₦{value:,.0f}', ha='center', va='bottom', fontsize=8, fontweight='bold')
    
    plt.suptitle('Financial Performance Comparison', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Top Left: Premium revenue growth/decline over years shows client retention and price changes | " +
             "Top Right: Claims spending trends indicate utilization patterns - increasing trends may need investigation | " +
             "Bottom Left: MLR shows profitability - above 75% is high risk, below 60% is optimal | " +
             "Bottom Right: Average premium per person in 2025 - if one company is much lower than others, may need pricing review",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_utilization_analysis(all_data):
    """Create utilization metrics analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    
    # Calculate metrics for 2025
    companies_list = []
    patients_list = []
    claims_patient = []
    avg_claim_size = []
    
    for company in AVIATION_COMPANIES:
        data = all_data[company][2025]
        if data['patient_count'] > 0:
            companies_list.append(company)
            patients_list.append(data['patient_count'])
            claims_patient.append(data['claim_count'] / data['patient_count'])
            avg_claim_size.append(data['avg_claim_amount'])
    
    # Chart 1: Patients vs Claims per Patient
    ax1 = axes[0]
    x = np.arange(len(companies_list))
    width = 0.35
    
    ax1.bar(x - width/2, [p/10 for p in patients_list], width, label='Patient Count (÷10)', color='skyblue')
    ax1.bar(x + width/2, claims_patient, width, label='Claims per Patient', color='orange')
    ax1.set_xlabel('Company', fontsize=11)
    ax1.set_ylabel('Count', fontsize=11)
    ax1.set_title('2025: Patient Count vs Utilization Rate', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(companies_list, rotation=45, ha='right')
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Chart 2: Average Claim Size
    ax2 = axes[1]
    bars = ax2.bar(range(len(companies_list)), avg_claim_size, color=['#3498db', '#e74c3c', '#2ecc71'])
    ax2.set_xlabel('Company', fontsize=11)
    ax2.set_ylabel('Average Claim Amount (₦)', fontsize=11)
    ax2.set_title('2025: Average Claim Size by Company', fontsize=12, fontweight='bold')
    ax2.set_xticks(range(len(companies_list)))
    ax2.set_xticklabels(companies_list, rotation=45, ha='right')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, value in zip(bars, avg_claim_size):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'₦{value:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    plt.suptitle('Utilization Metrics Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Top: Compares number of patients vs how many claims per patient - high claims/patient ratio indicates over-utilization | " +
             "Bottom: Shows average cost per claim - companies with very high avg claim size may have more serious/expensive procedures being used",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_provider_deep_dive(all_data):
    """Create comprehensive provider analysis for each aviation company"""
    fig, axes = plt.subplots(3, 2, figsize=(14, 16))
    fig.suptitle('Provider Deep Dive - Top Hospitals by Company (2025)', fontsize=16, fontweight='bold')
    
    for idx, (company_short, company_full) in enumerate(AVIATION_COMPANIES.items()):
        data_2025 = all_data[company_short][2025]
        providers_df = data_2025['top_providers']
        
        if not providers_df.empty and len(providers_df) > 0:
            # Top 5 providers by cost
            ax1 = axes[idx, 0]
            top_5 = providers_df.head(5)
            ax1.barh(range(len(top_5)), top_5['total_cost'] / 1e6, color='coral')
            ax1.set_yticks(range(len(top_5)))
            ax1.set_yticklabels([name[:30] for name in top_5['provider_name']], fontsize=8)
            ax1.set_xlabel('Total Cost (Millions ₦)')
            ax1.set_title(f'{company_short} - Top Hospitals by Cost')
            ax1.grid(True, alpha=0.3, axis='x')
            
            # Provider efficiency
            ax2 = axes[idx, 1]
            if len(providers_df) > 0:
                ax2.scatter(providers_df['claim_count'], providers_df['avg_cost'], 
                           s=100, alpha=0.6, color='steelblue')
                ax2.set_xlabel('Number of Claims')
                ax2.set_ylabel('Average Cost per Claim (₦)')
                ax2.set_title(f'{company_short} - Provider Efficiency')
                ax2.grid(True, alpha=0.3)
        else:
            ax1 = axes[idx, 0]
            ax2 = axes[idx, 1]
            ax1.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=12)
            ax2.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=12)
    
    plt.tight_layout(rect=[0, 0.03, 1, 0.98])
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Left charts: Top 5 hospitals by total cost for each company - identify cost drivers that may need renegotiation | " +
             "Right charts: Provider efficiency scatter plots - each dot is a hospital, position shows claim volume vs average cost - outliers may need investigation",
             ha='center', va='bottom', fontsize=7, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

def create_procedure_deep_dive(all_data):
    """Create comprehensive procedure analysis"""
    fig, axes = plt.subplots(3, 1, figsize=(11, 14))
    fig.suptitle('Procedure Deep Dive - Top Procedures by Company (2025)', fontsize=16, fontweight='bold')
    
    for idx, (company_short, company_full) in enumerate(AVIATION_COMPANIES.items()):
        ax = axes[idx]
        data_2025 = all_data[company_short][2025]
        procedures_df = data_2025['top_procedures']
        
        if not procedures_df.empty and len(procedures_df) > 0:
            top_10 = procedures_df.head(10)
            bars = ax.barh(range(len(top_10)), top_10['total_cost'] / 1e6, color='mediumpurple')
            ax.set_yticks(range(len(top_10)))
            ax.set_yticklabels([f"{row['procedure_code']}" for _, row in top_10.iterrows()], fontsize=9)
            ax.set_xlabel('Total Cost (Millions ₦)')
            ax.set_title(f'{company_short} - Top 10 Most Expensive Procedures')
            ax.grid(True, alpha=0.3, axis='x')
            
            # Add value labels
            for i, (bar, (_, row)) in enumerate(zip(bars, top_10.iterrows())):
                ax.text(bar.get_width(), bar.get_y() + bar.get_height()/2,
                       f'₦{bar.get_width():.2f}M\n({row["frequency"]}x)', 
                       ha='left', va='center', fontsize=7)
        else:
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=12)
    
    plt.tight_layout(rect=[0, 0.05, 1, 0.98])
    
    fig.text(0.5, 0.01,
             "Chart Explanation: Shows the most expensive procedures by total cost for each company. " +
             "Check if certain procedures are being used excessively - procedures used 100+ times should be reviewed for necessity.",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))

def create_enrollee_deep_dive(all_data):
    """Create high-utilization enrollee analysis"""
    fig, axes = plt.subplots(3, 1, figsize=(11, 14))
    fig.suptitle('High-Utilization Enrollee Analysis (2025)', fontsize=16, fontweight='bold')
    
    for idx, (company_short, company_full) in enumerate(AVIATION_COMPANIES.items()):
        ax = axes[idx]
        data_2025 = all_data[company_short][2025]
        enrollees_df = data_2025['top_enrollees']
        
        if not enrollees_df.empty and len(enrollees_df) > 0:
            top_10 = enrollees_df.head(10)
            bars = ax.bar(range(len(top_10)), top_10['total_cost'] / 1e6, color='crimson')
            ax.set_xticks(range(len(top_10)))
            ax.set_xticklabels([f"P{i+1}" for i in range(len(top_10))], fontsize=8)
            ax.set_ylabel('Total Cost (Millions ₦)')
            ax.set_title(f'{company_short} - Top 10 High-Cost Enrollees (Patients with 3+ Claims)')
            ax.grid(True, alpha=0.3, axis='y')
            
            # Add value labels
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'₦{height:.2f}M', ha='center', va='bottom', fontsize=7)
        else:
            ax.text(0.5, 0.5, 'No data available', ha='center', va='center', fontsize=12)
    
    plt.tight_layout(rect=[0, 0.05, 1, 0.98])
    
    fig.text(0.5, 0.01,
             "Chart Explanation: Patients with 3+ claims in 2025. High-cost patients may need: " +
             "(1) Care management review to ensure appropriate care, (2) Investigation if costs seem unreasonable, " +
             "(3) Consider if chronic conditions are being managed effectively.",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.3))

def create_year_trends(all_data):
    """Create year-over-year trend analysis"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    years = [2023, 2024, 2025]
    
    # Prepare data
    metrics_by_company = {}
    for company_short, company_full in AVIATION_COMPANIES.items():
        metrics_by_company[company_short] = {
            'cash': [all_data[company_short][y]['cash_received'] for y in years],
            'patients': [all_data[company_short][y]['patient_count'] for y in years],
            'avg_cost': [all_data[company_short][y]['avg_claim_amount'] for y in years],
            'claims_per_patient': []
        }
        
        for y in years:
            p = all_data[company_short][y]['patient_count']
            c = all_data[company_short][y]['claim_count']
            avg = c / p if p > 0 else 0
            metrics_by_company[company_short]['claims_per_patient'].append(avg)
    
    # Chart 1: Patient growth
    ax1 = axes[0, 0]
    for company_short in AVIATION_COMPANIES.keys():
        ax1.plot(years, metrics_by_company[company_short]['patients'], 
                marker='o', linewidth=2.5, label=company_short, markersize=8)
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Number of Patients')
    ax1.set_title('Patient Count Growth')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Chart 2: Average claim size trend
    ax2 = axes[0, 1]
    for company_short in AVIATION_COMPANIES.keys():
        ax2.plot(years, [m/1000 for m in metrics_by_company[company_short]['avg_cost']], 
                marker='s', linewidth=2.5, label=company_short, markersize=8)
    ax2.set_xlabel('Year')
    ax2.set_ylabel('Average Claim Size (₦000)')
    ax2.set_title('Average Claim Size Trends')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Chart 3: Claims per patient
    ax3 = axes[1, 0]
    x = np.arange(len(years))
    width = 0.25
    for i, company_short in enumerate(AVIATION_COMPANIES.keys()):
        ax3.bar(x + i*width, metrics_by_company[company_short]['claims_per_patient'], 
               width, label=company_short)
    ax3.set_xlabel('Year')
    ax3.set_ylabel('Claims per Patient')
    ax3.set_title('Utilization Rate (Claims/Patient)')
    ax3.set_xticks(x + width)
    ax3.set_xticklabels(years)
    ax3.legend()
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Chart 4: Combined cash trends
    ax4 = axes[1, 1]
    for company_short in AVIATION_COMPANIES.keys():
        ax4.plot(years, [c/1e8 for c in metrics_by_company[company_short]['cash']], 
                marker='^', linewidth=2.5, label=company_short, markersize=8)
    ax4.set_xlabel('Year')
    ax4.set_ylabel('Cash Received (₦100M)')
    ax4.set_title('Premium Revenue Trends')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.suptitle('Year-over-Year Performance Trends', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Top Left: Patient count growth - shows if enrollment is growing/declining | " +
             "Top Right: Average claim size trends - increasing means more expensive procedures or inflation | " +
             "Bottom Left: Utilization rate (claims per patient) - higher values indicate more active patients | " +
             "Bottom Right: Premium revenue trends - shows financial growth or decline",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_findings_and_actions(all_data):
    """Create key findings and action plan"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    # Calculate key metrics
    findings_text = "KEY FINDINGS & ACTIONABLE RECOMMENDATIONS\n"
    findings_text += "="*70 + "\n\n"
    
    for company_short, company_full in AVIATION_COMPANIES.items():
        total_cash = sum(all_data[company_short][y]['cash_received'] for y in [2023, 2024, 2025])
        total_claims = sum(all_data[company_short][y]['total_claims'] for y in [2023, 2024, 2025])
        total_patients = sum(all_data[company_short][y]['patient_count'] for y in [2023, 2024, 2025])
        
        avg_cash_per_person = total_cash / total_patients if total_patients > 0 else 0
        mlr = (total_claims / total_cash * 100) if total_cash > 0 else 0
        claims_per_patient = sum(all_data[company_short][y]['claim_count'] for y in [2023, 2024, 2025]) / total_patients if total_patients > 0 else 0
        
        findings_text += f"\n{company_short.upper()}:\n"
        findings_text += f"  Total Cash: ₦{total_cash:,.0f}\n"
        findings_text += f"  Total Claims: ₦{total_claims:,.0f}\n"
        findings_text += f"  MLR: {mlr:.1f}% {'⚠️ HIGH RISK' if mlr > 75 else '✅ GOOD' if mlr < 60 else '⚡ MODERATE'}\n"
        findings_text += f"  Avg Cash/Person: ₦{avg_cash_per_person:,.0f}\n"
        findings_text += f"  Claims/Patient: {claims_per_patient:.2f}\n"
        
        # Recommendations
        if mlr > 75:
            findings_text += f"  → ACTION: Urgent review needed - MLR exceeds 75% threshold\n"
        elif avg_cash_per_person < 100000:
            findings_text += f"  → ACTION: Premium pricing may be too low - consider increase\n"
        elif claims_per_patient > 5:
            findings_text += f"  → ACTION: High utilization rate - review claim patterns\n"
        
        findings_text += "\n"
    
    findings_text += "\nGENERAL RECOMMENDATIONS:\n" + "-"*70 + "\n"
    findings_text += "1. PRICING: Compare average cash per person across all three companies\n"
    findings_text += "   → If one company is significantly lower, consider price adjustment\n\n"
    findings_text += "2. PROVIDER NEGOTIATION: Top 3 hospitals driving highest costs\n"
    findings_text += "   → Renegotiate rates with these hospitals\n"
    findings_text += "   → Consider changing to lower-cost providers if quality allows\n\n"
    findings_text += "3. PROCEDURE REVIEW: Check if high-cost procedures are necessary\n"
    findings_text += "   → Implement stricter PA for expensive procedures\n"
    findings_text += "   → Review procedures used 100+ times per year\n\n"
    findings_text += "4. UTILIZATION MANAGEMENT:\n"
    findings_text += "   → Care management for high-cost enrollees\n"
    findings_text += "   → Preventive care programs to reduce claims\n"
    findings_text += "   → Telemedicine to reduce cost per visit\n\n"
    findings_text += "5. ANNUAL CONTRACT REVIEW:\n"
    findings_text += "   → Adjust premiums based on actual MLR performance\n"
    findings_text += "   → Implement MLR-based pricing if applicable\n\n"
    
    ax.text(0.1, 0.98, findings_text, va='top', ha='left', fontsize=9, family='monospace',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))
    
    plt.tight_layout()

if __name__ == '__main__':
    generate_complete_report()
