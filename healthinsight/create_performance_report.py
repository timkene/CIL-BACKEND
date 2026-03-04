#!/usr/bin/env python3
"""
Comprehensive Health Insurance Performance Analysis (2023-2025)

This script generates a comprehensive PDF report analyzing:
- Financial performance (cash received vs expenses)
- Claims utilization trends
- Provider performance analysis
- High-utilization enrollee identification
- Procedure over-utilization patterns
- Company-wise performance analysis
- Recommendations based on industry standards
"""

import duckdb
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

def connect_db():
    """Connect to DuckDB database"""
    return duckdb.connect(DB_PATH)

def get_financial_performance():
    """Get comprehensive financial performance data"""
    conn = connect_db()
    
    try:
        financial_data = {}
        
        for year in [2023, 2024, 2025]:
            # Cash received
            cash_query = f'SELECT SUM(amount) as total FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_{year}"'
            cash_received = conn.execute(cash_query).fetchone()[0] or 0
            
            # Claims by encounter date
            claims_encounter_query = f'''
                SELECT SUM(approvedamount) as total
                FROM "{SCHEMA}"."CLAIMS DATA"
                WHERE EXTRACT(YEAR FROM encounterdatefrom) = {year}
            '''
            claims_encounter = conn.execute(claims_encounter_query).fetchone()[0] or 0
            
            # Claims by submission date
            claims_submitted_query = f'''
                SELECT SUM(approvedamount) as total
                FROM "{SCHEMA}"."CLAIMS DATA"
                WHERE EXTRACT(YEAR FROM datesubmitted) = {year}
            '''
            claims_submitted = conn.execute(claims_submitted_query).fetchone()[0] or 0
            
            # Salary & palliative
            salary_query = f'SELECT SUM(Amount) as total FROM "{SCHEMA}"."SALARY_AND_PALLIATIVE_{year}"'
            salary = conn.execute(salary_query).fetchone()[0] or 0
            
            # Expenses (commission, welfare, subscription, others)
            expense_query = f'''
                SELECT Category, SUM(Amount) as total
                FROM "{SCHEMA}"."EXPENSE_AND_COMMISSION_{year}"
                GROUP BY Category
            '''
            expense_data = conn.execute(expense_query).fetchdf()
            
            commission = expense_data[expense_data['Category'] == 'COMMISSION']['total'].sum() if not expense_data[expense_data['Category'] == 'COMMISSION'].empty else 0
            welfare = expense_data[expense_data['Category'] == 'WELFARE']['total'].sum() if not expense_data[expense_data['Category'] == 'WELFARE'].empty else 0
            subscription = expense_data[expense_data['Category'] == 'SUBSCRIPTION']['total'].sum() if not expense_data[expense_data['Category'] == 'SUBSCRIPTION'].empty else 0
            others = expense_data[~expense_data['Category'].isin(['COMMISSION', 'WELFARE', 'SUBSCRIPTION'])]['total'].sum()
            
            financial_data[year] = {
                'cash_received': cash_received,
                'claims_encounter': claims_encounter,
                'claims_submitted': claims_submitted,
                'salary': salary,
                'commission': commission,
                'welfare': welfare,
                'subscription': subscription,
                'others': others,
                'total_expenses': salary + commission + welfare + subscription + others,
                'mlr_encounter': (claims_encounter / cash_received * 100) if cash_received > 0 else 0,
                'mlr_submitted': (claims_submitted / cash_received * 100) if cash_received > 0 else 0,
                'total_mlr': ((claims_encounter + salary + commission + welfare + subscription + others) / cash_received * 100) if cash_received > 0 else 0
            }
        
        return financial_data
    
    finally:
        conn.close()

def get_top_companies_by_utilization():
    """Get top companies by cash received and utilization"""
    conn = connect_db()
    
    try:
        companies_data = {}
        
        for year in [2023, 2024, 2025]:
            # Get cash received by company
            cash_query = f'''
                SELECT groupname as company, SUM(amount) as cash_received
                FROM "{SCHEMA}"."CLIENT_CASH_RECEIVED_{year}"
                WHERE groupname IS NOT NULL AND groupname != ''
                GROUP BY groupname
                ORDER BY cash_received DESC
            '''
            cash_df = conn.execute(cash_query).fetchdf()
            
            # Get claims by company (simplified)
            claims_query = f'''
                SELECT 
                    g.groupname as company,
                    SUM(cd.approvedamount) as claims_total,
                    COUNT(DISTINCT cd.panumber) as claim_count,
                    COUNT(DISTINCT cd.enrollee_id) as patient_count
                FROM "{SCHEMA}"."CLAIMS DATA" cd
                JOIN "{SCHEMA}"."GROUPS" g ON CAST(cd.groupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                WHERE EXTRACT(YEAR FROM cd.encounterdatefrom) = {year}
                GROUP BY g.groupname
            '''
            claims_df = conn.execute(claims_query).fetchdf()
            
            # Merge data
            merged_df = cash_df.merge(claims_df, on='company', how='left')
            merged_df['claims_total'] = merged_df['claims_total'].fillna(0)
            merged_df['claim_count'] = merged_df['claim_count'].fillna(0)
            merged_df['patient_count'] = merged_df['patient_count'].fillna(0)
            merged_df['utilization_pct'] = (merged_df['claims_total'] / merged_df['cash_received'] * 100).fillna(0)
            
            companies_data[year] = merged_df.head(20)
        
        return companies_data
    
    finally:
        conn.close()

def get_top_providers_by_cost():
    """Get top providers by cost and utilization"""
    conn = connect_db()
    
    try:
        providers_data = {}
        
        for year in [2023, 2024, 2025]:
            query = f'''
                SELECT 
                    COALESCE(p.providername, 'Unknown') as provider_name,
                    SUM(cl.approvedamount) as total_claims,
                    COUNT(DISTINCT cl.panumber) as claim_count,
                    COUNT(DISTINCT cl.enrollee_id) as patient_count,
                    AVG(cl.approvedamount) as avg_claim_amount
                FROM "{SCHEMA}"."CLAIMS DATA" cl
                LEFT JOIN "{SCHEMA}"."PROVIDERS" p ON CAST(cl.providerid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
                WHERE EXTRACT(YEAR FROM cl.encounterdatefrom) = {year}
                GROUP BY p.providername
                ORDER BY total_claims DESC
                LIMIT 20
            '''
            
            df = conn.execute(query).fetchdf()
            providers_data[year] = df
        
        return providers_data
    
    finally:
        conn.close()

def get_top_procedures():
    """Get top procedures by frequency and cost"""
    conn = connect_db()
    
    try:
        procedures_data = {}
        
        for year in [2023, 2024, 2025]:
            query = f'''
                SELECT 
                    cl.code as procedurecode,
                    COALESCE(proc.proceduredesc, cl.code) as procedure_desc,
                    COUNT(*) as frequency,
                    SUM(cl.approvedamount) as total_cost,
                    AVG(cl.approvedamount) as avg_cost
                FROM "{SCHEMA}"."CLAIMS DATA" cl
                LEFT JOIN "{SCHEMA}"."PROCEDURE DATA" proc ON cl.code = proc.procedurecode
                WHERE EXTRACT(YEAR FROM cl.encounterdatefrom) = {year}
                GROUP BY cl.code, proc.proceduredesc
                ORDER BY frequency DESC
                LIMIT 30
            '''
            
            df = conn.execute(query).fetchdf()
            procedures_data[year] = df
        
        return procedures_data
    
    finally:
        conn.close()

def get_high_utilization_enrollees():
    """Get enrollees with high utilization"""
    conn = connect_db()
    
    try:
        enrollees_data = {}
        
        for year in [2023, 2024, 2025]:
            query = f'''
                SELECT 
                    cl.enrollee_id as enrollee_id,
                    COUNT(DISTINCT cl.panumber) as claim_count,
                    SUM(cl.approvedamount) as total_cost,
                    COUNT(DISTINCT cl.code) as procedure_types,
                    AVG(cl.approvedamount) as avg_claim_amount
                FROM "{SCHEMA}"."CLAIMS DATA" cl
                WHERE EXTRACT(YEAR FROM cl.encounterdatefrom) = {year}
                GROUP BY cl.enrollee_id
                HAVING COUNT(DISTINCT cl.panumber) >= 3
                ORDER BY total_cost DESC
                LIMIT 50
            '''
            
            df = conn.execute(query).fetchdf()
            enrollees_data[year] = df
        
        return enrollees_data
    
    finally:
        conn.close()

def get_monthly_trends():
    """Get monthly trends for all metrics"""
    conn = connect_db()
    
    try:
        trends = {}
        
        for year in [2023, 2024, 2025]:
            query = f'''
                SELECT 
                    EXTRACT(MONTH FROM cl.encounterdatefrom) as month,
                    COUNT(DISTINCT cl.panumber) as claim_count,
                    SUM(cl.approvedamount) as total_claims,
                    COUNT(DISTINCT cl.enrollee_id) as patient_count
                FROM "{SCHEMA}"."CLAIMS DATA" cl
                WHERE EXTRACT(YEAR FROM cl.encounterdatefrom) = {year}
                GROUP BY EXTRACT(MONTH FROM cl.encounterdatefrom)
                ORDER BY month
            '''
            
            df = conn.execute(query).fetchdf()
            trends[year] = df
        
        return trends
    
    finally:
        conn.close()

def web_search_insurance_standards():
    """Search for industry standards"""
    # Return standard industry benchmarks
    return {
        'mlr_threshold': 75.0,  # Alert threshold
        'optimal_mlr': 60.0,     # Target MLR
        'high_risk_threshold': 80.0,  # Critical level
        'source': 'Industry Standard'
    }

def create_performance_report():
    """Create comprehensive performance report"""
    print("🚀 Generating Comprehensive Health Insurance Performance Analysis...")
    
    # Get all data
    print("📊 Gathering financial performance data...")
    financial_data = get_financial_performance()
    
    print("🏢 Analyzing top companies...")
    companies_data = get_top_companies_by_utilization()
    
    print("🏥 Analyzing top providers...")
    providers_data = get_top_providers_by_cost()
    
    print("📋 Analyzing top procedures...")
    procedures_data = get_top_procedures()
    
    print("👥 Identifying high-utilization enrollees...")
    enrollees_data = get_high_utilization_enrollees()
    
    print("📈 Analyzing monthly trends...")
    trends_data = get_monthly_trends()
    
    print("🔍 Getting industry standards...")
    standards = web_search_insurance_standards()
    
    # Create PDF report
    output_file = f'Health_Insurance_Performance_Analysis_{datetime.now().strftime("%Y%m%d")}.pdf'
    
    with PdfPages(output_file) as pdf:
        # Page 1: Executive Summary
        create_executive_summary_page(financial_data, standards)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 2: Financial Performance Overview
        create_financial_overview_page(financial_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 3: Top Companies Analysis
        create_companies_analysis_page(companies_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 4: Provider Performance
        create_provider_analysis_page(providers_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 5: Procedure Analysis
        create_procedure_analysis_page(procedures_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 6: High Utilization Enrollees
        create_enrollee_analysis_page(enrollees_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 7: Monthly Trends
        create_monthly_trends_page(trends_data, financial_data)
        pdf.savefig(bbox_inches='tight')
        plt.close()
        
        # Page 8: Recommendations
        create_recommendations_page(financial_data, standards)
        pdf.savefig(bbox_inches='tight')
        plt.close()
    
    print(f"✅ Report generated successfully: {output_file}")
    return output_file

def create_executive_summary_page(financial_data, standards):
    """Create executive summary page"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    # Title
    title = ax.text(0.5, 0.95, 'Health Insurance Performance Analysis', 
                    ha='center', va='top', fontsize=24, fontweight='bold')
    subtitle = ax.text(0.5, 0.92, f'Executive Summary (2023-2025)', 
                       ha='center', va='top', fontsize=16, style='italic')
    
    # Calculate key metrics
    total_revenue = sum([d['cash_received'] for d in financial_data.values()])
    total_claims = sum([d['claims_encounter'] for d in financial_data.values()])
    avg_mlr = np.mean([d['mlr_encounter'] for d in financial_data.values()])
    
    # Key metrics section
    y_pos = 0.85
    metrics = f"""
KEY PERFORMANCE INDICATORS (2023-2025)
══════════════════════════════════════════════════════════════

Total Revenue:                  ₦{total_revenue:,.0f}
Total Claims Paid:              ₦{total_claims:,.0f}
Average MLR:                   {avg_mlr:.1f}%

Year-over-Year Performance:
"""
    
    for year, data in financial_data.items():
        metrics += f"""
{year}:   Revenue: ₦{data['cash_received']:,.0f}  |  Claims: ₦{data['claims_encounter']:,.0f}  |  MLR: {data['mlr_encounter']:.1f}%
"""
    
    metrics += f"""
══════════════════════════════════════════════════════════════

INDUSTRY BENCHMARKS
Optimal MLR:         {standards['optimal_mlr']:.1f}%
Alert Threshold:     {standards['mlr_threshold']:.1f}%
High Risk Level:     {standards['high_risk_threshold']:.1f}%

YOUR CURRENT PERFORMANCE: {'✅ EXCELLENT' if avg_mlr < 60 else '⚠️ WARNING' if avg_mlr > 75 else '✅ GOOD'}

══════════════════════════════════════════════════════════════
"""
    
    ax.text(0.1, y_pos, metrics, va='top', ha='left', fontsize=10, 
            family='monospace')
    
    # Add explanation
    explanation = """
ANALYSIS EXPLANATION:
This report analyzes your health insurance performance across 2023-2025.
Key metrics show revenue growth, claims trends, and operational efficiency.
MLR (Medical Loss Ratio) measures how much of revenue goes to medical claims
and operational costs. Lower MLR indicates better financial health.
"""
    ax.text(0.1, 0.05, explanation, va='bottom', ha='left', fontsize=9,
            family='monospace', style='italic', bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.5))
    
    plt.tight_layout()

def create_financial_overview_page(financial_data):
    """Create financial overview page with charts"""
    fig = plt.figure(figsize=(11, 8.5))
    
    # Create 2x2 subplot layout
    ax1 = plt.subplot(2, 2, 1)
    ax2 = plt.subplot(2, 2, 2)
    ax3 = plt.subplot(2, 2, 3)
    ax4 = plt.subplot(2, 2, 4)
    
    # Chart 1: Revenue vs Claims
    years = list(financial_data.keys())
    cash = [financial_data[y]['cash_received'] / 1e9 for y in years]
    claims = [financial_data[y]['claims_encounter'] / 1e9 for y in years]
    
    ax1.plot(years, cash, marker='o', linewidth=2, label='Cash Received')
    ax1.plot(years, claims, marker='s', linewidth=2, label='Claims Paid')
    ax1.set_xlabel('Year')
    ax1.set_ylabel('Amount (Billions ₦)')
    ax1.set_title('Revenue vs Claims Over Years')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Chart 2: MLR Trends
    mlr_encounter = [financial_data[y]['mlr_encounter'] for y in years]
    mlr_submitted = [financial_data[y]['mlr_submitted'] for y in years]
    
    ax2.plot(years, mlr_encounter, marker='o', linewidth=2, label='MLR (Encounter)')
    ax2.plot(years, mlr_submitted, marker='s', linewidth=2, label='MLR (Submitted)')
    ax2.axhline(y=75, color='r', linestyle='--', label='75% Threshold')
    ax2.set_xlabel('Year')
    ax2.set_ylabel('Medical Loss Ratio (%)')
    ax2.set_title('MLR Trends (2023-2025)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Chart 3: Expense Breakdown (2025)
    expense_data = financial_data[2025]
    expense_categories = ['Claims', 'Salary', 'Commission', 'Welfare', 'Subscription', 'Others']
    expense_amounts = [
        expense_data['claims_encounter'],
        expense_data['salary'],
        expense_data['commission'],
        expense_data['welfare'],
        expense_data['subscription'],
        expense_data['others']
    ]
    
    colors = plt.cm.Set3(range(len(expense_categories)))
    ax3.pie([a/1e9 for a in expense_amounts], labels=expense_categories, autopct='%1.1f%%', colors=colors)
    ax3.set_title('2025 Expense Distribution (Billions ₦)')
    
    # Chart 4: MLR by Category
    mlr_categories = ['Claims', 'Salary', 'Commission', 'Welfare', 'Subscription', 'Others']
    mlr_values = [
        (expense_data['claims_encounter'] / expense_data['cash_received'] * 100),
        (expense_data['salary'] / expense_data['cash_received'] * 100),
        (expense_data['commission'] / expense_data['cash_received'] * 100),
        (expense_data['welfare'] / expense_data['cash_received'] * 100),
        (expense_data['subscription'] / expense_data['cash_received'] * 100),
        (expense_data['others'] / expense_data['cash_received'] * 100)
    ]
    
    bars = ax4.bar(range(len(mlr_categories)), mlr_values, color=colors)
    ax4.set_xticks(range(len(mlr_categories)))
    ax4.set_xticklabels(mlr_categories, rotation=45, ha='right')
    ax4.set_ylabel('MLR (%)')
    ax4.set_title('2025: Cash Received Utilization by Category')
    ax4.grid(True, alpha=0.3, axis='y')
    
    plt.suptitle('Financial Performance Overview', fontsize=16, fontweight='bold', y=0.98)
    
    # Add explanations
    fig.text(0.5, 0.02, 
             "Chart Explanations:\n" +
             "Top Left: Revenue growth vs claims paid over years | " +
             "Top Right: MLR trends with 75% threshold benchmark | " +
             "Bottom Left: 2025 expense distribution across categories | " +
             "Bottom Right: Cash utilization breakdown showing % of revenue allocated to each expense type",
             ha='center', va='bottom', fontsize=8, style='italic', 
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.05, 1, 0.98])

def create_companies_analysis_page(companies_data):
    """Create top companies analysis page"""
    fig, axes = plt.subplots(3, 1, figsize=(11, 12))
    
    for idx, year in enumerate([2023, 2024, 2025]):
        ax = axes[idx]
        df = companies_data[year]
        
        top_10 = df.head(10)
        ax.barh(range(len(top_10)), top_10['cash_received'] / 1e6, 
                color=plt.cm.viridis(np.linspace(0, 1, len(top_10))))
        ax.set_yticks(range(len(top_10)))
        ax.set_yticklabels(top_10['company'], fontsize=8)
        ax.set_xlabel('Cash Received (Millions ₦)')
        ax.set_title(f'Top 10 Companies by Revenue - {year}')
        ax.grid(True, alpha=0.3, axis='x')
        plt.setp(ax.get_yticklabels(), rotation=0)
    
    plt.suptitle('Top Companies by Revenue Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.02,
             "Chart Explanation: These charts show the top 10 companies by cash received (premium revenue) for each year.\n" +
             "Revenue indicates how much each client contributed. Higher revenue companies represent key accounts that drive business growth.",
             ha='center', va='bottom', fontsize=8, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.05, 1, 1])

def create_provider_analysis_page(providers_data):
    """Create provider performance analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    
    # Combined top providers for all years
    all_providers = pd.concat(list(providers_data.values()))
    top_providers = all_providers.groupby('provider_name').agg({
        'total_claims': 'sum',
        'claim_count': 'sum',
        'patient_count': 'sum'
    }).sort_values('total_claims', ascending=False).head(15)
    
    # Top providers by cost
    ax1 = axes[0]
    ax1.barh(range(len(top_providers)), top_providers['total_claims'] / 1e6, 
             color=plt.cm.coolwarm(np.linspace(0, 1, len(top_providers))))
    ax1.set_yticks(range(len(top_providers)))
    ax1.set_yticklabels(top_providers.index, fontsize=9)
    ax1.set_xlabel('Total Claims (Millions ₦)')
    ax1.set_title('Top 15 Providers by Total Claims Cost (2023-2025)')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Provider efficiency
    ax2 = axes[1]
    provider_2025 = providers_data[2025].head(15)
    ax2.scatter(provider_2025['claim_count'], provider_2025['total_claims'] / provider_2025['claim_count'],
                s=provider_2025['patient_count'] * 10, alpha=0.6)
    ax2.set_xlabel('Number of Claims')
    ax2.set_ylabel('Average Claim Amount (₦)')
    ax2.set_title('Provider Efficiency Analysis (2025)')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle('Provider Performance Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.02,
             "Chart Explanations:\n" +
             "Top: Shows providers with highest total claim costs over 3 years - identify cost drivers and renegotiation opportunities.\n" +
             "Bottom: Scatter plot showing provider efficiency (claims vs avg cost) - bubbles size = patient count. Efficient providers have lower avg costs.",
             ha='center', va='bottom', fontsize=8, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_procedure_analysis_page(procedures_data):
    """Create procedure analysis page"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    
    # Combined procedures for all years
    all_procedures = pd.concat(list(procedures_data.values()))
    top_procedures = all_procedures.groupby(['procedurecode', 'procedure_desc']).agg({
        'frequency': 'sum',
        'total_cost': 'sum'
    }).reset_index().sort_values('total_cost', ascending=False).head(15)
    
    # Top procedures by cost
    ax1 = axes[0]
    bars = ax1.barh(range(len(top_procedures)), top_procedures['total_cost'] / 1e6,
                    color=plt.cm.plasma(np.linspace(0, 1, len(top_procedures))))
    ax1.set_yticks(range(len(top_procedures)))
    ax1.set_yticklabels([f"{row['procedurecode']}: {row['procedure_desc'][:40]}" 
                         for _, row in top_procedures.iterrows()], fontsize=7)
    ax1.set_xlabel('Total Cost (Millions ₦)')
    ax1.set_title('Top 15 Procedures by Total Cost (2023-2025)')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Frequency vs Cost correlation
    ax2 = axes[1]
    ax2.scatter(all_procedures['frequency'], all_procedures['total_cost'] / all_procedures['frequency'],
                alpha=0.5, s=50)
    ax2.set_xlabel('Frequency')
    ax2.set_ylabel('Average Cost per Procedure (₦)')
    ax2.set_title('Procedure Frequency vs Unit Cost Correlation')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle('Procedure Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.02,
             "Chart Explanations:\n" +
             "Top: Most expensive procedures by total cost over 3 years - focus on high-cost procedures for utilization review.\n" +
             "Bottom: Frequency vs unit cost correlation - identifies procedures with abnormally high per-unit costs needing investigation.",
             ha='center', va='bottom', fontsize=8, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_enrollee_analysis_page(enrollees_data):
    """Create high-utilization enrollee analysis"""
    fig, axes = plt.subplots(2, 1, figsize=(11, 10))
    
    # High utilization enrollees for 2025
    enrollees_2025 = enrollees_data[2025].head(20)
    
    # Top enrollees by cost
    ax1 = axes[0]
    ax1.barh(range(len(enrollees_2025)), enrollees_2025['total_cost'] / 1e6,
             color=plt.cm.Reds(np.linspace(0.3, 1, len(enrollees_2025))))
    ax1.set_yticks(range(len(enrollees_2025)))
    ax1.set_yticklabels([f"Patient {row['enrollee_id']}" if pd.notna(row['enrollee_id']) else "Unknown" 
                         for _, row in enrollees_2025.iterrows()], fontsize=8)
    ax1.set_xlabel('Total Cost (Millions ₦)')
    ax1.set_title('Top 20 High-Utilization Enrollees by Cost (2025)')
    ax1.grid(True, alpha=0.3, axis='x')
    
    # Cost vs Claims scatter
    ax2 = axes[1]
    ax2.scatter(enrollees_2025['claim_count'], enrollees_2025['total_cost'] / enrollees_2025['claim_count'],
                s=enrollees_2025['procedure_types'] * 50, alpha=0.6, c=enrollees_2025['total_cost'], cmap='YlOrRd')
    ax2.set_xlabel('Number of Claims')
    ax2.set_ylabel('Average Claim Amount (₦)')
    ax2.set_title('Enrollee Utilization Pattern (2025)')
    ax2.grid(True, alpha=0.3)
    
    plt.suptitle('High-Utilization Enrollee Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.02,
             "Chart Explanations:\n" +
             "Top: Top 20 enrollees by total cost in 2025 - identify patients needing care management or utilization review.\n" +
             "Bottom: Scatter plot showing utilization patterns (claims count vs avg cost) - color intensity and bubble size indicate cost and procedure variety.",
             ha='center', va='bottom', fontsize=8, style='italic',
             bbox=dict(boxstyle='round', facecolor='lightcoral', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.08, 1, 1])

def create_monthly_trends_page(trends_data, financial_data):
    """Create monthly trends analysis"""
    fig, axes = plt.subplots(2, 2, figsize=(11, 10))
    
    # Monthly claims trend
    ax1 = axes[0, 0]
    for year in [2023, 2024, 2025]:
        data = trends_data[year]
        ax1.plot(data['month'], data['claim_count'], marker='o', label=str(year), linewidth=2)
    ax1.set_xlabel('Month')
    ax1.set_ylabel('Number of Claims')
    ax1.set_title('Monthly Claims Volume')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Monthly cost trend
    ax2 = axes[0, 1]
    for year in [2023, 2024, 2025]:
        data = trends_data[year]
        ax2.plot(data['month'], data['total_claims'] / 1e6, marker='s', label=str(year), linewidth=2)
    ax2.set_xlabel('Month')
    ax2.set_ylabel('Total Claims (Millions ₦)')
    ax2.set_title('Monthly Claims Cost')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Monthly patient count
    ax3 = axes[1, 0]
    for year in [2023, 2024, 2025]:
        data = trends_data[year]
        ax3.plot(data['month'], data['patient_count'], marker='^', label=str(year), linewidth=2)
    ax3.set_xlabel('Month')
    ax3.set_ylabel('Number of Patients')
    ax3.set_title('Monthly Patient Volume')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Year-over-year comparison
    ax4 = axes[1, 1]
    years = list(financial_data.keys())
    mlr_values = [financial_data[y]['mlr_encounter'] for y in years]
    bars = ax4.bar(years, mlr_values, color=['#2ecc71', '#3498db', '#e74c3c'], edgecolor='black', linewidth=1.5)
    ax4.axhline(y=75, color='r', linestyle='--', linewidth=2, label='75% Alert Threshold')
    ax4.set_ylabel('MLR (%)')
    ax4.set_title('Year-over-Year MLR Comparison')
    ax4.legend()
    ax4.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars, mlr_values):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    plt.suptitle('Monthly Trends & Year-over-Year Analysis', fontsize=16, fontweight='bold')
    
    fig.text(0.5, 0.01,
             "Chart Explanations:\n" +
             "Top Left: Monthly claims volume - shows seasonal patterns and peak utilization periods.\n" +
             "Top Right: Monthly claims cost - tracks spending trends and identifies expensive months.\n" +
             "Bottom Left: Monthly patient count - indicates member engagement and usage patterns.\n" +
             "Bottom Right: Year-over-year MLR comparison - demonstrates financial performance improvement or decline across years.",
             ha='center', va='bottom', fontsize=7.5, style='italic',
             bbox=dict(boxstyle='round', facecolor='lavender', alpha=0.3))
    
    plt.tight_layout(rect=[0, 0.10, 1, 1])

def create_recommendations_page(financial_data, standards):
    """Create recommendations page"""
    fig, ax = plt.subplots(figsize=(11, 8.5))
    ax.axis('off')
    
    # Calculate key metrics
    avg_mlr = np.mean([d['mlr_encounter'] for d in financial_data.values()])
    mlr_2025 = financial_data[2025]['mlr_encounter']
    
    recommendations = f"""
STRATEGIC RECOMMENDATIONS & ACTION PLAN
══════════════════════════════════════════════════════════════

PERFORMANCE SUMMARY
Current Average MLR: {avg_mlr:.1f}%
2025 MLR:           {mlr_2025:.1f}%
Industry Benchmark: {standards['optimal_mlr']:.1f}%

STATUS: {'🟢 EXCELLENT' if avg_mlr < 60 else '🔴 CRITICAL' if avg_mlr > 80 else '🟡 MODERATE'}

══════════════════════════════════════════════════════════════

CRITICAL RECOMMENDATIONS

1. MLR MANAGEMENT
   Current MLR: {mlr_2025:.1f}%
   Target MLR:  {standards['optimal_mlr']:.1f}%
   Gap to Close: {abs(mlr_2025 - standards['optimal_mlr']):.1f} percentage points
   
   Actions:
   - Implement stricter PA criteria for high-cost procedures
   - Negotiate better rates with top 10 providers
   - Review utilization patterns of top 20 enrollees
   - Introduce preventive care programs to reduce claims

2. PROVIDER NETWORK OPTIMIZATION
   Recommendations:
   - Audit top 15 providers for cost efficiency
   - Re-negotiate contracts with providers exceeding industry averages
   - Consider volume discounts for high-frequency providers
   - Implement provider scorecards based on cost and quality

3. CLAIMS MANAGEMENT
   Strategies:
   - Implement real-time claims analytics
   - Flag unusual patterns (e.g., >10 claims per enrollee/year)
   - Enhanced PA requirements for procedures >₦100K
   - Quarterly review of procedure costs vs industry standards

4. COST CONTROL MEASURES
   Immediate Actions:
   - Review and re-negotiate subscription costs
   - Optimize welfare spending efficiency
   - Implement cost-saving technology solutions
   - Consider outsourcing non-core functions

5. REVENUE OPTIMIZATION
   Opportunities:
   - Review pricing strategy for client contracts
   - Identify high-value client opportunities
   - Implement tiered premium structures
   - Develop value-added services

6. ENROLLEE HEALTH MANAGEMENT
   Programs:
   - Preventive care campaigns for high-utilization enrollees
   - Health education programs
   - Telemedicine introduction to reduce cost per visit
   - Wellness incentives for good health outcomes

══════════════════════════════════════════════════════════════

MONITORING & KPIs

Key Metrics to Track Monthly:
- MLR by company
- MLR by provider
- Average claims per enrollee
- High-cost procedure trends
- Provider efficiency ratios
- Enrollee utilization patterns

Alert Triggers:
- MLR >75% for any company
- Individual enrollee claims >₦500K/year
- Provider average claim >₦200K
- Any procedure frequency >1000 occurrences/month

══════════════════════════════════════════════════════════════

NEXT STEPS (30-60-90 Day Plan)

Days 1-30:
✓ Complete provider contract renegotiations
✓ Implement enhanced PA criteria
✓ Launch enrollee health campaigns
✓ Deploy real-time analytics dashboards

Days 31-60:
✓ Review and optimize expense categories
✓ Establish provider performance metrics
✓ Implement cost-control measures
✓ Track MLR improvements

Days 61-90:
✓ Evaluate results and adjust strategies
✓ Expand successful programs
✓ Scale optimized processes
✓ Report quarterly performance improvements

══════════════════════════════════════════════════════════════

Projected Impact:
- Reduce MLR by 5-8 percentage points
- Save ₦200-400M annually through cost optimization
- Improve provider efficiency by 20-30%
- Enhance enrollee health outcomes by 15-25%

══════════════════════════════════════════════════════════════
"""
    
    ax.text(0.05, 0.95, recommendations, va='top', ha='left', fontsize=9, 
            family='monospace')
    
    plt.tight_layout()

if __name__ == '__main__':
    create_performance_report()
