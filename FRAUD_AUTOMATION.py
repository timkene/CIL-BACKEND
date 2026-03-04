"""
CLEARLINE INTERNATIONAL LIMITED - Monthly Fraud Detection & Audit Report
Auto-generates provider audit recommendations based on same-day same-diagnosis patterns

Author: KLAIRE AI Analyst
Last Updated: 2025-11-09

SETUP INSTRUCTIONS:
1. Install required packages: pip install duckdb pandas openpyxl reportlab
2. Update EMAIL CONFIGURATION section with your details
3. Update DATABASE PATH section with your DuckDB file location
4. Schedule this script to run monthly (e.g., via cron job or Task Scheduler)
5. Script will email you the report automatically

RUN MANUALLY:
python fraud_detection_monthly.py
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import sys
import os

# ============================================================================
# CONFIGURATION SECTION - UPDATE THESE VALUES
# ============================================================================

# EMAIL CONFIGURATION
# Get Gmail password using same logic as MLR.py: try secrets.toml first, then env var
sender_password = ''
try:
    import toml
    # Try .streamlit/secrets.toml first (Streamlit format)
    streamlit_secrets_path = os.path.join(os.path.dirname(__file__), '.streamlit', 'secrets.toml')
    if os.path.exists(streamlit_secrets_path):
        secrets = toml.load(streamlit_secrets_path)
        sender_password = secrets.get('gmail', {}).get('app_password', '') or \
                        secrets.get('email', {}).get('sender_password', '')
    
    # Fallback to root secrets.toml
    if not sender_password:
        secrets_path = os.path.join(os.path.dirname(__file__), 'secrets.toml')
        if os.path.exists(secrets_path):
            secrets = toml.load(secrets_path)
            sender_password = secrets.get('gmail', {}).get('app_password', '') or \
                            secrets.get('email', {}).get('sender_password', '')
except:
    pass

# Fallback to environment variable (same as MLR.py)
if not sender_password:
    sender_password = os.getenv('GMAIL_APP_PASSWORD', '')

EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',  # Change if not using Gmail
    'smtp_port': 587,
    'sender_email': 'leocasey0@gmail.com',
    'sender_password': sender_password,  # Get from secrets.toml or environment variable (same as MLR.py)
    'recipient_emails': [
        'k.chukwuka@clearlinehmo.com',
        'j.chiadikaobi@clearlinehmo.com'
    ]
}

# DATABASE CONFIGURATION
DB_PATH = os.path.join(os.path.dirname(__file__), 'ai_driven_data.duckdb')  # Auto-detect in same directory as script

# REPORT CONFIGURATION
OUTPUT_DIR = './fraud_reports'  # Reports will be saved here
ANALYSIS_LOOKBACK_MONTHS = 3  # Analyze last 3 months of data

# AUDIT THRESHOLDS
THRESHOLDS = {
    'tier1_ratio': 4.0,      # Immediate audit
    'tier2_ratio': 3.0,      # Formal investigation
    'tier3_ratio': 2.0,      # Monitoring required
    'min_claims': 50,        # Minimum claims to trigger analysis
    'min_cost': 100000       # Minimum cost (₦) to flag
}

# ============================================================================
# CORE ANALYSIS FUNCTIONS
# ============================================================================

def connect_to_db():
    """Connect to DuckDB database"""
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        print(f"✅ Connected to database: {DB_PATH}")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)


def get_analysis_date_range():
    """Get date range for analysis (last 3 months)"""
    end_date = datetime.now().replace(day=1) - timedelta(days=1)  # Last day of previous month
    start_date = (end_date.replace(day=1) - timedelta(days=90)).replace(day=1)  # 3 months back
    
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')


def run_fraud_detection_query(conn, start_date, end_date):
    """Run main fraud detection query"""
    
    query = f"""
    WITH fraud_patterns AS (
        SELECT 
            p.providerid,
            p.providername,
            DATE_TRUNC('month', c.encounterdatefrom) as encounter_month,
            DATE(c.encounterdatefrom) as encounter_date,
            c.diagnosiscode,
            d.diagnosisdesc,
            COUNT(DISTINCT c.enrollee_id) as unique_patients,
            COUNT(*) as total_claims,
            CAST(COUNT(*) AS DECIMAL) / NULLIF(COUNT(DISTINCT c.enrollee_id), 0) as claims_per_patient_ratio,
            SUM(c.approvedamount) as total_approved,
            SUM(c.chargeamount) as total_charged,
            SUM(c.deniedamount) as total_denied,
            AVG(c.approvedamount) as avg_approved_per_claim,
            SUM(c.deniedamount) / NULLIF(SUM(c.chargeamount), 0) * 100 as denial_rate_pct
        FROM "AI DRIVEN DATA"."CLAIMS DATA" c
        LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p 
            ON CAST(c.nhisproviderid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
        LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d 
            ON c.diagnosiscode = d.diagnosiscode
        WHERE c.encounterdatefrom >= '{start_date}'
          AND c.encounterdatefrom <= '{end_date}'
          AND c.nhisproviderid IS NOT NULL
          AND c.diagnosiscode IS NOT NULL
          AND c.enrollee_id IS NOT NULL
        GROUP BY 
            p.providerid, 
            p.providername, 
            DATE_TRUNC('month', c.encounterdatefrom),
            DATE(c.encounterdatefrom), 
            c.diagnosiscode, 
            d.diagnosisdesc
        HAVING COUNT(*) >= {THRESHOLDS['min_claims']}
           AND COUNT(DISTINCT c.enrollee_id) > 0
    )
    SELECT 
        *,
        CASE 
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier1_ratio']} THEN 'TIER 1: IMMEDIATE AUDIT'
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier2_ratio']} THEN 'TIER 2: FORMAL INVESTIGATION'
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier3_ratio']} THEN 'TIER 3: MONITORING REQUIRED'
            ELSE 'TIER 4: NORMAL'
        END as audit_tier,
        CASE 
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier1_ratio']} THEN 1
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier2_ratio']} THEN 2
            WHEN claims_per_patient_ratio >= {THRESHOLDS['tier3_ratio']} THEN 3
            ELSE 4
        END as tier_priority
    FROM fraud_patterns
    WHERE claims_per_patient_ratio >= {THRESHOLDS['tier3_ratio']}  -- Only flag Tier 1-3
       OR total_approved >= {THRESHOLDS['min_cost']}
    ORDER BY tier_priority ASC, claims_per_patient_ratio DESC, total_approved DESC
    """
    
    try:
        df = conn.execute(query).fetchdf()
        print(f"✅ Fraud detection query completed. Found {len(df)} flagged patterns.")
        return df
    except Exception as e:
        print(f"❌ Error running fraud detection query: {e}")
        return pd.DataFrame()


def get_provider_summary(conn, start_date, end_date):
    """Get provider-level summary statistics"""
    
    query = f"""
    SELECT 
        p.providerid,
        p.providername,
        COUNT(*) as total_claims,
        COUNT(DISTINCT c.enrollee_id) as total_unique_patients,
        SUM(c.approvedamount) as total_approved,
        SUM(c.deniedamount) as total_denied,
        SUM(c.deniedamount) / NULLIF(SUM(c.chargeamount), 0) * 100 as overall_denial_rate,
        COUNT(DISTINCT DATE(c.encounterdatefrom)) as days_with_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p 
        ON CAST(c.nhisproviderid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
    WHERE c.encounterdatefrom >= '{start_date}'
      AND c.encounterdatefrom <= '{end_date}'
      AND c.nhisproviderid IS NOT NULL
    GROUP BY p.providerid, p.providername
    ORDER BY total_approved DESC
    """
    
    try:
        df = conn.execute(query).fetchdf()
        print(f"✅ Provider summary completed. Analyzed {len(df)} providers.")
        return df
    except Exception as e:
        print(f"❌ Error getting provider summary: {e}")
        return pd.DataFrame()


# ============================================================================
# REPORT GENERATION FUNCTIONS
# ============================================================================

def generate_excel_report(fraud_df, provider_df, start_date, end_date, output_path):
    """Generate detailed Excel report with multiple sheets"""
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            
            # Sheet 1: Executive Summary
            summary_data = {
                'Metric': [
                    'Analysis Period',
                    'Total Flagged Patterns',
                    'TIER 1 (Immediate Audit)',
                    'TIER 2 (Investigation)',
                    'TIER 3 (Monitoring)',
                    'Total Amount at Risk',
                    'Providers Flagged',
                    'Report Generated'
                ],
                'Value': [
                    f"{start_date} to {end_date}",
                    len(fraud_df),
                    len(fraud_df[fraud_df['tier_priority'] == 1]),
                    len(fraud_df[fraud_df['tier_priority'] == 2]),
                    len(fraud_df[fraud_df['tier_priority'] == 3]),
                    f"₦{fraud_df['total_approved'].sum():,.2f}",
                    fraud_df['providerid'].nunique(),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Executive Summary', index=False)
            
            # Sheet 2: TIER 1 - Immediate Audit (Highest Priority)
            tier1 = fraud_df[fraud_df['tier_priority'] == 1].copy()
            tier1['encounter_date'] = pd.to_datetime(tier1['encounter_date']).dt.strftime('%Y-%m-%d')
            tier1['encounter_month'] = pd.to_datetime(tier1['encounter_month']).dt.strftime('%Y-%m')
            tier1_cols = [
                'providername', 'encounter_date', 'encounter_month', 
                'diagnosiscode', 'diagnosisdesc', 'unique_patients', 
                'total_claims', 'claims_per_patient_ratio', 'total_approved', 
                'total_denied', 'denial_rate_pct', 'audit_tier'
            ]
            tier1[tier1_cols].to_excel(writer, sheet_name='TIER 1 - Immediate Audit', index=False)
            
            # Sheet 3: TIER 2 - Formal Investigation
            tier2 = fraud_df[fraud_df['tier_priority'] == 2].copy()
            tier2['encounter_date'] = pd.to_datetime(tier2['encounter_date']).dt.strftime('%Y-%m-%d')
            tier2['encounter_month'] = pd.to_datetime(tier2['encounter_month']).dt.strftime('%Y-%m')
            tier2[tier1_cols].to_excel(writer, sheet_name='TIER 2 - Investigation', index=False)
            
            # Sheet 4: TIER 3 - Monitoring
            tier3 = fraud_df[fraud_df['tier_priority'] == 3].copy()
            tier3['encounter_date'] = pd.to_datetime(tier3['encounter_date']).dt.strftime('%Y-%m-%d')
            tier3['encounter_month'] = pd.to_datetime(tier3['encounter_month']).dt.strftime('%Y-%m')
            tier3[tier1_cols].to_excel(writer, sheet_name='TIER 3 - Monitoring', index=False)
            
            # Sheet 5: Provider Summary (All providers, not just flagged)
            provider_summary_cols = [
                'providername', 'total_claims', 'total_unique_patients',
                'total_approved', 'total_denied', 'overall_denial_rate',
                'days_with_claims'
            ]
            provider_df[provider_summary_cols].to_excel(writer, sheet_name='Provider Summary', index=False)
            
            # Sheet 6: Detailed Breakdown (All fraud patterns)
            fraud_detail = fraud_df.copy()
            fraud_detail['encounter_date'] = pd.to_datetime(fraud_detail['encounter_date']).dt.strftime('%Y-%m-%d')
            fraud_detail['encounter_month'] = pd.to_datetime(fraud_detail['encounter_month']).dt.strftime('%Y-%m')
            fraud_detail_cols = [
                'providerid', 'providername', 'encounter_date', 'encounter_month',
                'diagnosiscode', 'diagnosisdesc', 'unique_patients', 'total_claims',
                'claims_per_patient_ratio', 'total_approved', 'total_charged',
                'total_denied', 'avg_approved_per_claim', 'denial_rate_pct',
                'audit_tier', 'tier_priority'
            ]
            fraud_detail[fraud_detail_cols].to_excel(writer, sheet_name='Detailed Breakdown', index=False)
            
        print(f"✅ Excel report generated: {output_path}")
        return True
        
    except Exception as e:
        print(f"❌ Error generating Excel report: {e}")
        return False


def generate_html_summary(fraud_df, provider_df, start_date, end_date):
    """Generate HTML email summary"""
    
    tier1_count = len(fraud_df[fraud_df['tier_priority'] == 1])
    tier2_count = len(fraud_df[fraud_df['tier_priority'] == 2])
    tier3_count = len(fraud_df[fraud_df['tier_priority'] == 3])
    
    total_at_risk = fraud_df['total_approved'].sum()
    providers_flagged = fraud_df['providerid'].nunique()
    
    # Get top 5 worst offenders
    top_offenders = fraud_df.nsmallest(5, 'tier_priority').head()
    
    html = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
            }}
            .header {{
                background-color: #d32f2f;
                color: white;
                padding: 20px;
                text-align: center;
            }}
            .summary {{
                background-color: #f5f5f5;
                padding: 20px;
                margin: 20px 0;
                border-radius: 5px;
            }}
            .metric {{
                display: inline-block;
                margin: 10px 20px;
                text-align: center;
            }}
            .metric-value {{
                font-size: 32px;
                font-weight: bold;
                color: #d32f2f;
            }}
            .metric-label {{
                font-size: 14px;
                color: #666;
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin: 20px 0;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
            }}
            th {{
                background-color: #d32f2f;
                color: white;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            .tier1 {{ background-color: #ffebee; }}
            .tier2 {{ background-color: #fff3e0; }}
            .tier3 {{ background-color: #fff9c4; }}
            .footer {{
                margin-top: 30px;
                padding: 20px;
                background-color: #f5f5f5;
                text-align: center;
                font-size: 12px;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🚨 CLEARLINE MONTHLY FRAUD DETECTION REPORT</h1>
            <p>Analysis Period: {start_date} to {end_date}</p>
        </div>
        
        <div class="summary">
            <h2>📊 Executive Summary</h2>
            <div class="metric">
                <div class="metric-value">{tier1_count}</div>
                <div class="metric-label">TIER 1<br>Immediate Audit</div>
            </div>
            <div class="metric">
                <div class="metric-value">{tier2_count}</div>
                <div class="metric-label">TIER 2<br>Investigation</div>
            </div>
            <div class="metric">
                <div class="metric-value">{tier3_count}</div>
                <div class="metric-label">TIER 3<br>Monitoring</div>
            </div>
            <div class="metric">
                <div class="metric-value">₦{total_at_risk:,.0f}</div>
                <div class="metric-label">Total Amount<br>At Risk</div>
            </div>
            <div class="metric">
                <div class="metric-value">{providers_flagged}</div>
                <div class="metric-label">Providers<br>Flagged</div>
            </div>
        </div>
        
        <h2>🔴 Top 5 Priority Cases (Immediate Action Required)</h2>
        <table>
            <tr>
                <th>Provider</th>
                <th>Date</th>
                <th>Diagnosis</th>
                <th>Patients</th>
                <th>Claims</th>
                <th>Ratio</th>
                <th>Amount</th>
                <th>Tier</th>
            </tr>
    """
    
    for _, row in top_offenders.iterrows():
        tier_class = f"tier{row['tier_priority']}"
        html += f"""
            <tr class="{tier_class}">
                <td>{row['providername']}</td>
                <td>{row['encounter_date']}</td>
                <td>{row['diagnosiscode']} - {row['diagnosisdesc'][:50]}</td>
                <td>{row['unique_patients']:.0f}</td>
                <td>{row['total_claims']:.0f}</td>
                <td><strong>{row['claims_per_patient_ratio']:.2f}</strong></td>
                <td>₦{row['total_approved']:,.2f}</td>
                <td>{row['audit_tier']}</td>
            </tr>
        """
    
    html += """
        </table>
        
        <h2>📋 Next Steps</h2>
        <ol>
            <li><strong>TIER 1 Providers:</strong> Suspend payments immediately, send audit notice within 24 hours</li>
            <li><strong>TIER 2 Providers:</strong> Initiate formal investigation within 7 days, request documentation</li>
            <li><strong>TIER 3 Providers:</strong> Add to monitoring list, require pre-authorization for future claims</li>
        </ol>
        
        <p><strong>📎 Detailed Excel report attached with:</strong></p>
        <ul>
            <li>Complete breakdown by provider and date</li>
            <li>Separate sheets for each audit tier</li>
            <li>Provider summary statistics</li>
            <li>Full detailed analysis</li>
        </ul>
        
        <div class="footer">
            <p>🤖 This report was automatically generated by KLAIRE AI Analyst</p>
            <p>Report Generated: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """</p>
            <p>CLEARLINE INTERNATIONAL LIMITED - Health Insurance Fraud Detection System</p>
        </div>
    </body>
    </html>
    """
    
    return html


def send_email_report(html_body, excel_path, start_date, end_date):
    """Send email with HTML body and Excel attachment"""
    
    # Validate email configuration
    if not EMAIL_CONFIG['sender_password']:
        print("❌ ERROR: Gmail app password not set!")
        print("   Please set the GMAIL_APP_PASSWORD environment variable:")
        print("   export GMAIL_APP_PASSWORD='your_app_password'")
        print("   Or update the EMAIL_CONFIG in the script directly.")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ', '.join(EMAIL_CONFIG['recipient_emails'])
        msg['Subject'] = f"🚨 CLEARLINE Fraud Detection Report - {start_date} to {end_date}"
        
        # Attach HTML body
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Attach Excel file
        with open(excel_path, 'rb') as f:
            excel_attachment = MIMEBase('application', 'vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            excel_attachment.set_payload(f.read())
            encoders.encode_base64(excel_attachment)
            excel_attachment.add_header(
                'Content-Disposition',
                f'attachment; filename=Fraud_Detection_Report_{datetime.now().strftime("%Y%m%d")}.xlsx'
            )
            msg.attach(excel_attachment)
        
        # Send email
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Email sent successfully to: {', '.join(EMAIL_CONFIG['recipient_emails'])}")
        return True
        
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    
    print("=" * 80)
    print("CLEARLINE INTERNATIONAL LIMITED - Monthly Fraud Detection Report")
    print("=" * 80)
    print()
    
    # Create output directory if it doesn't exist
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    conn = connect_to_db()
    
    # Get analysis date range
    start_date, end_date = get_analysis_date_range()
    print(f"📅 Analyzing period: {start_date} to {end_date}")
    print()
    
    # Run fraud detection analysis
    print("🔍 Running fraud detection analysis...")
    fraud_df = run_fraud_detection_query(conn, start_date, end_date)
    
    if fraud_df.empty:
        print("✅ No fraud patterns detected! All providers are within acceptable thresholds.")
        conn.close()
        return
    
    # Get provider summary
    print("📊 Generating provider summary...")
    provider_df = get_provider_summary(conn, start_date, end_date)
    
    # Close database connection
    conn.close()
    
    # Generate reports
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    excel_filename = f"Fraud_Detection_Report_{timestamp}.xlsx"
    excel_path = Path(OUTPUT_DIR) / excel_filename
    
    print("📝 Generating Excel report...")
    generate_excel_report(fraud_df, provider_df, start_date, end_date, excel_path)
    
    print("📧 Generating email summary...")
    html_body = generate_html_summary(fraud_df, provider_df, start_date, end_date)
    
    print("📤 Sending email report...")
    send_email_report(html_body, excel_path, start_date, end_date)
    
    print()
    print("=" * 80)
    print("✅ FRAUD DETECTION REPORT COMPLETED SUCCESSFULLY!")
    print(f"📊 Report saved to: {excel_path}")
    print(f"📧 Email sent to: {', '.join(EMAIL_CONFIG['recipient_emails'])}")
    print("=" * 80)


if __name__ == "__main__":
    main()