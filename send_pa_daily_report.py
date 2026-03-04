#!/usr/bin/env python3
"""
PA Daily Report Email Script
============================
Fetches PA data from MediCloud for a specific date and sends an email report.

Usage:
    python send_pa_daily_report.py 2026-01-26
    python send_pa_daily_report.py  # Will prompt for date
"""

import sys
import os
from datetime import datetime
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import toml

# Import medicloud connection from dlt_sources
from dlt_sources import create_medicloud_connection

# Email configuration - load from secrets.toml or environment
def load_email_config():
    """Load email configuration from secrets.toml or environment variables"""
    sender_email = os.getenv('SENDER_EMAIL', 'leocasey0@gmail.com')
    sender_password = os.getenv('GMAIL_APP_PASSWORD', '')
    
    # Try to load from secrets.toml
    try:
        secrets_path = 'secrets.toml'
        if os.path.exists(secrets_path):
            secrets = toml.load(secrets_path)
            sender_password = secrets.get('gmail', {}).get('app_password', '') or \
                            secrets.get('email', {}).get('sender_password', '') or \
                            sender_password
    except Exception as e:
        print(f"⚠️  Could not load secrets.toml: {e}")
    
    # Fallback to .env file
    if not sender_password:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            sender_password = os.getenv('GMAIL_APP_PASSWORD', '')
        except:
            pass
    
    return {
        'smtp_server': 'smtp.gmail.com',
        'smtp_port': 587,
        'sender_email': sender_email,
        'sender_password': sender_password,
        'recipient_emails': [
            'o.nwanagu@clearlinehmo.com', 
            't.adeosun@clearlinehmo.com'
        ]
    }

def fetch_pa_data_for_date(encounter_date):
    """
    Fetch PA data from MediCloud for a specific encounter date.
    
    Returns DataFrame with columns:
    - panumber
    - enrollee_id
    - first_name
    - last_name
    - client_name
    - email
    - phone_number
    - providername
    """
    print(f"🔌 Connecting to MediCloud...")
    conn = create_medicloud_connection()
    
    try:
        print(f"📊 Fetching PA data for date: {encounter_date}")
        
        # Query PA data with joins to get all required information
        # Strategy: Try using IID from tbPATxn first (if it's member identifier)
        # Fallback to claims join if IID doesn't work
        # We use claims table because it reliably links panumber to memberid
        # (Claims are created from approved PAs, so they have both panumber and memberid)
        query = f"""
        SELECT DISTINCT
            txn.panumber AS panumber,
            m.legacycode AS enrollee_id,
            m.FirstName AS first_name,
            m.LastName AS last_name,
            COALESCE(g.GroupName, txn.groupname) AS client_name,
            m.Email1 AS email,
            m.Phone1 AS phone_number,
            p.providername AS providername
        FROM dbo.tbPATxn txn
        LEFT JOIN dbo.member m ON txn.IID = m.legacycode
        LEFT JOIN dbo.[group] g ON m.GroupId = g.GroupId
        LEFT JOIN dbo.provider p ON txn.providerid = p.providertin
        WHERE CAST(txn.requestdate AS DATE) = '{encounter_date}'
          AND txn.panumber IS NOT NULL
        ORDER BY txn.panumber
        """
        
        df = pd.read_sql(query, conn)
        
        # If no results or missing member data, try alternative using claims join
        # Claims table is needed because:
        # 1. It links panumber to memberid (claims are created from approved PAs)
        # 2. PAIssueRequest doesn't have memberid directly
        # 3. tbPATxn.IID might not always match member.legacycode
        if len(df) == 0 or df['first_name'].isna().all():
            print("   Trying alternative query with claims join...")
            query2 = f"""
            SELECT DISTINCT
                txn.panumber AS panumber,
                m.legacycode AS enrollee_id,
                m.FirstName AS first_name,
                m.LastName AS last_name,
                COALESCE(g.GroupName, txn.groupname) AS client_name,
                m.Email1 AS email,
                m.Phone1 AS phone_number,
                p.providername AS providername
            FROM dbo.tbPATxn txn
            LEFT JOIN dbo.claims c ON txn.panumber = c.panumber 
                AND CAST(c.encounterdatefrom AS DATE) = '{encounter_date}'
            LEFT JOIN dbo.member m ON c.memberid = m.memberid
            LEFT JOIN dbo.[group] g ON m.GroupId = g.GroupId
            LEFT JOIN dbo.provider p ON txn.providerid = p.providertin
            WHERE CAST(txn.requestdate AS DATE) = '{encounter_date}'
              AND txn.panumber IS NOT NULL
            ORDER BY txn.panumber
            """
            df2 = pd.read_sql(query2, conn)
            if len(df2) > 0:
                df = df2
        
        # If still no results, try PAIssueRequest with claims join
        if len(df) == 0:
            print("   Trying PAIssueRequest with claims join...")
            query3 = f"""
            SELECT DISTINCT
                pa.PANumber AS panumber,
                m.legacycode AS enrollee_id,
                m.FirstName AS first_name,
                m.LastName AS last_name,
                g.GroupName AS client_name,
                m.Email1 AS email,
                m.Phone1 AS phone_number,
                p.providername AS providername
            FROM dbo.PAIssueRequest pa
            LEFT JOIN dbo.claims c ON pa.PANumber = c.panumber 
                AND CAST(c.encounterdatefrom AS DATE) = '{encounter_date}'
            LEFT JOIN dbo.member m ON c.memberid = m.memberid
            LEFT JOIN dbo.[group] g ON m.GroupId = g.GroupId
            LEFT JOIN dbo.provider p ON pa.ProviderId = p.providertin
            WHERE CAST(pa.EncounterDate AS DATE) = '{encounter_date}'
              AND pa.PANumber IS NOT NULL
            ORDER BY pa.PANumber
            """
            df3 = pd.read_sql(query3, conn)
            if len(df3) > 0:
                df = df3
        
        print(f"✅ Found {len(df)} PA records for {encounter_date}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error fetching PA data: {e}")
        raise
    finally:
        conn.close()

def format_email_body(df, encounter_date):
    """Format the email body with PA data"""
    
    if len(df) == 0:
        body = f"""
Dear Team,

PA Daily Report for {encounter_date}

No PA records found for this date.

Best regards,
PA Reporting System
        """
        return body
    
    # Create HTML table
    html_table = df.to_html(index=False, classes='table', table_id='pa-table')
    
    body = f"""
<!DOCTYPE html>
<html>
<head>
    <style>
        body {{ font-family: Arial, sans-serif; }}
        h2 {{ color: #333; }}
        .table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        .table th {{ background-color: #4CAF50; color: white; }}
        .table tr:nth-child(even) {{ background-color: #f2f2f2; }}
        .summary {{ background-color: #e7f3ff; padding: 15px; border-radius: 5px; margin: 20px 0; }}
    </style>
</head>
<body>
    <h2>PA Daily Report - {encounter_date}</h2>
    
    <div class="summary">
        <strong>Summary:</strong><br>
        Total PA Records: {len(df)}<br>
        Date: {encounter_date}<br>
        Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    </div>
    
    <h3>PA Details:</h3>
    {html_table}
    
    <p><em>This is an automated report from the PA Reporting System.</em></p>
</body>
</html>
    """
    
    return body

def send_email(df, encounter_date, email_config):
    """Send email with PA data"""
    
    if not email_config['sender_password']:
        print("❌ ERROR: Gmail app password not configured!")
        print("   Please set GMAIL_APP_PASSWORD environment variable or")
        print("   add it to secrets.toml under [gmail].app_password")
        return False
    
    try:
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = email_config['sender_email']
        msg['To'] = ', '.join(email_config['recipient_emails'])
        msg['Subject'] = f"PA Daily Report - {encounter_date} ({len(df)} records)"
        
        # Create both plain text and HTML versions
        if len(df) == 0:
            text_body = f"PA Daily Report for {encounter_date}\n\nNo PA records found for this date."
            html_body = f"<html><body><h2>PA Daily Report - {encounter_date}</h2><p>No PA records found for this date.</p></body></html>"
        else:
            text_body = f"""
PA Daily Report - {encounter_date}

Total Records: {len(df)}

Details:
{df.to_string(index=False)}
            """
            html_body = format_email_body(df, encounter_date)
        
        # Attach plain text version
        text_part = MIMEText(text_body, 'plain')
        msg.attach(text_part)
        
        # Attach HTML version
        html_part = MIMEText(html_body, 'html')
        msg.attach(html_part)
        
        # Send email
        print(f"📧 Sending email to: {', '.join(email_config['recipient_emails'])}")
        server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
        server.starttls()
        server.login(email_config['sender_email'], email_config['sender_password'])
        server.send_message(msg)
        server.quit()
        
        print(f"✅ Email sent successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error sending email: {e}")
        return False

def main():
    """Main execution function"""
    
    # Get date from command line argument or prompt
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
    else:
        date_str = input("Enter date (YYYY-MM-DD): ").strip()
    
    # Validate date format
    try:
        encounter_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        encounter_date_str = encounter_date.strftime('%Y-%m-%d')
    except ValueError:
        print(f"❌ Invalid date format: {date_str}")
        print("   Please use format: YYYY-MM-DD (e.g., 2026-01-26)")
        sys.exit(1)
    
    print("=" * 80)
    print("PA DAILY REPORT EMAIL SCRIPT")
    print("=" * 80)
    print(f"Date: {encounter_date_str}")
    print()
    
    # Load email configuration
    email_config = load_email_config()
    
    # Fetch PA data
    try:
        df = fetch_pa_data_for_date(encounter_date_str)
        
        if len(df) == 0:
            print(f"⚠️  No PA records found for {encounter_date_str}")
            response = input("Send email anyway? (yes/no): ").strip().lower()
            if response != 'yes':
                print("❌ Email not sent.")
                return
        
        # Display summary
        print()
        print("=" * 80)
        print("DATA SUMMARY")
        print("=" * 80)
        print(f"Total Records: {len(df)}")
        if len(df) > 0:
            print(f"\nFirst 5 records:")
            print(df.head().to_string(index=False))
        print()
        
        # Send email
        success = send_email(df, encounter_date_str, email_config)
        
        if success:
            print()
            print("=" * 80)
            print("✅ REPORT COMPLETE")
            print("=" * 80)
        else:
            print()
            print("=" * 80)
            print("❌ EMAIL FAILED")
            print("=" * 80)
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
