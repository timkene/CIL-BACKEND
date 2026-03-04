#!/usr/bin/env python3
"""
Zoho Raw Data Export - Get all available columns
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Zoho API Configuration
ZOHO_ORG_ID = os.getenv('ZOHO_ORG_ID')
ZOHO_API_DOMAIN = os.getenv('ZOHO_API_DOMAIN', 'https://desk.zoho.com')

def get_cached_token():
    """Get cached access token or refresh if expired"""
    try:
        with open('zoho_tokens.json', 'r') as f:
            tokens = json.load(f)
            # Check if token is expired
            if 'expires_at' in tokens and datetime.now().timestamp() < tokens['expires_at']:
                return tokens.get('access_token')
    except:
        pass
    
    # Token expired or doesn't exist, refresh it
    print("Token expired or not found. Refreshing...")
    return refresh_token()

def refresh_token():
    """Refresh the access token"""
    from dotenv import load_dotenv
    load_dotenv()
    
    ZOHO_CLIENT_ID = os.getenv('ZOHO_CLIENT_ID')
    ZOHO_CLIENT_SECRET = os.getenv('ZOHO_CLIENT_SECRET')
    ZOHO_REFRESH_TOKEN = os.getenv('ZOHO_REFRESH_TOKEN')
    ZOHO_ACCOUNTS_DOMAIN = os.getenv('ZOHO_ACCOUNTS_DOMAIN', 'https://accounts.zoho.com')
    
    url = f'{ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token'
    
    params = {
        'refresh_token': ZOHO_REFRESH_TOKEN,
        'client_id': ZOHO_CLIENT_ID,
        'client_secret': ZOHO_CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    
    try:
        response = requests.post(url, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        # Save the new access token with expiration time
        tokens = {
            'access_token': data['access_token'],
            'expires_at': datetime.now().timestamp() + data.get('expires_in', 3600) - 300  # 5 min buffer
        }
        
        with open('zoho_tokens.json', 'w') as f:
            json.dump(tokens, f)
        
        print("✅ Token refreshed successfully!")
        return data['access_token']
        
    except Exception as e:
        print(f"❌ Error refreshing token: {e}")
        return None

def get_all_ticket_columns(access_token, limit=100):
    """Fetch tickets and extract all available columns"""
    all_tickets = []
    from_index = 0
    
    headers = {
        'Authorization': f'Zoho-oauthtoken {access_token}',
        'orgId': ZOHO_ORG_ID
    }
    
    # Yesterday's date range
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - timedelta(days=1)
    tomorrow = today
    
    print(f"Fetching tickets for yesterday ({yesterday.date()})...")
    print("Collecting all available columns...")
    
    all_columns = set()
    
    while True:
        url = f'{ZOHO_API_DOMAIN}/api/v1/tickets'
        params = {
            'from': from_index,
            'limit': limit
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            tickets = data.get('data', [])
            if not tickets:
                break
            
            # Filter tickets by yesterday's date
            yesterday_tickets = []
            for ticket in tickets:
                try:
                    created_time = datetime.strptime(ticket['createdTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                    if yesterday <= created_time < tomorrow:
                        yesterday_tickets.append(ticket)
                        # Collect all column names
                        all_columns.update(ticket.keys())
                except:
                    continue
            
            all_tickets.extend(yesterday_tickets)
            print(f"Fetched {len(tickets)} tickets, {len(yesterday_tickets)} from yesterday (Total: {len(all_tickets)})")
            print(f"Columns found so far: {len(all_columns)}")
            
            # Check if there are more tickets
            if len(tickets) < limit:
                break
                
            from_index += limit
            
            # Stop if we've gone too far back in time
            if tickets and len(yesterday_tickets) == 0:
                last_ticket_time = datetime.strptime(tickets[-1]['createdTime'], '%Y-%m-%dT%H:%M:%S.%fZ')
                if last_ticket_time < yesterday:
                    print("Reached tickets older than yesterday, stopping...")
                    break
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching tickets: {e}")
            break
    
    print(f"\n✅ Found {len(all_columns)} unique columns:")
    for col in sorted(all_columns):
        print(f"  - {col}")
    
    return all_tickets, sorted(all_columns)

def export_raw_data(tickets, all_columns):
    """Export raw data with all available columns"""
    
    if not tickets:
        print("No tickets to export")
        return
    
    print(f"\n📊 EXPORTING RAW DATA WITH ALL COLUMNS...")
    
    # Create DataFrame with all columns
    df_data = []
    
    for ticket in tickets:
        row = {}
        for col in all_columns:
            value = ticket.get(col, None)
            
            # Handle nested objects by converting to string
            if isinstance(value, dict):
                row[col] = json.dumps(value)
            elif isinstance(value, list):
                row[col] = json.dumps(value)
            else:
                row[col] = value
        
        df_data.append(row)
    
    df = pd.DataFrame(df_data)
    
    # Display column info
    print(f"\n📋 COLUMN INFORMATION:")
    print(f"Total columns: {len(df.columns)}")
    print(f"Total tickets: {len(df)}")
    
    # Show data types
    print(f"\n📊 DATA TYPES:")
    for col in df.columns:
        non_null_count = df[col].notna().sum()
        print(f"  {col}: {df[col].dtype} ({non_null_count}/{len(df)} non-null)")
    
    # Show sample data for each column
    print(f"\n📝 SAMPLE DATA (first 3 rows):")
    for col in df.columns:
        sample_values = df[col].dropna().head(3).tolist()
        print(f"\n{col}:")
        for i, val in enumerate(sample_values):
            print(f"  {i+1}. {val}")
    
    # Save to CSV
    output_file = f'zoho_raw_data_all_columns_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df.to_csv(output_file, index=False)
    print(f"\n💾 Raw data exported to: {output_file}")
    
    # Also create a summary of important columns
    important_columns = [
        'id', 'ticketNumber', 'subject', 'status', 'assigneeId', 'createdTime',
        'customerResponseTime', 'agentResponseTime', 'systemResponseTime',
        'agentRespondedTime', 'threadCount', 'channel', 'priority', 'dueDate',
        'responseDueDate', 'closedTime', 'departmentId', 'contactId'
    ]
    
    available_important = [col for col in important_columns if col in df.columns]
    print(f"\n🎯 IMPORTANT COLUMNS FOUND: {available_important}")
    
    if available_important:
        important_df = df[available_important]
        important_file = f'zoho_important_columns_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        important_df.to_csv(important_file, index=False)
        print(f"💾 Important columns exported to: {important_file}")

def main():
    """Main function - Daily Zoho Data Export"""
    print("🔍 Zoho Desk - Daily Raw Data Export")
    print("=" * 45)
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Get access token (auto-refreshes if needed)
    access_token = get_cached_token()
    if not access_token:
        print("❌ Failed to get access token. Check your .env file.")
        return
    
    print(f"✅ Using token: {access_token[:20]}...")
    print()
    
    # Get all tickets and columns
    tickets, all_columns = get_all_ticket_columns(access_token)
    
    if not tickets:
        print("❌ No tickets found for yesterday")
        return
    
    # Export raw data
    export_raw_data(tickets, all_columns)
    
    print(f"\n🎉 Daily export complete!")
    print(f"📁 Files saved with timestamp: {datetime.now().strftime('%Y%m%d_%H%M%S')}")
    print(f"💡 Run this script daily to get fresh data")

if __name__ == "__main__":
    main()
