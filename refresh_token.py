#!/usr/bin/env python3
"""
Refresh and save Zoho token
"""

import requests
import json
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Zoho API Configuration
ZOHO_CLIENT_ID = os.getenv('ZOHO_CLIENT_ID')
ZOHO_CLIENT_SECRET = os.getenv('ZOHO_CLIENT_SECRET')
ZOHO_REFRESH_TOKEN = os.getenv('ZOHO_REFRESH_TOKEN')
ZOHO_ACCOUNTS_DOMAIN = os.getenv('ZOHO_ACCOUNTS_DOMAIN', 'https://accounts.zoho.com')

def refresh_and_save_token():
    """Refresh the access token and save it"""
    url = f'{ZOHO_ACCOUNTS_DOMAIN}/oauth/v2/token'
    
    params = {
        'refresh_token': ZOHO_REFRESH_TOKEN,
        'client_id': ZOHO_CLIENT_ID,
        'client_secret': ZOHO_CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    
    try:
        print("Refreshing access token...")
        response = requests.post(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Save the new access token with expiration time
        tokens = {
            'access_token': data['access_token'],
            'expires_at': datetime.now().timestamp() + data.get('expires_in', 3600) - 300  # 5 min buffer
        }
        
        with open('zoho_tokens.json', 'w') as f:
            json.dump(tokens, f)
        
        print("✅ Token refreshed and saved successfully!")
        print(f"New token: {data['access_token'][:20]}...")
        return data['access_token']
        
    except Exception as e:
        print(f"Error refreshing token: {e}")
        return None

if __name__ == "__main__":
    refresh_and_save_token()
