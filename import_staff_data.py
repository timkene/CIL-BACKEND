#!/usr/bin/env python3
"""
Import Staff Data from CSV
==========================

Imports staff data from CSV file into the USERS table in DuckDB.
Password format: Password-{first_name_length}-{last_name_length}
"""

import duckdb
import pandas as pd
import os
import sys
from pathlib import Path

# Database configuration
DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'
CSV_FILE = 'staff+email+address.csv_EMAILS AND NAME.csv'

def generate_password(first_name: str, last_name: str) -> str:
    """Generate password in format: Password-{first_name_length}-{last_name_length}"""
    first_len = len(first_name.strip()) if first_name else 0
    last_len = len(last_name.strip()) if last_name else 0
    return f"Password-{first_len}-{last_len}"

def import_staff_data():
    """Import staff data from CSV into USERS table"""
    print("=" * 70)
    print("Importing Staff Data from CSV")
    print("=" * 70)
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE):
        print(f"❌ Error: CSV file not found: {CSV_FILE}")
        return False
    
    # Read CSV file
    print(f"\n📖 Reading CSV file: {CSV_FILE}")
    try:
        df = pd.read_csv(CSV_FILE)
        print(f"✅ Loaded {len(df)} rows from CSV")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return False
    
    # Validate required columns
    required_columns = ['First Name', 'Last Name', 'Email address', 'Department', 'Status']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    # Clean and prepare data
    print("\n🧹 Cleaning and preparing data...")
    users_data = []
    
    for idx, row in df.iterrows():
        first_name = str(row['First Name']).strip() if pd.notna(row['First Name']) else ''
        last_name = str(row['Last Name']).strip() if pd.notna(row['Last Name']) else ''
        email = str(row['Email address']).strip() if pd.notna(row['Email address']) else ''
        department = str(row['Department']).strip() if pd.notna(row['Department']) else None
        status = str(row['Status']).strip() if pd.notna(row['Status']) else 'Active'
        
        # Skip rows with missing essential data
        if not first_name or not last_name or not email:
            print(f"⚠️  Skipping row {idx + 2}: Missing essential data (First Name, Last Name, or Email)")
            continue
        
        # Normalize status
        status = 'Active' if status.lower() == 'active' else 'Terminated'
        
        # Generate password
        password = generate_password(first_name, last_name)
        
        users_data.append({
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'department': department,
            'password': password,
            'status': status
        })
    
    print(f"✅ Prepared {len(users_data)} valid user records")
    
    # Connect to database
    print(f"\n🔌 Connecting to database: {DB_PATH}")
    try:
        conn = duckdb.connect(DB_PATH, read_only=False)
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return False
    
    # Initialize USERS table
    print(f"\n📋 Initializing USERS table...")
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."USERS" (
                user_id INTEGER PRIMARY KEY,
                first_name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                email VARCHAR NOT NULL UNIQUE,
                department VARCHAR,
                password VARCHAR NOT NULL,
                status VARCHAR DEFAULT 'Active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ USERS table initialized")
    except Exception as e:
        print(f"❌ Error initializing table: {e}")
        conn.close()
        return False
    
    # Get existing users (to check for duplicates)
    print(f"\n🔍 Checking for existing users...")
    try:
        existing_emails = set()
        existing_df = conn.execute(f'SELECT email FROM "{SCHEMA}"."USERS"').fetchdf()
        if not existing_df.empty:
            existing_emails = set(existing_df['email'].str.lower())
        print(f"✅ Found {len(existing_emails)} existing users")
    except Exception as e:
        print(f"⚠️  Could not check existing users: {e}")
        existing_emails = set()
    
    # Import users
    print(f"\n📥 Importing users...")
    imported = 0
    updated = 0
    skipped = 0
    errors = []
    
    for user_data in users_data:
        email_lower = user_data['email'].lower()
        
        try:
            if email_lower in existing_emails:
                # Update existing user
                conn.execute(f'''
                    UPDATE "{SCHEMA}"."USERS"
                    SET first_name = ?,
                        last_name = ?,
                        department = ?,
                        password = ?,
                        status = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE LOWER(email) = ?
                ''', [
                    user_data['first_name'],
                    user_data['last_name'],
                    user_data['department'],
                    user_data['password'],
                    user_data['status'],
                    email_lower
                ])
                updated += 1
                print(f"  ✅ Updated: {user_data['email']}")
            else:
                # Insert new user
                # Get next user_id
                max_id_result = conn.execute(f'SELECT COALESCE(MAX(user_id), 0) FROM "{SCHEMA}"."USERS"').fetchone()
                next_id = (max_id_result[0] if max_id_result else 0) + 1
                
                conn.execute(f'''
                    INSERT INTO "{SCHEMA}"."USERS"
                    (user_id, first_name, last_name, email, department, password, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', [
                    next_id,
                    user_data['first_name'],
                    user_data['last_name'],
                    user_data['email'],
                    user_data['department'],
                    user_data['password'],
                    user_data['status']
                ])
                imported += 1
                existing_emails.add(email_lower)  # Track newly added
                print(f"  ✅ Imported: {user_data['email']} (Password: {user_data['password']})")
        except Exception as e:
            skipped += 1
            error_msg = f"Error processing {user_data['email']}: {str(e)}"
            errors.append(error_msg)
            print(f"  ❌ {error_msg}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Import Summary")
    print("=" * 70)
    print(f"✅ Imported: {imported} new users")
    print(f"🔄 Updated: {updated} existing users")
    print(f"⚠️  Skipped/Errors: {skipped} users")
    
    if errors:
        print(f"\n❌ Errors encountered:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"   - {error}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
    
    # Verify import
    try:
        total_users = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."USERS"').fetchone()[0]
        active_users = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."USERS" WHERE status = \'Active\'').fetchone()[0]
        print(f"\n📊 Database Status:")
        print(f"   Total users: {total_users}")
        print(f"   Active users: {active_users}")
        print(f"   Terminated users: {total_users - active_users}")
    except Exception as e:
        print(f"⚠️  Could not verify import: {e}")
    
    conn.close()
    print("\n✅ Import completed!")
    return True

if __name__ == "__main__":
    success = import_staff_data()
    sys.exit(0 if success else 1)
