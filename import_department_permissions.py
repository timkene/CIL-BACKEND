#!/usr/bin/env python3
"""
Import Department Permissions from CSV
======================================

Imports department permissions from CSV file into the DEPARTMENT_PERMISSIONS table in DuckDB.
CSV format: DEPARTMENTS, MODULES (comma-separated module IDs)
"""

import duckdb
import pandas as pd
import os
import sys
from pathlib import Path

# Database configuration
DB_PATH = 'ai_driven_data.duckdb'
SCHEMA = 'AI DRIVEN DATA'

def init_department_permissions_table(conn):
    """Initialize the DEPARTMENT_PERMISSIONS table if it doesn't exist"""
    try:
        conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        conn.execute(f'''
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."DEPARTMENT_PERMISSIONS" (
                permission_id INTEGER PRIMARY KEY,
                department VARCHAR NOT NULL UNIQUE,
                modules VARCHAR NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        print("✅ DEPARTMENT_PERMISSIONS table initialized")
    except Exception as e:
        print(f"❌ Failed to initialize DEPARTMENT_PERMISSIONS table: {e}")
        raise

def import_department_permissions(csv_file):
    """Import department permissions from CSV into DEPARTMENT_PERMISSIONS table"""
    print("=" * 70)
    print("Importing Department Permissions from CSV")
    print("=" * 70)
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: CSV file not found: {csv_file}")
        return False
    
    # Read CSV file
    print(f"\n📖 Reading CSV file: {csv_file}")
    try:
        df = pd.read_csv(csv_file)
        print(f"✅ Loaded {len(df)} rows from CSV")
    except Exception as e:
        print(f"❌ Error reading CSV file: {e}")
        return False
    
    # Validate required columns
    required_columns = ['DEPARTMENTS', 'MODULES']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        return False
    
    # Connect to database
    print(f"\n🔌 Connecting to database: {DB_PATH}")
    try:
        conn = duckdb.connect(DB_PATH, read_only=False)
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return False
    
    # Initialize table
    init_department_permissions_table(conn)
    
    # Process and import data
    print(f"\n📥 Importing department permissions...")
    imported = 0
    updated = 0
    skipped = 0
    errors = []
    
    # Available modules mapping (for validation)
    available_modules = ['mlr', 'client', 'enrollee', 'finance', 'paclaims', 'admin']
    
    # Module name mappings (CSV names -> module IDs)
    module_name_mapping = {
        'MLR ANALYSIS': 'mlr',
        'MLR': 'mlr',
        'CLIENT ANALYSIS': 'client',
        'ENROLLEE MANAGEMENT': 'enrollee',
        'FINANCES': 'finance',
        'PA & CLAIMS': 'paclaims',
        'PA/CLAIMS': 'paclaims',
        'PA CLAIMS': 'paclaims',
        'ADMIN': 'admin',
        'ALL': 'ALL',  # Special case - will be handled separately
        'NONE': 'NONE'  # Special case - will be handled separately
    }
    
    for idx, row in df.iterrows():
        department = str(row['DEPARTMENTS']).strip() if pd.notna(row['DEPARTMENTS']) else ''
        modules_str = str(row['MODULES']).strip() if pd.notna(row['MODULES']) else ''
        
        # Skip rows with missing department
        if not department:
            skipped += 1
            print(f"  ⚠️  Skipping row {idx + 2}: Missing department")
            continue
        
        # Parse modules (comma-separated)
        raw_modules = [m.strip() for m in modules_str.split(',') if m.strip()]
        
        # Special handling for MGT, ADMIN, and IT departments
        if department.upper() == 'ADMIN':
            # ADMIN gets all modules including admin
            modules_list = available_modules.copy()
            print(f"  ℹ️  {department}: Auto-assigned all modules (ADMIN)")
        elif department.upper() in ['MGT', 'IT']:
            # MGT and IT get all modules EXCEPT admin
            modules_list = [m for m in available_modules if m != 'admin']
            print(f"  ℹ️  {department}: Auto-assigned all modules except admin (MGT/IT)")
        elif 'ALL' in [m.upper() for m in raw_modules]:
            # If CSV says ALL, give all modules
            modules_list = available_modules.copy()
            print(f"  ℹ️  {department}: Assigned all modules (from CSV)")
        elif 'NONE' in [m.upper() for m in raw_modules]:
            # If CSV says NONE, no modules
            modules_list = []
            print(f"  ℹ️  {department}: No modules assigned (NONE)")
        else:
            # Map module names to IDs
            modules_list = []
            for raw_module in raw_modules:
                mapped = module_name_mapping.get(raw_module.upper(), None)
                if mapped == 'ALL':
                    # If we encounter ALL in mapping, use all modules
                    modules_list = available_modules.copy()
                    break
                elif mapped == 'NONE':
                    # Skip NONE
                    continue
                elif mapped and mapped in available_modules:
                    if mapped not in modules_list:
                        modules_list.append(mapped)
                else:
                    print(f"  ⚠️  {department}: Unknown module name '{raw_module}' - skipping")
            
            # Remove duplicates while preserving order
            seen = set()
            modules_list = [m for m in modules_list if not (m in seen or seen.add(m))]
        
        # Convert modules list to comma-separated string
        modules_str_final = ','.join(modules_list)
        
        try:
            # Check if department already exists
            existing = conn.execute(f'''
                SELECT permission_id FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                WHERE department = ?
            ''', [department]).fetchone()
            
            if existing:
                # Update existing
                conn.execute(f'''
                    UPDATE "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                    SET modules = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE department = ?
                ''', [modules_str_final, department])
                updated += 1
                print(f"  ✅ Updated: {department} ({len(modules_list)} modules)")
            else:
                # Insert new
                max_id_result = conn.execute(f'SELECT COALESCE(MAX(permission_id), 0) FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"').fetchone()
                next_id = (max_id_result[0] if max_id_result else 0) + 1
                
                conn.execute(f'''
                    INSERT INTO "{SCHEMA}"."DEPARTMENT_PERMISSIONS"
                    (permission_id, department, modules, created_at, updated_at)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ''', [next_id, department, modules_str_final])
                imported += 1
                print(f"  ✅ Imported: {department} ({len(modules_list)} modules: {', '.join(modules_list)})")
        except Exception as e:
            skipped += 1
            error_msg = f"Error processing {department}: {str(e)}"
            errors.append(error_msg)
            print(f"  ❌ {error_msg}")
    
    # Summary
    print("\n" + "=" * 70)
    print("Import Summary")
    print("=" * 70)
    print(f"✅ Imported: {imported} new permissions")
    print(f"🔄 Updated: {updated} existing permissions")
    print(f"⚠️  Skipped/Errors: {skipped} rows")
    
    if errors:
        print(f"\n❌ Errors encountered:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"   - {error}")
        if len(errors) > 10:
            print(f"   ... and {len(errors) - 10} more errors")
    
    # Verify import
    try:
        total_permissions = conn.execute(f'SELECT COUNT(*) FROM "{SCHEMA}"."DEPARTMENT_PERMISSIONS"').fetchone()[0]
        print(f"\n📊 Database Status:")
        print(f"   Total department permissions: {total_permissions}")
    except Exception as e:
        print(f"⚠️  Could not verify import: {e}")
    
    conn.close()
    print("\n✅ Import completed!")
    return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python import_department_permissions.py <csv_file>")
        print("Example: python import_department_permissions.py department_permissions.csv")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    success = import_department_permissions(csv_file)
    sys.exit(0 if success else 1)
