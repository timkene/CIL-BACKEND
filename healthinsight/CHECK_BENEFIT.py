"""
Script to fetch plan_procedure table from Medicloud database
"""

import pandas as pd
import pyodbc
import toml
import os

def load_secrets():
    secrets_path = "secrets.toml"
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    else:
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")

def get_sql_driver():
    """Get the appropriate SQL Server driver"""
    drivers = [x for x in pyodbc.drivers()]
    preferred = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server'
    ]
    for d in preferred:
        if d in drivers:
            return d
    raise RuntimeError("No compatible SQL Server driver found.")

def create_medicloud_connection():
    """Create connection to Medicloud database"""
    secrets = load_secrets()
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['credentials']['server']},{secrets['credentials']['port']};"
        f"DATABASE={secrets['credentials']['database']};"
        f"UID={secrets['credentials']['username']};"
        f"PWD={secrets['credentials']['password']};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def get_plan_procedure():
    """Fetch plan_procedure table from Medicloud database"""
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT TOP 1000 *
            FROM dbo.plan_procedure
            ORDER BY planid, procedurecode
        """
        
        print("🔍 Fetching plan_procedure data from Medicloud...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from plan_procedure")
        
        # Display all columns
        print("\n📋 All Columns in plan_procedure table:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")
        
        # Display summary
        print("\n📊 Summary:")
        print(f"   Total Rows: {len(df):,}")
        if 'planid' in df.columns:
            print(f"   Unique Plans: {df['planid'].nunique():,}")
        if 'procedurecode' in df.columns:
            print(f"   Unique Procedures: {df['procedurecode'].nunique():,}")
        
        # Show first few rows
        print("\n📋 First 10 rows:")
        print(df.head(10))
        
        # Show column data types
        print("\n📊 Column Data Types:")
        print(df.dtypes)
        
        return df
    
    except Exception as e:
        print(f"❌ Error loading plan_procedure: {str(e)}")
        return None

def get_plans():
    """Fetch plans table from Medicloud database"""
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.plans
            ORDER BY planid
        """
        
        print("🔍 Fetching plans data from Medicloud...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from plans")
        
        # Display all columns
        print("\n📋 All Columns in plans table:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")
        
        # Display summary
        print("\n📊 Summary:")
        print(f"   Total Rows: {len(df):,}")
        if 'planid' in df.columns:
            print(f"   Unique Plans: {df['planid'].nunique():,}")
        
        # Show first few rows
        print("\n📋 First 10 rows:")
        print(df.head(10))
        
        # Show column data types
        print("\n📊 Column Data Types:")
        print(df.dtypes)
        
        return df
    
    except Exception as e:
        print(f"❌ Error loading plans: {str(e)}")
        return None

def get_benefitcode():
    """Fetch benefitcode table from Medicloud database"""
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.benefitcode
            ORDER BY benefitcodeid
        """
        
        print("🔍 Fetching benefitcode data from Medicloud...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from benefitcode")
        
        # Display all columns
        print("\n📋 All Columns in benefitcode table:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")
        
        # Display summary
        print("\n📊 Summary:")
        print(f"   Total Rows: {len(df):,}")
        if 'benefitcodeid' in df.columns:
            print(f"   Unique Benefit Codes: {df['benefitcodeid'].nunique():,}")
        
        # Show first few rows
        print("\n📋 First 10 rows:")
        print(df.head(10))
        
        return df
    
    except Exception as e:
        print(f"❌ Error loading benefitcode: {str(e)}")
        return None

def get_claims():
    """Fetch claims table from Medicloud database (Sept 2024 to date)"""
    try:
        conn = create_medicloud_connection()
        
        # Query to get claims from September 2024 to date
        # Use specific columns like in dlt_sources.py
        query = """
            SELECT TOP 10000 
                nhislegacynumber, 
                nhisproviderid, 
                nhisgroupid, 
                panumber, 
                encounterdatefrom, 
                datesubmitted, 
                chargeamount, 
                approvedamount, 
                procedurecode, 
                deniedamount 
            FROM dbo.claims
            WHERE encounterdatefrom >= '2024-09-01'
            ORDER BY encounterdatefrom DESC
        """
        
        print("🔍 Fetching claims data from Medicloud (Sept 2024 to date)...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from claims")
        
        # Display all columns
        print("\n📋 All Columns in claims table:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i}. {col}")
        
        # Display summary
        print("\n📊 Summary:")
        print(f"   Total Rows: {len(df):,}")
        if 'encounterdatefrom' in df.columns:
            print(f"   Date Range: {df['encounterdatefrom'].min()} to {df['encounterdatefrom'].max()}")
        if 'planid' in df.columns:
            print(f"   Unique Plans: {df['planid'].nunique():,}")
        if 'memberid' in df.columns:
            print(f"   Unique Members: {df['memberid'].nunique():,}")
        
        # Show first few rows
        print("\n📋 First 10 rows:")
        print(df.head(10))
        
        # Show column data types
        print("\n📊 Column Data Types:")
        print(df.dtypes)
        
        return df
    
    except Exception as e:
        print(f"❌ Error loading claims: {str(e)}")
        print(f"   Trying to fetch all columns first to identify the date column...")
        try:
            # Try to get column info first
            conn = create_medicloud_connection()
            info_query = """
                SELECT TOP 1 *
                FROM dbo.claims
            """
            temp_df = pd.read_sql(info_query, conn)
            conn.close()
            
            print(f"\n📋 Available columns in claims table:")
            for col in temp_df.columns:
                print(f"   - {col}")
            
            # Try alternative date field names
            date_columns = ['encounterdatefrom', 'claimdate', 'createdate', 'date', 'submissiondate', 'treateddate', 'servicedate']
            found_date_col = None
            for col in date_columns:
                if col in temp_df.columns:
                    found_date_col = col
                    break
            
            if found_date_col:
                print(f"\n✅ Found date column: {found_date_col}")
                print(f"   Re-running query with {found_date_col}...")
                return get_claims_with_date_column(found_date_col)
            else:
                print("\n⚠️ No recognized date column found. Try to fetch without date filter.")
                return temp_df
        except Exception as e2:
            print(f"❌ Error in retry: {str(e2)}")
            return None

def get_claims_with_date_column(date_column):
    """Fetch claims with specific date column"""
    try:
        conn = create_medicloud_connection()
        
        query = f"""
            SELECT TOP 10000 *
            FROM dbo.claims
            WHERE {date_column} >= '2024-09-01'
            ORDER BY {date_column} DESC
        """
        
        print(f"🔍 Fetching claims with date filter on {date_column}...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from claims")
        return df
    
    except Exception as e:
        print(f"❌ Error loading claims with date filter: {str(e)}")
        return None

def get_merged_data():
    """Get merged plan_procedure with planname, plancode, and benefitcodedesc"""
    print("\n" + "="*70)
    print("MERGE: Adding planname, plancode, and benefitcodedesc to plan_procedure")
    print("="*70)
    
    # Get all three tables
    plan_procedure_df = get_plan_procedure()
    plans_df = get_plans()
    benefitcode_df = get_benefitcode()
    
    if plan_procedure_df is None or plans_df is None:
        print("\n⚠️ Cannot merge - missing data")
        return None
    
    # Step 1: Merge plan_procedure with plans
    plans_subset = plans_df[['planid', 'planname', 'plancode']].rename(columns={'plancode': 'plan_code'})
    
    merged_df = plan_procedure_df.merge(
        plans_subset, 
        on='planid', 
        how='left'
    )
    
    print(f"\n✅ Step 1: Merged plan_procedure with plans")
    print(f"   Rows after merging plans: {len(merged_df):,}")
    
    # Step 2: Merge with benefitcode (if available)
    if benefitcode_df is not None and not benefitcode_df.empty:
        benefitcode_subset = benefitcode_df[['benefitcodeid', 'benefitcodedesc']].copy()
        
        merged_df = merged_df.merge(
            benefitcode_subset,
            on='benefitcodeid',
            how='left'
        )
        
        print(f"✅ Step 2: Merged with benefitcode")
        print(f"   Final rows: {len(merged_df):,}")
    
    # Display summary of merged columns
    print("\n📋 Final Merged Data Columns:")
    for i, col in enumerate(merged_df.columns, 1):
        print(f"   {i}. {col}")
    
    # Show first few rows with new columns
    print("\n📋 First 5 rows of merged data:")
    display_cols = ['planid', 'planname', 'plan_code', 'procedurecode', 'benefitcodeid', 'benefitcodedesc']
    available_cols = [col for col in display_cols if col in merged_df.columns]
    print(merged_df[available_cols].head())
    
    return merged_df

def export_tables():
    """Export all tables to Excel, with merged version"""
    # Get raw tables
    plan_procedure_df = get_plan_procedure()
    plans_df = get_plans()
    benefitcode_df = get_benefitcode()
    claims_df = get_claims()  # Get claims (Sept 2024 to date)
    
    # Get merged data
    merged_df = get_merged_data()
    
    if plan_procedure_df is not None or plans_df is not None or benefitcode_df is not None or claims_df is not None or merged_df is not None:
        output_file = f'medicloud_tables_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if merged_df is not None and not merged_df.empty:
                merged_df.to_excel(writer, sheet_name='plan_procedure_merged', index=False)
                print(f"\n✅ Exported merged plan_procedure to sheet: plan_procedure_merged")
            
            if claims_df is not None and not claims_df.empty:
                claims_df.to_excel(writer, sheet_name='claims', index=False)
                print(f"✅ Exported claims (Sept 2024 to date) to sheet: claims")
            
            if plan_procedure_df is not None and not plan_procedure_df.empty:
                plan_procedure_df.to_excel(writer, sheet_name='plan_procedure', index=False)
                print(f"✅ Exported plan_procedure to sheet: plan_procedure")
            
            if plans_df is not None and not plans_df.empty:
                plans_df.to_excel(writer, sheet_name='plans', index=False)
                print(f"✅ Exported plans to sheet: plans")
            
            if benefitcode_df is not None and not benefitcode_df.empty:
                benefitcode_df.to_excel(writer, sheet_name='benefitcode', index=False)
                print(f"✅ Exported benefitcode to sheet: benefitcode")
        
        print(f"\n✅ All tables exported to: {output_file}")
        print(f"\n📊 NEW DATAFRAME NAME: 'plan_procedure_merged'")
        print(f"   This is your enriched dataset with:")
        print(f"   - plan_procedure data")
        print(f"   - planname (from plans)")
        print(f"   - plan_code (from plans)")
        print(f"   - benefitcodedesc (from benefitcode)")
        print(f"   Total columns: {len(merged_df.columns) if merged_df is not None else 0}")
        if claims_df is not None:
            print(f"\n📊 CLAIMS: {len(claims_df):,} rows (Sept 2024 to date)")
        
        return output_file, merged_df, claims_df
    else:
        print("\n⚠️ No data to export")
        return None, None, None

def get_proceduredata():
    """Fetch proceduredata table from Medicloud database"""
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT procedurecode, proceduredesc
            FROM dbo.proceduredata
        """
        
        print("🔍 Fetching proceduredata from Medicloud...")
        df = pd.read_sql(query, conn)
        conn.close()
        
        print(f"✅ Successfully loaded {len(df):,} rows from proceduredata")
        return df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load proceduredata: {str(e)}")
        return None

def analyze_unmapped_procedures():
    """Analyze top 200 procedurecodes from claims and find which plancodes don't have them mapped"""
    print("\n" + "="*70)
    print("ANALYZING: Top 200 ProcedureCodes and their mapping status")
    print("="*70)
    
    # Get all data
    plan_procedure_df = get_plan_procedure()
    plans_df = get_plans()
    claims_df = get_claims()
    
    # Get procedure descriptions
    procedure_df = get_proceduredata()
    procedure_dict = {}
    if procedure_df is not None and not procedure_df.empty:
        procedure_dict = dict(zip(procedure_df['procedurecode'], procedure_df['proceduredesc']))
        print(f"✅ Loaded procedure descriptions for {len(procedure_dict):,} codes")
    
    if plan_procedure_df is None or plans_df is None or claims_df is None:
        print("❌ Missing required data. Cannot perform analysis.")
        return None
    
    # Step 1: Get top 200 procedurecodes from claims by count
    print("\n📊 Step 1: Finding top 200 procedurecodes from claims...")
    procedurecode_counts = claims_df['procedurecode'].value_counts().head(200)
    top_200_procedures = procedurecode_counts.index.tolist()
    print(f"✅ Found top 200 procedurecodes")
    print(f"   Total claims analyzed: {len(claims_df):,}")
    
    # Step 2: Get unique procedurecodes from plan_procedure (mapped ones)
    print("\n📊 Step 2: Getting mapped procedurecodes from plan_procedure...")
    mapped_procedures = set(plan_procedure_df['procedurecode'].unique())
    print(f"✅ Found {len(mapped_procedures):,} mapped procedurecodes in plan_procedure")
    
    # Step 3: Find unmapped procedurecodes
    print("\n📊 Step 3: Identifying unmapped procedurecodes...")
    unmapped_procedures = [proc for proc in top_200_procedures if proc not in mapped_procedures]
    print(f"✅ Found {len(unmapped_procedures):,} unmapped procedurecodes")
    
    # Step 4: For each unmapped procedurecode, find all plancodes that don't have it
    print("\n📊 Step 4: Finding plancodes missing each unmapped procedurecode...")
    
    # Get all unique plancodes from plans
    all_plancodes = plans_df[['planid', 'plancode', 'planname']].drop_duplicates()
    print(f"   Total unique plans: {len(all_plancodes):,}")
    
    # Get mapped plan-procedure pairs
    mapped_plan_procedure = plan_procedure_df[['planid', 'procedurecode']].drop_duplicates()
    
    # For each unmapped procedurecode, find all plancodes (since it's unmapped, no plan has it)
    result_data = {}
    
    for proc_code in unmapped_procedures:
        # Since this procedurecode is unmapped, ALL plans don't have it
        plans_without_this_proc = all_plancodes.copy()
        
        if len(plans_without_this_proc) > 0:
            result_data[proc_code] = plans_without_this_proc.copy()
            
            # Add claim count for reference
            claim_count = procedurecode_counts[proc_code]
            result_data[proc_code]['claim_count'] = claim_count
    
    print(f"✅ Analysis complete. Found {len(result_data)} unmapped procedurecodes with missing plancodes")
    
    # Step 5: Export to Excel
    if len(result_data) > 0:
        output_file = f'unmapped_procedures_analysis_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
        print(f"\n📝 Exporting results to {output_file}...")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            for i, (proc_code, data) in enumerate(result_data.items(), 1):
                # Use procedurecode as sheet name (Excel has 31 char limit)
                sheet_name = str(proc_code)[:31] if len(str(proc_code)) > 31 else str(proc_code)
                
                # Get procedure description
                proc_desc = procedure_dict.get(proc_code, 'Not found')
                
                # Add summary info at the top
                summary_df = pd.DataFrame({
                    'Info': [
                        f'Procedure Code: {proc_code}',
                        f'Procedure Name: {proc_desc}',
                        f'Total Plans Missing This Procedure: {len(data)}',
                        f'Claim Count (Top 200): {data["claim_count"].iloc[0] if "claim_count" in data else "N/A"}'
                    ]
                })
                
                # Get the plans data without the claim_count column
                export_data = data[['planid', 'plancode', 'planname']].copy() if 'claim_count' in data else data
                
                # Write summary and data
                summary_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0)
                export_data.to_excel(writer, sheet_name=sheet_name, index=False, startrow=5)
                
                if i % 50 == 0:
                    print(f"   Processed {i}/{len(result_data)} sheets...")
        
        print(f"\n✅ Analysis complete!")
        print(f"📄 File: {output_file}")
        print(f"📊 Total unmapped procedurecodes: {len(result_data)}")
        print(f"📋 Total sheets created: {len(result_data)}")
        
        return output_file, result_data
    else:
        print("\n✅ All top 200 procedurecodes are mapped!")
        return None, None

def analyze_unmapped_top500_export_single():
    """Top 500 claims procedurecodes not mapped to any plancode (excluding planids 1522, 1533).
    Export a single-sheet Excel with columns: plancode, Procedurecode, benefitcodeid.
    """
    print("\n" + "="*70)
    print("ANALYZING: Top 500 ProcedureCodes unmapped to any plancode (excl. 1522,1533)")
    print("="*70)

    claims_df = get_claims()
    merged_df = get_merged_data()
    if claims_df is None or merged_df is None or merged_df.empty:
        print("❌ Missing data for analysis.")
        return None

    # Exclude specified planids
    excluded_planids = {1522, 1533, '1522', '1533'}
    merged_filtered = merged_df[~merged_df['planid'].isin(excluded_planids)].copy()

    # Top 500 procedurecodes by count in claims
    top500 = claims_df['procedurecode'].value_counts().head(500).index.tolist()

    # Determine codes not present in any plancode (after exclusions)
    mapped_codes = set(merged_filtered['procedurecode'].dropna().unique())
    unmapped_codes = [code for code in top500 if code not in mapped_codes]

    # Build export DataFrame: one row per unmapped code; plancode/benefitcodeid empty
    export_rows = []
    for code in unmapped_codes:
        export_rows.append({
            'plancode': None,
            'Procedurecode': code,
            'benefitcodeid': None
        })

    result_df = pd.DataFrame(export_rows, columns=['plancode', 'Procedurecode', 'benefitcodeid'])

    out_file = f'unmapped_top500_procedurecodes_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
    result_df.to_excel(out_file, index=False)
    print(f"✅ Exported unmapped top 500 to {out_file}")
    print(f"   Total unmapped: {len(result_df)} (of 500)")
    return out_file

def analyze_unmapped_top500_from_excel(excel_path: str = None):
    """Offline analysis: read from medicloud_tables_*.xlsx and export single-sheet
    with columns [plancode, Procedurecode, benefitcodeid] for top 500 claims procedurecodes
    that are not present in any plancode (excluding planids 1522, 1533).
    """
    import glob
    from pathlib import Path

    # Locate latest medicloud_tables_*.xlsx if not provided
    if not excel_path:
        candidates = sorted(glob.glob('medicloud_tables_*.xlsx'))
        if not candidates:
            print("❌ No medicloud_tables_*.xlsx file found in the current directory.")
            return None
        excel_path = candidates[-1]

    print(f"\n📄 Using source workbook: {excel_path}")
    xls = pd.ExcelFile(excel_path)

    # Read sheets
    if 'claims' not in xls.sheet_names:
        print("❌ 'claims' sheet not found in workbook.")
        return None
    claims_df = pd.read_excel(excel_path, sheet_name='claims')

    merged_sheet = 'plan_procedure_merged' if 'plan_procedure_merged' in xls.sheet_names else None
    if not merged_sheet:
        print("❌ 'plan_procedure_merged' sheet not found in workbook.")
        return None
    merged_df = pd.read_excel(excel_path, sheet_name=merged_sheet)

    # Ensure required columns exist
    required_claims_cols = {'procedurecode'}
    required_merged_cols = {'planid', 'plancode', 'procedurecode', 'benefitcodeid'}
    if not required_claims_cols.issubset(claims_df.columns) or not required_merged_cols.issubset(merged_df.columns):
        print("❌ Required columns missing in sheets.")
        return None

    # Compute top 500 procedurecodes by count
    top500 = claims_df['procedurecode'].value_counts().head(500).index.tolist()

    # Exclude planids 1522, 1533
    excluded_planids = {1522, 1533, '1522', '1533'}
    merged_filtered = merged_df[~merged_df['planid'].isin(excluded_planids)].copy()

    # Define mapped codes as those with any non-null plancode row for that procedurecode
    has_plancode = merged_filtered.dropna(subset=['plancode'])
    mapped_codes = set(has_plancode['procedurecode'].dropna().unique())

    unmapped_codes = [code for code in top500 if code not in mapped_codes]

    # Build export DataFrame
    rows = []
    for code in unmapped_codes:
        # benefitcodeid not meaningful since unmapped; leave None
        rows.append({'plancode': None, 'Procedurecode': code, 'benefitcodeid': None})

    result_df = pd.DataFrame(rows, columns=['plancode', 'Procedurecode', 'benefitcodeid'])

    out_file = f'unmapped_top500_procedurecodes_offline_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
    result_df.to_excel(out_file, index=False)
    print(f"✅ Exported: {out_file}  (rows: {len(result_df)})")
    return out_file

def flatten_unmapped_workbook_to_single_sheet(source_path: str, output_path: str = None):
    """Read the per-procedure unmapped workbook and flatten into a single sheet
    with columns [plancode, Procedurecode, benefitcodeid], leaving benefitcodeid empty.
    Excludes planids 1522 and 1533.
    """
    import pandas as pd
    import numpy as np
    from pathlib import Path

    if not Path(source_path).exists():
        print(f"❌ Source workbook not found: {source_path}")
        return None

    xls = pd.ExcelFile(source_path)
    rows = []
    excluded_planids = {1522, 1533, '1522', '1533'}

    for sheet in xls.sheet_names:
        # Each sheet has summary on first 5 rows, then table starting at row 6 (0-indexed startrow=5)
        df = pd.read_excel(source_path, sheet_name=sheet, header=5)
        # Expect columns include planid, plancode, planname
        if 'plancode' not in df.columns:
            # Try to auto-detect header by finding the row where 'plancode' appears
            df_tmp = pd.read_excel(source_path, sheet_name=sheet, header=None)
            header_row_idx = None
            for i in range(min(15, len(df_tmp))):
                row = df_tmp.iloc[i].astype(str).str.lower()
                if 'plancode' in set(row):
                    header_row_idx = i
                    break
            if header_row_idx is not None:
                df = pd.read_excel(source_path, sheet_name=sheet, header=header_row_idx)
            else:
                continue

        # Clean
        cols = [c.strip() if isinstance(c, str) else c for c in df.columns]
        df.columns = cols
        df = df.dropna(subset=['plancode'], how='all')

        # Exclude planids 1522 and 1533
        if 'planid' in df.columns:
            df = df[~df['planid'].isin(excluded_planids)]

        # Append rows
        for plancode in df['plancode'].dropna().astype(str):
            rows.append({'plancode': plancode, 'Procedurecode': sheet, 'benefitcodeid': None})

    result = pd.DataFrame(rows, columns=['plancode', 'Procedurecode', 'benefitcodeid'])
    if output_path is None:
        output_path = f'unmapped_top500_procedurecodes_from_workbook_{pd.Timestamp.now().strftime("%Y%m%d")}.xlsx'
    result.to_excel(output_path, index=False)
    print(f"✅ Flattened to: {output_path} (rows: {len(result)})")
    return output_path

if __name__ == '__main__':
    print("="*70)
    print("MEDICLOUD TABLES CHECKER")
    print("Fetching: plan_procedure, plans, benefitcode, and claims")
    print("Will merge to add planname, plancode, and benefitcodedesc")
    print("Claims will be filtered to Sept 2024 to date")
    print("="*70)
    print()
    
    # Automatically export tables and run analysis
    print("\n" + "="*70)
    print("Exporting tables to Excel and running analysis...")
    print("="*70)
    
    file_path, merged_df, claims_df = export_tables()
    
    if merged_df is not None and claims_df is not None:
        print("\n" + "="*70)
        print("READY FOR ANALYSIS!")
        print(f"Total merged rows: {len(merged_df):,}")
        print(f"Total claims rows: {len(claims_df):,}")
        print("="*70)
        
        # Automatically run analysis
        print("\nRunning unmapped procedurecode analysis...")
        analyze_unmapped_procedures()
        
        print("\nRunning unmapped TOP 500 export (single sheet)...")
        analyze_unmapped_top500_export_single()
    
    print("\n" + "="*70)
    print("Done!")
    print("="*70)