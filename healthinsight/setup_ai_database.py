import duckdb
import pandas as pd
import os
from dlt_sources import total_pa_procedures, claims, all_providers, all_group, all_active_member, benefitcode, benefitcode_procedure, group_plan, pa_issue_request, proceduredata, e_account_group, tbpadiagnosis, tariff as tariff_src, providers_tariff as providers_tariff_src, fin_gl as fin_gl_src, premium1_schedule as premium1_schedule_src, debit_note as debit_note_src
import streamlit as st

def clean_existing_databases():
    """Delete existing DuckDB files to start fresh"""
    db_files = [
        'dashboard_pipeline.duckdb',
        'dashboard_pipeline_backup.duckdb', 
        'excel_to_duckdb.duckdb',
        'ai_driven_data.duckdb'
    ]
    
    deleted_files = []
    for db_file in db_files:
        if os.path.exists(db_file):
            os.remove(db_file)
            deleted_files.append(db_file)
            print(f"✅ Deleted {db_file}")
    
    if not deleted_files:
        print("ℹ️ No existing DuckDB files found to delete")
    else:
        print(f"🧹 Cleaned up {len(deleted_files)} existing database files")

def create_ai_database():
    """Create new DuckDB database with AI DRIVEN DATA schema"""
    print("🚀 Creating new AI DRIVEN DATA database...")
    
    # Connect to new database
    conn = duckdb.connect('ai_driven_data.duckdb')
    
    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS \"AI DRIVEN DATA\"")
    print("✅ Created 'AI DRIVEN DATA' schema")
    
    return conn

def load_pa_data(conn):
    """Load total_pa_procedures data as PA DATA table"""
    print("📊 Loading PA DATA from total_pa_procedures...")
    
    try:
        # Get data from dlt_sources
        pa_data = list(total_pa_procedures())[0]  # Get the DataFrame from the generator
        
        if pa_data.empty:
            print("⚠️ No PA data found - creating sample data structure")
            # Create sample data if no real data available
            pa_data = create_sample_pa_data()
        
        print(f"✅ Loaded {len(pa_data)} rows of PA data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        pa_data = create_sample_pa_data()
    
    # Create table in AI DRIVEN DATA schema (matching updated real data structure)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PA DATA" (
            panumber VARCHAR,
            groupname VARCHAR,
            divisionname VARCHAR,
            plancode VARCHAR,
            IID VARCHAR,
            providerid VARCHAR,
            requestdate TIMESTAMP,
            pastatus VARCHAR,
            code VARCHAR,
            userid VARCHAR,
            totaltariff DOUBLE,
            benefitcode VARCHAR,
            dependantnumber VARCHAR,
            requested DOUBLE,
            granted DOUBLE
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"PA DATA\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."PA DATA" 
        SELECT * FROM pa_data
    """)
    
    print("✅ Successfully loaded PA DATA into DuckDB")
    
    # Add table documentation
    add_table_documentation(conn, "PA DATA")
    
    return pa_data

def create_sample_pa_data():
    """Create sample PA data if database connection fails"""
    import numpy as np
    from datetime import datetime, timedelta
    
    sample_data = {
        'panumber': [f'PA{str(i).zfill(6)}' for i in range(1, 21)],
        'groupname': ['ACME Corp', 'Tech Solutions Ltd', 'Healthcare Plus', 'Global Industries', 'MediCare Group'] * 4,
        'divisionname': ['Lagos', 'Abuja', 'Kano', 'Lagos', 'Abuja'] * 4,
        'plancode': ['PLAN001', 'PLAN002', 'PLAN003', 'PLAN001', 'PLAN002'] * 4,
        'IID': [f'IID{str(i).zfill(4)}' for i in range(1, 21)],
        'providerid': [f'PROV{str(i).zfill(3)}' for i in range(1, 21)],
        'requestdate': [datetime.now() - timedelta(days=i*2) for i in range(20)],
        'pastatus': ['APPROVED', 'PENDING', 'REJECTED', 'APPROVED', 'PENDING'] * 4,
        'code': [f'PROC{str(i).zfill(3)}' for i in range(1, 21)],
        'userid': [f'USER{str(i).zfill(3)}' for i in range(1, 21)],
        'totaltariff': [np.random.uniform(1000, 50000) for _ in range(20)],
        'benefitcode': [f'BEN{str(i).zfill(3)}' for i in range(1, 21)],
        'dependantnumber': [f'DEP{str(i).zfill(3)}' for i in range(1, 21)],
        'requested': [np.random.uniform(500, 25000) for _ in range(20)],
        'granted': [np.random.uniform(400, 20000) for _ in range(20)]
    }
    
    return pd.DataFrame(sample_data)

def add_table_documentation(conn, table_name):
    """Add documentation for the table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Pre-Authorization (PA) procedures data with authorization codes, client info, and procedure details
    
    -- Column Documentation (Complete Real Data Structure - 15 columns):
    -- panumber: Unique authorization code for each PA request (can appear multiple times for multiple procedures)
    -- groupname: Client/company name (unique per client, can be used as key with other tables)
    -- divisionname: Geographic division of the client (not unique, multiple clients can share same division)
    -- plancode: Insurance plan code for enrolled members (unique per plan)
    -- IID: Unique identifier for each enrollee/member (unique per person)
    -- providerid: Unique identifier for healthcare provider/hospital (can be joined with provider table)
    -- requestdate: Date when authorization was granted (same for all procedures under same panumber)
    -- pastatus: Status of the PA request (AUTHORIZED, PENDING, REJECTED)
    -- code: Procedure code (unique per procedure, can be joined with procedure table)
    -- userid: Agent/authorizer who granted the authorization (unique per agent)
    -- totaltariff: Total tariff amount for the procedure
    -- benefitcode: Benefit code associated with the procedure
    -- dependantnumber: Dependent member code (0=principal, 1=spouse, 2-5=children, max 5 dependents)
    -- requested: Amount requested for the procedure
    -- granted: Amount actually authorized/granted for the procedure
    
    -- Note: This table contains {len(pa_data) if 'pa_data' in locals() else 'unknown'} rows of real PA data from 2022-2025
    """
    
    # Store documentation in a comment table
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."table_documentation" (
            table_name VARCHAR,
            documentation TEXT
        )
    """)
    
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def verify_database():
    """Verify the database was created correctly"""
    conn = duckdb.connect('ai_driven_data.duckdb')
    
    print("\n🔍 Verifying database setup...")
    
    # Check schema exists
    try:
        schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
        print(f"📋 Available schemas: {[s[0] for s in schemas]}")
    except:
        print("📋 Using default schema")
    
    # Check tables in AI DRIVEN DATA schema
    try:
        tables = conn.execute('SHOW TABLES FROM "AI DRIVEN DATA"').fetchall()
        print(f"📊 Tables in AI DRIVEN DATA schema: {[t[0] for t in tables]}")
    except:
        # Try alternative method
        tables = conn.execute('SELECT table_name FROM information_schema.tables WHERE table_schema = \'AI DRIVEN DATA\'').fetchall()
        print(f"📊 Tables in AI DRIVEN DATA schema: {[t[0] for t in tables]}")
    
    # Check PA DATA table
    try:
        row_count = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."PA DATA"').fetchone()[0]
        print(f"📈 PA DATA table has {row_count} rows")
        
        # Show sample data
        sample = conn.execute('SELECT * FROM "AI DRIVEN DATA"."PA DATA" LIMIT 3').fetchall()
        print(f"🔍 Sample data (first 3 rows):")
        for i, row in enumerate(sample, 1):
            print(f"   Row {i}: {row[:5]}...")  # Show first 5 columns
    except Exception as e:
        print(f"⚠️ Error checking PA DATA table: {e}")
    
    conn.close()
    print("✅ Database verification complete")

def load_fin_gl(conn):
    print("📊 Loading FIN_GL from fin_gl...")
    try:
        df = list(fin_gl_src())[0]
        print(f"✅ Loaded {len(df):,} rows from FIN_GL source")
    except Exception as e:
        print(f"⚠️ Failed to fetch FIN_GL: {e}")
        df = pd.DataFrame(columns=["GLCode","Description","Amount","Date"])  # fallback columns

    conn.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
    conn.execute('CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."FIN_GL" (GLCode VARCHAR, Description VARCHAR, Amount DOUBLE, Date TIMESTAMP)')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."FIN_GL"')
    conn.register("df_src", df)
    try:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."FIN_GL" SELECT GLCode, Description, Amount, Date FROM df_src')
    except Exception:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."FIN_GL" SELECT GLCode, Description, Amount, try_cast(Date as TIMESTAMP) FROM df_src')
    print("✅ FIN_GL loaded")

def load_premium1_schedule(conn):
    print("📊 Loading PREMIUM1_SCHEDULE from premium1_schedule...")
    try:
        df = list(premium1_schedule_src())[0]
        print(f"✅ Loaded {len(df):,} rows from PREMIUM1_SCHEDULE source")
    except Exception as e:
        print(f"⚠️ Failed to fetch PREMIUM1_SCHEDULE: {e}")
        df = pd.DataFrame(columns=["ScheduleID","Premium","Date"])  # fallback columns

    conn.execute('CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PREMIUM1_SCHEDULE" (ScheduleID VARCHAR, Premium DOUBLE, Date TIMESTAMP)')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."PREMIUM1_SCHEDULE"')
    conn.register("df_src", df)
    try:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."PREMIUM1_SCHEDULE" SELECT ScheduleID, Premium, Date FROM df_src')
    except Exception:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."PREMIUM1_SCHEDULE" SELECT ScheduleID, Premium, try_cast(Date as TIMESTAMP) FROM df_src')
    print("✅ PREMIUM1_SCHEDULE loaded")

def load_debit_note(conn):
    print("📊 Loading DEBIT_NOTE from debit_note...")
    try:
        df = list(debit_note_src())[0]
        print(f"✅ Loaded {len(df):,} rows from DEBIT_NOTE source")
    except Exception as e:
        print(f"⚠️ Failed to fetch DEBIT_NOTE: {e}")
        df = pd.DataFrame(columns=["From","To","Description","Amount","CompanyName"])  # fallback

    conn.execute('CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."DEBIT_NOTE" ("From" TIMESTAMP, "To" TIMESTAMP, Description VARCHAR, Amount DOUBLE, CompanyName VARCHAR)')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."DEBIT_NOTE"')
    conn.register("df_src", df)
    try:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."DEBIT_NOTE" SELECT "From", "To", Description, Amount, CompanyName FROM df_src')
    except Exception:
        conn.execute('INSERT INTO "AI DRIVEN DATA"."DEBIT_NOTE" SELECT try_cast("From" as TIMESTAMP), try_cast("To" as TIMESTAMP), Description, try_cast(Amount as DOUBLE), CompanyName FROM df_src')
    print("✅ DEBIT_NOTE loaded")

def build_salary_and_expenses(conn):
    print("🧮 Building derived SALARY_AND_EXPENSES ...")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."SALARY_AND_EXPENSES" (
            glcode VARCHAR,
            description VARCHAR,
            amount DOUBLE,
            txn_date TIMESTAMP,
            category VARCHAR
        )
    ''')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."SALARY_AND_EXPENSES"')
    conn.execute('''
        INSERT INTO "AI DRIVEN DATA"."SALARY_AND_EXPENSES"
        SELECT 
            GLCode,
            Description,
            Amount,
            Date as txn_date,
            CASE 
                WHEN lower(coalesce(Description,'')) LIKE '%salary%'
                  OR lower(coalesce(Description,'')) LIKE '%wage%'
                  OR lower(coalesce(Description,'')) LIKE '%payroll%'
                THEN 'SALARY' ELSE 'EXPENSE' END AS category
        FROM "AI DRIVEN DATA"."FIN_GL"
    ''')
    print("✅ SALARY_AND_EXPENSES built")

def build_client_cash_received(conn):
    print("🧮 Building derived CLIENT_CASH_RECEIVED ...")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED" (
            schedule_id VARCHAR,
            amount DOUBLE,
            received_date TIMESTAMP
        )
    ''')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"')
    conn.execute('''
        INSERT INTO "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
        SELECT ScheduleID, Premium, Date FROM "AI DRIVEN DATA"."PREMIUM1_SCHEDULE"
    ''')
    print("✅ CLIENT_CASH_RECEIVED built")

def build_debit_note_accrued(conn):
    print("🧮 Building derived DEBIT_NOTE_ACCRUED ...")
    conn.execute('''
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."DEBIT_NOTE_ACCRUED" (
            company_name VARCHAR,
            description VARCHAR,
            amount DOUBLE,
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            accrual_date TIMESTAMP
        )
    ''')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."DEBIT_NOTE_ACCRUED"')
    conn.execute('''
        INSERT INTO "AI DRIVEN DATA"."DEBIT_NOTE_ACCRUED"
        SELECT 
            CompanyName,
            Description,
            Amount,
            "From"  AS period_start,
            "To"    AS period_end,
            COALESCE("To","From") AS accrual_date
        FROM "AI DRIVEN DATA"."DEBIT_NOTE"
    ''')
    print("✅ DEBIT_NOTE_ACCRUED built")

def load_tbpadiagnosis_data(conn):
    """Load tbpadiagnosis as TBPADIAGNOSIS table"""
    print("📊 Loading TBPADIAGNOSIS from tbpadiagnosis...")
    try:
        df = list(tbpadiagnosis())[0]
        print(f"✅ Loaded {len(df):,} rows of TBPADIAGNOSIS")
    except Exception as e:
        print(f"⚠️ Failed to fetch tbpadiagnosis: {e}")
        df = pd.DataFrame(columns=["uniqueid","code","desc","panumber"])  # empty frame with expected cols

    conn.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
    conn.execute('DROP TABLE IF EXISTS "AI DRIVEN DATA"."TBPADIAGNOSIS"')
    conn.register("df_src", df)
    conn.execute('CREATE TABLE "AI DRIVEN DATA"."TBPADIAGNOSIS" AS SELECT * FROM df_src')
    print("✅ Successfully loaded TBPADIAGNOSIS into DuckDB")

def load_tariff_table(conn):
    """Load tariff resource as TARIFF table"""
    print("📊 Loading TARIFF from tariff...")
    try:
        df = list(tariff_src())[0]
        print(f"✅ Loaded {len(df):,} rows of tariff")
    except Exception as e:
        print(f"⚠️ Failed to fetch tariff: {e}")
        df = pd.DataFrame(columns=["tariffid","tariffname","tariffamount","procedurecode","comments","expirydate"])

    conn.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
    conn.execute('CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."TARIFF" (tariffid VARCHAR, tariffname VARCHAR, tariffamount DOUBLE, procedurecode VARCHAR, comments VARCHAR, expirydate TIMESTAMP)')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."TARIFF"')
    conn.register("df_src", df)
    conn.execute('INSERT INTO "AI DRIVEN DATA"."TARIFF" SELECT * FROM df_src')
    print("✅ Successfully loaded TARIFF into DuckDB")

def load_providers_tariff_table(conn):
    """Load providers_tariff resource as PROVIDERS_TARIFF table"""
    print("📊 Loading PROVIDERS_TARIFF from providers_tariff...")
    try:
        df = list(providers_tariff_src())[0]
        print(f"✅ Loaded {len(df):,} rows of providers_tariff")
    except Exception as e:
        print(f"⚠️ Failed to fetch providers_tariff: {e}")
        df = pd.DataFrame(columns=["tariffprovid","tariffid","providerid","tariffdescr","dateadded"])

    conn.execute('CREATE SCHEMA IF NOT EXISTS "AI DRIVEN DATA"')
    conn.execute('CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROVIDERS_TARIFF" (tariffprovid VARCHAR, tariffid VARCHAR, providerid VARCHAR, tariffdescr VARCHAR, dateadded TIMESTAMP)')
    conn.execute('DELETE FROM "AI DRIVEN DATA"."PROVIDERS_TARIFF"')
    conn.register("df_src", df)
    conn.execute('INSERT INTO "AI DRIVEN DATA"."PROVIDERS_TARIFF" SELECT * FROM df_src')
    print("✅ Successfully loaded PROVIDERS_TARIFF into DuckDB")

def load_pa_issue_request(conn):
    """Load pa_issue_request data as PA ISSUE REQUEST table"""
    print("📊 Loading PA ISSUE REQUEST from pa_issue_request...")

    try:
        df = list(pa_issue_request())[0]

        if df.empty:
            print("⚠️ No PA issue request data found - creating sample structure")
            df = create_sample_pa_issue_request()
        else:
            # Standardize/rename columns
            column_mapping = {
                'PANumber': 'panumber',
                'RequestDate': 'requestdate',
                'EncounterDate': 'encounterdate',
                'ResolutionTime': 'resolutiontime',
                'Providerid': 'providerid',
                'DateAdded': 'dateadded'
            }
            df = df.rename(columns=column_mapping)

            # Filter invalid rows: require panumber and resolutiontime
            required_cols = ['panumber', 'resolutiontime']
            for c in required_cols:
                if c not in df.columns:
                    df[c] = None
            before = len(df)
            df = df.dropna(subset=['panumber', 'resolutiontime'])
            print(f"✅ Filtered invalid rows: {before-len(df)} removed, {len(df)} remaining")

        print(f"✅ Loaded {len(df):,} rows of PA ISSUE REQUEST data")

    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        df = create_sample_pa_issue_request()

    # Create table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PA ISSUE REQUEST" (
            panumber VARCHAR,
            requestdate TIMESTAMP,
            encounterdate TIMESTAMP,
            resolutiontime TIMESTAMP,
            providerid VARCHAR,
            dateadded TIMESTAMP
        )
    """)

    # Clear and insert
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"PA ISSUE REQUEST\"")
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."PA ISSUE REQUEST"
        SELECT panumber, requestdate, encounterdate, resolutiontime, providerid, dateadded
        FROM df
    """)

    print("✅ Successfully loaded PA ISSUE REQUEST into DuckDB")

    add_pa_issue_request_documentation(conn, "PA ISSUE REQUEST", df)

    return df

def create_sample_pa_issue_request():
    from datetime import datetime, timedelta
    sample = {
        'panumber': ['PA000001', 'PA000002', 'PA000003'],
        'requestdate': [datetime.now()-timedelta(days=3), datetime.now()-timedelta(days=2), datetime.now()-timedelta(days=1)],
        'encounterdate': [datetime.now()-timedelta(days=3, hours=1), datetime.now()-timedelta(days=2, hours=2), datetime.now()-timedelta(days=1, hours=3)],
        'resolutiontime': [datetime.now()-timedelta(days=3, hours=-2), datetime.now()-timedelta(days=2, hours=-1), datetime.now()],
        'providerid': ['977', '123', '456'],
        'dateadded': [datetime.now()-timedelta(days=3), datetime.now()-timedelta(days=2), datetime.now()-timedelta(days=1)]
    }
    return pd.DataFrame(sample)

def add_pa_issue_request_documentation(conn, table_name, df):
    """Add documentation for PA ISSUE REQUEST table"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Online PA requests only (subset of PA); used to measure online vs offline and response times

    -- Column Documentation:
    -- panumber: Online authorization number (same key as panumber in PA DATA/CLAIMS DATA)
    -- requestdate: Date the request was made (aligns with PA DATA.requestdate)
    -- encounterdate: Timestamp the request was initiated (used for response-time calc)
    -- resolutiontime: Timestamp the request was closed (used for response-time calc)
    -- providerid: Provider submitting the online request (join to PROVIDERS)
    -- dateadded: Record creation timestamp

    -- Data Quality Rules:
    -- - Rows must have both panumber and resolutiontime; otherwise excluded
    -- - Only represents requests submitted online

    -- Key Relationships:
    -- - Links to PA DATA via panumber
    -- - Links to PROVIDERS via providerid

    -- Note: This table contains {len(df):,} rows of online PA requests
    """

    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation"
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_claims_data(conn):
    """Load claims data as CLAIMS DATA table"""
    print("📊 Loading CLAIMS DATA from claims...")
    
    try:
        # Get data from dlt_sources
        claims_data = list(claims())[0]  # Get the DataFrame from the generator
        
        if claims_data.empty:
            print("⚠️ No claims data found - creating sample data structure")
            claims_data = create_sample_claims_data()
        
        print(f"✅ Loaded {len(claims_data):,} rows of claims data")
        
        # Standardize column names to match PA DATA table
        column_mapping = {
            'nhislegacynumber': 'enrollee_id',      # Same as IID in PA
            'nhisproviderid': 'providerid',         # Same as providerid in PA
            'nhisgroupid': 'groupid',               # Same as groupid in PA
            'panumber': 'panumber',                 # Same as panumber in PA
            'encounterdatefrom': 'encounterdatefrom', # Keep as is
            'datesubmitted': 'datesubmitted',       # Keep as is
            'chargeamount': 'chargeamount',         # Keep as is
            'approvedamount': 'approvedamount',     # Keep as is
            'procedurecode': 'code',                # Same as code in PA
            'deniedamount': 'deniedamount'          # Keep as is
        }
        
        # Rename columns
        claims_data = claims_data.rename(columns=column_mapping)
        print("✅ Standardized column names to match PA DATA table")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        claims_data = create_sample_claims_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."CLAIMS DATA" (
            enrollee_id VARCHAR,
            providerid VARCHAR,
            groupid VARCHAR,
            panumber INTEGER,
            encounterdatefrom DATE,
            datesubmitted TIMESTAMP,
            chargeamount DOUBLE,
            approvedamount DOUBLE,
            code VARCHAR,
            deniedamount DOUBLE
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"CLAIMS DATA\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."CLAIMS DATA" 
        SELECT * FROM claims_data
    """)
    
    print("✅ Successfully loaded CLAIMS DATA into DuckDB")
    
    # Add table documentation
    add_claims_documentation(conn, "CLAIMS DATA", claims_data)
    
    return claims_data

def create_sample_claims_data():
    """Create sample claims data if database connection fails"""
    import numpy as np
    from datetime import datetime, timedelta
    
    sample_data = {
        'enrollee_id': [f'CL/IIS/378/01A', f'CL/IGCL/739660/2024-A', f'CL/ARIK/482/2017~B', f'CL/TECH/123/2024', f'CL/HEALTH/456/2023'],
        'providerid': ['977', '977', '977', '123', '456'],
        'groupid': ['1292', '1453', '1328', '2001', '2002'],
        'panumber': [0, 0, 0, 100001, 100002],
        'encounterdatefrom': [(datetime.now() - timedelta(days=i*30)).date() for i in range(5)],
        'datesubmitted': [(datetime.now() - timedelta(days=i*7)).date() for i in range(5)],
        'chargeamount': [2000.0, 900.0, 1350.0, 27.0, 2000.0],
        'approvedamount': [2000.0, 900.0, 1350.0, 486.0, 2000.0],
        'code': ['CONS021', 'DIAG435', 'DRG1081', 'DRG2641', 'CONS021'],
        'deniedamount': [0.0, 0.0, 0.0, 0.0, 0.0]
    }
    
    return pd.DataFrame(sample_data)

def add_claims_documentation(conn, table_name, claims_data):
    """Add documentation for the claims table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Claims data with submitted claims, amounts, and procedure information
    
    -- Column Documentation (Claims Data Structure - 10 columns):
    -- enrollee_id: Unique identifier for each enrollee (same as IID in PA DATA, can be used as key)
    -- providerid: Unique identifier for healthcare provider/hospital (same as providerid in PA DATA)
    -- groupid: Unique identifier for each client/group (same as groupid in PA DATA)
    -- panumber: Pre-authorization number from PA DATA (0 if no authorization, links to PA DATA table)
    -- encounterdatefrom: Date when the medical procedure was actually performed
    -- datesubmitted: Date when the claim was submitted for payment (can be months after encounter)
    -- chargeamount: Amount charged by provider for the procedure
    -- approvedamount: Amount approved for payment after vetting chargeamount
    -- code: Procedure code (same as code in PA DATA, links to procedure table)
    -- deniedamount: Amount denied/rejected (chargeamount - approvedamount)
    
    -- Key Relationships:
    -- - Links to PA DATA via panumber (not all PA requests result in claims)
    -- - Links to members table via enrollee_id
    -- - Links to providers table via providerid
    -- - Links to groups table via groupid
    
    -- Note: This table contains {len(claims_data):,} rows of claims data from 2022-2025
    -- Date Range: Based on datesubmitted (when claims were submitted for payment)
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_providers_data(conn):
    """Load all_providers data as PROVIDERS table"""
    print("📊 Loading PROVIDERS DATA from all_providers...")
    
    try:
        # Get data from dlt_sources
        providers_data = list(all_providers())[0]  # Get the DataFrame from the generator
        
        if providers_data.empty:
            print("⚠️ No providers data found - creating sample data structure")
            providers_data = create_sample_providers_data()
        
        print(f"✅ Loaded {len(providers_data):,} rows of providers data")
        
        # Column names are already standardized in dlt_sources.py
        # providertin is now providerid, categoryname is now bands
        print("✅ Column names already standardized in source")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        providers_data = create_sample_providers_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROVIDERS" (
            providerid VARCHAR,
            providername VARCHAR,
            dateadded TIMESTAMP,
            isvisible BOOLEAN,
            lganame VARCHAR,
            statename VARCHAR,
            bands VARCHAR
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"PROVIDERS\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."PROVIDERS" 
        SELECT 
            providerid,
            providername,
            dateadded,
            isvisible,
            lganame,
            statename,
            bands
        FROM providers_data
    """)
    
    print("✅ Successfully loaded PROVIDERS into DuckDB")
    
    # Add table documentation
    add_providers_documentation(conn, "PROVIDERS", providers_data)
    
    return providers_data

def create_sample_providers_data():
    """Create sample providers data if database connection fails"""
    from datetime import datetime, timedelta
    
    sample_data = {
        'providerid': ['850', '851', '852', '853', '854'],
        'providername': ['Bolasad Specialist Hospital', 'Bidems Victory Hosp & Diag Centre', 'Ayo Clinic', 'Channels Clinic & Hosp. Nig. Ltd', 'City International Clinics Taraba'],
        'dateadded': [datetime.now() - timedelta(days=i*30) for i in range(5)],
        'isvisible': [False, True, True, True, False],
        'lganame': ['Ikeja', 'Ikorodu', 'KOSOFE', 'Obia/Akpor', 'Jalingo'],
        'statename': ['Lagos', 'Lagos', 'Lagos', 'Rivers', 'Taraba'],
        'bands': ['Band D', 'Band D', 'Band D', 'Band D', 'Band D']
    }
    
    return pd.DataFrame(sample_data)

def add_providers_documentation(conn, table_name, providers_data):
    """Add documentation for the providers table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Healthcare providers/hospitals in the network
    
    -- Column Documentation (Providers Data Structure - 7 columns):
    -- providerid: Unique identifier for each provider (same as providertin in source)
    -- providername: Name of the healthcare provider/hospital
    -- dateadded: Date when the provider was onboarded into the network
    -- isvisible: Boolean indicating whether the hospital is active/visible (True/False)
    -- lganame: Local Government Area where the hospital is located
    -- statename: State where the hospital is located
    -- bands: Provider category/band classification (Band A, B, C, D, etc.)
    
    -- Key Relationships:
    -- - Links to PA DATA via providerid
    -- - Links to CLAIMS DATA via providerid
    -- - Geographic data via lganame and statename
    
    -- Note: This table contains {len(providers_data):,} rows of provider data
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_groups_data(conn):
    """Load all_group data as GROUPS table"""
    print("📊 Loading GROUPS DATA from all_group...")
    
    try:
        # Get data from dlt_sources
        groups_data = list(all_group())[0]  # Get the DataFrame from the generator
        
        if groups_data.empty:
            print("⚠️ No groups data found - creating sample data structure")
            groups_data = create_sample_groups_data()
        else:
            # Add placeholder lga/state columns if not present
            if 'lganame' not in groups_data.columns:
                groups_data['lganame'] = 'Unknown'
            if 'statename' not in groups_data.columns:
                groups_data['statename'] = 'Unknown'
        
        print(f"✅ Loaded {len(groups_data):,} rows of groups data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        groups_data = create_sample_groups_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."GROUPS" (
            groupid INTEGER,
            groupname VARCHAR,
            lganame VARCHAR,
            statename VARCHAR,
            dateadded TIMESTAMP
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"GROUPS\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."GROUPS" 
        SELECT 
            groupid,
            groupname,
            lganame,
            statename,
            dateadded
        FROM groups_data
    """)
    
    print("✅ Successfully loaded GROUPS into DuckDB")
    
    # Add table documentation
    add_groups_documentation(conn, "GROUPS", groups_data)
    
    return groups_data

def create_sample_groups_data():
    """Create sample groups data if database connection fails"""
    from datetime import datetime, timedelta
    
    sample_data = {
        'groupid': [800, 801, 802, 803, 804],
        'groupname': ['CRYSTAL FINANCE COMPANY LIMITED', 'AG HOMES', 'FUNSHO LOGISTICS LIMITED', 'Spice Digital Nigeria Limited', 'Proximity Communications Limited'],
        'lganame': ['Ikeja', 'Ikeja', 'Lekki', 'Victoria Island', 'Ikeja'],
        'statename': ['Lagos', 'Lagos', 'Lagos', 'Lagos', 'Lagos'],
        'dateadded': [datetime.now() - timedelta(days=i*30) for i in range(5)]
    }
    
    return pd.DataFrame(sample_data)

def add_groups_documentation(conn, table_name, groups_data):
    """Add documentation for the groups table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Client groups/companies in the network
    
    -- Column Documentation (Groups Data Structure - 5 columns):
    -- groupid: Unique identifier for each group/client (correct groupid from all_group)
    -- groupname: Name of the client/company group
    -- lganame: Local Government Area where the group is located
    -- statename: State where the group is located
    -- dateadded: Date when the client/group was added to the network
    
    -- Key Relationships:
    -- - Links to PA DATA via groupname (PA DATA has incorrect groupid)
    -- - Links to CLAIMS DATA via groupid
    -- - Geographic data via lganame and statename
    
    -- Note: This table contains {len(groups_data):,} rows of group data
    -- IMPORTANT: Use groupname as key when relating to PA DATA (not groupid)
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_members_data(conn):
    """Load all_active_member data as MEMBERS table"""
    print("📊 Loading MEMBERS DATA from all_active_member...")
    
    try:
        # Get data from dlt_sources
        members_data = list(all_active_member())[0]  # Get the DataFrame from the generator
        
        if members_data.empty:
            print("⚠️ No members data found - creating sample data structure")
            members_data = create_sample_members_data()
        else:
            # Standardize column names
            column_mapping = {
                'legacycode': 'enrollee_id'  # Change legacycode to enrollee_id
            }
            
            # Rename columns
            members_data = members_data.rename(columns=column_mapping)
            print("✅ Standardized column names")
        
        print(f"✅ Loaded {len(members_data):,} rows of members data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        members_data = create_sample_members_data()
    
    # Create table in AI DRIVEN DATA schema (matching actual data structure with demographics)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."MEMBERS" (
            memberid VARCHAR,
            groupid INTEGER,
            enrollee_id VARCHAR,
            planid VARCHAR,
            iscurrent BOOLEAN,
            isterminated BOOLEAN,
            dob DATE,
            genderid INTEGER,
            email1 VARCHAR,
            email2 VARCHAR,
            email3 VARCHAR,
            email4 VARCHAR,
            phone1 VARCHAR,
            phone2 VARCHAR,
            phone3 VARCHAR,
            phone4 VARCHAR,
            address1 VARCHAR,
            address2 VARCHAR,
            registrationdate TIMESTAMP,
            effectivedate TIMESTAMP,
            terminationdate TIMESTAMP
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"MEMBERS\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."MEMBERS" 
        SELECT 
            memberid,
            groupid,
            enrollee_id,
            planid,
            iscurrent,
            isterminated,
            dob,
            genderid,
            email1,
            email2,
            email3,
            email4,
            phone1,
            phone2,
            phone3,
            phone4,
            address1,
            address2,
            registrationdate,
            effectivedate,
            terminationdate
        FROM members_data
    """)
    
    print("✅ Successfully loaded MEMBERS into DuckDB")
    
    # Add table documentation
    add_members_documentation(conn, "MEMBERS", members_data)
    
    return members_data

def create_sample_members_data():
    """Create sample members data if database connection fails"""
    from datetime import datetime, timedelta
    import random
    
    sample_data = {
        'memberid': ['M001', 'M002', 'M003', 'M004', 'M005'],
        'groupid': [800, 801, 802, 803, 804],
        'enrollee_id': ['CL/MEM/001', 'CL/MEM/002', 'CL/MEM/003', 'CL/MEM/004', 'CL/MEM/005'],
        'planid': ['PLAN001', 'PLAN002', 'PLAN001', 'PLAN003', 'PLAN002'],
        'iscurrent': [True, True, True, True, True],
        'isterminated': [False, False, False, False, False],
        'dob': [(datetime.now() - timedelta(days=random.randint(18*365, 65*365))).date() for _ in range(5)],
        'genderid': [1, 2, 1, 2, 1],  # 1=Male, 2=Female
        'email1': ['john@example.com', 'jane@example.com', 'bob@example.com', 'alice@example.com', 'charlie@example.com'],
        'email2': [None, None, None, None, None],
        'email3': [None, None, None, None, None],
        'email4': [None, None, None, None, None],
        'phone1': ['08012345678', '08023456789', '08034567890', '08045678901', '08056789012'],
        'phone2': [None, None, None, None, None],
        'phone3': [None, None, None, None, None],
        'phone4': [None, None, None, None, None],
        'address1': ['123 Main St, Lagos', '456 Oak Ave, Abuja', '789 Pine Rd, Kano', '321 Elm St, Port Harcourt', '654 Maple Dr, Ibadan'],
        'address2': [None, None, None, None, None],
        'registrationdate': [(datetime.now() - timedelta(days=random.randint(30, 365))) for _ in range(5)],
        'effectivedate': [(datetime.now() - timedelta(days=random.randint(1, 30))) for _ in range(5)],
        'terminationdate': [(datetime.now() + timedelta(days=random.randint(30, 365))) for _ in range(5)]
    }
    
    return pd.DataFrame(sample_data)

def add_members_documentation(conn, table_name, members_data):
    """Add documentation for the members table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Active enrollees/members in the network with complete demographic information
    
    -- Column Documentation (Members Data Structure - 21 columns):
    -- memberid: Unique member identifier
    -- groupid: Group ID for the client that each enrollee is under (links to GROUPS table)
    -- enrollee_id: Unique enrollee identifier (same as legacycode, links to PA DATA and CLAIMS DATA)
    -- planid: Insurance plan ID for the member
    -- iscurrent: Boolean indicating if the member is currently active
    -- isterminated: Boolean indicating if the member is terminated
    -- dob: Date of birth of the enrollee
    -- genderid: Gender identifier (1=Male, 2=Female, 3=Other)
    -- email1, email2, email3, email4: Email addresses (primary and additional)
    -- phone1, phone2, phone3, phone4: Phone numbers (primary and additional)
    -- address1, address2: Address information (primary and additional)
    -- registrationdate: Date the enrollee was registered/entered the network
    -- effectivedate: Date when the member's coverage became effective
    -- terminationdate: Date when the member's coverage will terminate
    
    -- Key Relationships:
    -- - Links to GROUPS via groupid
    -- - Links to PA DATA via enrollee_id (same as IID in PA DATA)
    -- - Links to CLAIMS DATA via enrollee_id
    -- - Contains active members only (isterminated=0, iscurrent=1)
    
    -- Demographic Information:
    -- - Gender distribution: 1=Male, 2=Female, 3=Other
    -- - Age can be calculated from dob
    -- - Contact information available for communication
    -- - Address information for geographic analysis
    
    -- Note: This table contains {len(members_data):,} rows of active member data with demographics
    -- Filter: Only active members with legacycode starting with 'CL'
    -- Demographics: Complete gender, age, and contact information available
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_benefitcode_data(conn):
    """Load benefitcode data as BENEFITCODES table"""
    print("📊 Loading BENEFITCODES DATA from benefitcode...")
    
    try:
        # Get data from dlt_sources
        benefitcode_data = list(benefitcode())[0]  # Get the DataFrame from the generator
        
        if benefitcode_data.empty:
            print("⚠️ No benefitcode data found - creating sample data structure")
            benefitcode_data = create_sample_benefitcode_data()
        
        print(f"✅ Loaded {len(benefitcode_data):,} rows of benefitcode data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        benefitcode_data = create_sample_benefitcode_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."BENEFITCODES" (
            benefitcodeid INTEGER,
            benefitcodedesc VARCHAR
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"BENEFITCODES\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."BENEFITCODES" 
        SELECT 
            benefitcodeid,
            benefitcodedesc
        FROM benefitcode_data
    """)
    
    print("✅ Successfully loaded BENEFITCODES into DuckDB")
    
    # Add table documentation
    add_benefitcode_documentation(conn, "BENEFITCODES", benefitcode_data)
    
    return benefitcode_data

def create_sample_benefitcode_data():
    """Create sample benefitcode data if database connection fails"""
    sample_data = {
        'benefitcodeid': [1, 3, 4, 5, 7],
        'benefitcodedesc': ['ADMISSION AND FEEDING', 'ICU ADMISSION', 'PERSONAL MEDICAL DEVICES', 'PHOTOTHERAPY', 'PSYCHIATRIC TREATMENT']
    }
    
    return pd.DataFrame(sample_data)

def add_benefitcode_documentation(conn, table_name, benefitcode_data):
    """Add documentation for the benefitcode table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Benefit codes and their descriptions (inpatient/outpatient coverage types)
    
    -- Column Documentation (Benefit Codes Structure - 2 columns):
    -- benefitcodeid: Unique identifier for each benefit type
    -- benefitcodedesc: Name/description of the benefit (e.g., ADMISSION AND FEEDING, ICU ADMISSION)
    
    -- Key Relationships:
    -- - Links to BENEFITCODE_PROCEDURES via benefitcodeid
    -- - Used to identify benefit types for procedures in PA DATA and CLAIMS DATA
    -- - Each benefit has limits (count or money) defined in planbenefitcode_limit table
    
    -- Note: This table contains {len(benefitcode_data):,} rows of benefit code data
    -- Examples: ADM (Admission), ICU (ICU Admission), PERSONAL MED DEVICES, PHOTO (Phototherapy)
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_benefitcode_procedure_data(conn):
    """Load benefitcode_procedure data as BENEFITCODE_PROCEDURES table"""
    print("📊 Loading BENEFITCODE_PROCEDURES DATA from benefitcode_procedure...")
    
    try:
        # Get data from dlt_sources
        benefitcode_proc_data = list(benefitcode_procedure())[0]  # Get the DataFrame from the generator
        
        if benefitcode_proc_data.empty:
            print("⚠️ No benefitcode_procedure data found - creating sample data structure")
            benefitcode_proc_data = create_sample_benefitcode_procedure_data()
        
        print(f"✅ Loaded {len(benefitcode_proc_data):,} rows of benefitcode_procedure data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        benefitcode_proc_data = create_sample_benefitcode_procedure_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" (
            benefitcodeid INTEGER,
            procedurecode VARCHAR
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"BENEFITCODE_PROCEDURES\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" 
        SELECT 
            benefitcodeid,
            procedurecode
        FROM benefitcode_proc_data
    """)
    
    print("✅ Successfully loaded BENEFITCODE_PROCEDURES into DuckDB")
    
    # Add table documentation
    add_benefitcode_procedure_documentation(conn, "BENEFITCODE_PROCEDURES", benefitcode_proc_data)
    
    return benefitcode_proc_data

def create_sample_benefitcode_procedure_data():
    """Create sample benefitcode_procedure data if database connection fails"""
    sample_data = {
        'benefitcodeid': [1, 1, 3, 3, 4],
        'procedurecode': ['DIAG010', 'DIAG019', 'DIAG228', 'DIAG278', 'DIAG307']
    }
    
    return pd.DataFrame(sample_data)

def add_benefitcode_procedure_documentation(conn, table_name, benefitcode_proc_data):
    """Add documentation for the benefitcode_procedure table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Mapping of procedure codes to benefit codes (which procedures belong to which benefits)
    
    -- Column Documentation (Benefit Code Procedures Structure - 2 columns):
    -- benefitcodeid: Benefit code ID (links to BENEFITCODES table)
    -- procedurecode: Procedure code (same as code in PA DATA and CLAIMS DATA)
    
    -- Key Relationships:
    -- - Links to BENEFITCODES via benefitcodeid
    -- - Links to PA DATA via procedurecode (same as code column)
    -- - Links to CLAIMS DATA via procedurecode (same as code column)
    -- - Used to calculate enrollee spending under each benefit type
    
    -- Note: This table contains {len(benefitcode_proc_data):,} rows of procedure-to-benefit mappings
    -- Purpose: Determine which benefit category each procedure belongs to for spending analysis
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_group_plan_data(conn):
    """Load group_plan data as GROUP_PLANS table"""
    print("📊 Loading GROUP_PLANS DATA from group_plan...")
    
    try:
        # Get data from dlt_sources
        group_plan_data = list(group_plan())[0]  # Get the DataFrame from the generator
        
        if group_plan_data.empty:
            print("⚠️ No group_plan data found - creating sample data structure")
            group_plan_data = create_sample_group_plan_data()
        
        print(f"✅ Loaded {len(group_plan_data):,} rows of group_plan data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        group_plan_data = create_sample_group_plan_data()
    
    # Create table in AI DRIVEN DATA schema (only important columns)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."GROUP_PLANS" (
            groupid INTEGER,
            planlimit DOUBLE,
            countofindividual INTEGER,
            countoffamily INTEGER,
            individualprice DOUBLE,
            familyprice DOUBLE,
            maxnumdependant INTEGER
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"GROUP_PLANS\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."GROUP_PLANS" 
        SELECT 
            groupid,
            planlimit,
            countofindividual,
            countoffamily,
            individualprice,
            familyprice,
            maxnumdependant
        FROM group_plan_data
    """)
    
    print("✅ Successfully loaded GROUP_PLANS into DuckDB")
    
    # Add table documentation
    add_group_plan_documentation(conn, "GROUP_PLANS", group_plan_data)
    
    return group_plan_data

def create_sample_group_plan_data():
    """Create sample group_plan data if database connection fails"""
    from datetime import datetime, timedelta
    
    sample_data = {
        'groupid': [800, 801, 802, 803, 804],
        'planlimit': [1200000.0, 1800000.0, 1300000.0, 1200000.0, 1300000.0],
        'countofindividual': [4, 10, 1, 20, 2],
        'countoffamily': [0, 0, 0, 0, 0],
        'individualprice': [65000.0, 79200.0, 48000.0, 48000.0, 60000.0],
        'familyprice': [0.0, 0.0, 0.0, 0.0, 0.0],
        'maxnumdependant': [5, 5, 5, 5, 5]
    }
    
    return pd.DataFrame(sample_data)

def add_group_plan_documentation(conn, table_name, group_plan_data):
    """Add documentation for the group_plan table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Plans that each client/group has purchased (individual and family plans)
    
    -- Column Documentation (Group Plans Structure - 7 columns):
    -- groupid: Unique identifier for each client/group (links to GROUPS table)
    -- planlimit: Monetary limit each plan can use (enrollees can utilize up to this amount)
    -- countofindividual: Number of individual enrollee slots paid for in this plan
    -- countoffamily: Number of family slots paid for in this group
    -- individualprice: Price of individual plan paid by the group
    -- familyprice: Price of family plan paid by the group
    -- maxnumdependant: Maximum number of dependents allowed (family size = maxnumdependant + 1 principal)
    
    -- Key Relationships:
    -- - Links to GROUPS via groupid
    -- - Links to MEMBERS via groupid (determines which plan each member is on)
    -- - Used to calculate total amount paid by group (sum of individual + family prices)
    -- - Used to calculate per-person cost in family plans (familyprice / (maxnumdependant + 1))
    
    -- Business Logic:
    -- - A client can have multiple plans for different enrollees
    -- - Each plan has both individual and family types
    -- - Family size = maxnumdependant + 1 (principal + dependents)
    -- - Total group payment = sum(individualprice * countofindividual) + sum(familyprice * countoffamily)
    
    -- Note: This table contains {len(group_plan_data):,} rows of group plan data
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_proceduredata_data(conn):
    """Load proceduredata as PROCEDURE DATA table"""
    print("📊 Loading PROCEDURE DATA from proceduredata...")
    
    try:
        # Get data from dlt_sources
        procedure_data = list(proceduredata())[0]  # Get the DataFrame from the generator
        
        if procedure_data.empty:
            print("⚠️ No proceduredata found - creating sample data structure")
            procedure_data = create_sample_proceduredata_data()
        
        print(f"✅ Loaded {len(procedure_data):,} rows of procedure data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        procedure_data = create_sample_proceduredata_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."PROCEDURE DATA" (
            procedurecode VARCHAR,
            proceduredesc VARCHAR
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"PROCEDURE DATA\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."PROCEDURE DATA" 
        SELECT 
            procedurecode,
            proceduredesc
        FROM procedure_data
    """)
    
    print("✅ Successfully loaded PROCEDURE DATA into DuckDB")
    
    # Add table documentation
    add_proceduredata_documentation(conn, "PROCEDURE DATA", procedure_data)
    
    return procedure_data

def create_sample_proceduredata_data():
    """Create sample proceduredata if database connection fails"""
    sample_data = {
        'procedurecode': ['DRG1081', 'DRG1106', 'DRG1477', 'DRG2641', 'DRG4925', 'MED0499', 'PRE001', 'PRE002'],
        'proceduredesc': [
            'Cataract Surgery - Phacoemulsification',
            'Cataract Surgery - Extracapsular Extraction', 
            'Glaucoma Surgery - Trabeculectomy',
            'Diabetic Retinopathy Treatment',
            'Retinal Detachment Surgery',
            'Diabetes Management - Insulin Therapy',
            'Preventive Care - Annual Checkup',
            'Preventive Care - Vaccination'
        ]
    }
    
    return pd.DataFrame(sample_data)

def add_proceduredata_documentation(conn, table_name, procedure_data):
    """Add documentation for the proceduredata table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Master procedure code bank with descriptions for all procedures on the network
    
    -- Column Documentation (Procedure Data Structure - 2 columns):
    -- procedurecode: Unique procedure code (e.g., DRG1081, MED0499, PRE001)
    -- proceduredesc: Full description/name of the procedure (e.g., "Cataract Surgery - Phacoemulsification")
    
    -- Key Relationships:
    -- - Links to PA DATA via procedurecode (code column)
    -- - Links to CLAIMS DATA via procedurecode
    -- - Links to BENEFITCODE_PROCEDURES via procedurecode
    -- - Master reference for all procedure codes in the system
    
    -- Business Logic:
    -- - Contains ALL procedure codes available on the network
    -- - Used to get human-readable procedure names instead of just codes
    -- - Essential for analysis to understand what each procedure actually does
    -- - DRG codes: Diagnosis Related Groups (surgical procedures)
    -- - MED codes: Medical procedures (non-surgical)
    -- - PRE codes: Preventive care procedures
    
    -- Note: This table contains {len(procedure_data):,} rows of procedure code data
    -- This is the master lookup table for procedure descriptions
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def load_e_account_group_data(conn):
    """Load e_account_group as E_ACCOUNT_GROUP table"""
    print("📊 Loading E_ACCOUNT_GROUP from e_account_group...")
    
    try:
        # Get data from dlt_sources
        e_account_group_data = list(e_account_group())[0]  # Get the DataFrame from the generator
        
        if e_account_group_data.empty:
            print("⚠️ No e_account_group found - creating sample data structure")
            e_account_group_data = create_sample_e_account_group_data()
        
        print(f"✅ Loaded {len(e_account_group_data):,} rows of e_account_group data")
        
    except Exception as e:
        print(f"⚠️ Database connection failed: {e}")
        print("📝 Creating sample data to demonstrate structure...")
        e_account_group_data = create_sample_e_account_group_data()
    
    # Create table in AI DRIVEN DATA schema
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "AI DRIVEN DATA"."E_ACCOUNT_GROUP" (
            groupid INTEGER,
            ID_Company INTEGER,
            groupname VARCHAR,
            address1 VARCHAR,
            address2 VARCHAR,
            CompCode VARCHAR,
            AgentCode VARCHAR
        )
    """)
    
    # Insert data
    conn.execute("DELETE FROM \"AI DRIVEN DATA\".\"E_ACCOUNT_GROUP\"")  # Clear existing data
    
    # Convert DataFrame to DuckDB
    conn.execute("""
        INSERT INTO "AI DRIVEN DATA"."E_ACCOUNT_GROUP" 
        SELECT 
            groupid,
            ID_Company,
            groupname,
            address1,
            address2,
            CompCode,
            AgentCode
        FROM e_account_group_data
    """)
    
    print("✅ Successfully loaded E_ACCOUNT_GROUP into DuckDB")
    
    # Add table documentation
    add_e_account_group_documentation(conn, "E_ACCOUNT_GROUP", e_account_group_data)
    
    return e_account_group_data

def create_sample_e_account_group_data():
    """Create sample e_account_group if database connection fails"""
    sample_data = {
        'groupid': [800, 801, 802, 803, 804],
        'ID_Company': [371, 372, 373, 374, 375],
        'groupname': [
            'CRYSTAL FINANCE COMPANY LIMITED',
            'AG HOMES',
            'FUNSHO LOGISTICS LIMITED',
            'SAMPLE COMPANY A',
            'SAMPLE COMPANY B'
        ],
        'address1': [
            '11, Mobolaji Johnson Street',
            '96, Opebi Road',
            'Fola Osibo Street',
            '123 Sample Street',
            '456 Test Avenue'
        ],
        'address2': [
            'off Adeniran Ogunsanya Surulere',
            'Ikeja',
            'Lekki Expressway, Lekki',
            'Sample City',
            'Test City'
        ],
        'CompCode': ['CRYS', 'AGHO', 'FUNS', 'SAMP', 'TEST'],
        'AgentCode': ['A', 'B', 'C', 'D', 'E']
    }
    
    return pd.DataFrame(sample_data)

def add_e_account_group_documentation(conn, table_name, e_account_group_data):
    """Add documentation for the e_account_group table columns"""
    documentation = f"""
    -- Table: {table_name}
    -- Description: Company/Group mapping between network (groupid) and finance software (ID_Company)
    
    -- Column Documentation (E_ACCOUNT_GROUP Structure - 7 columns):
    -- groupid: Network group identifier (INTEGER) - Links to GROUPS, PA DATA, CLAIMS DATA
    -- ID_Company: Finance software company identifier (INTEGER) - Used for financial analysis
    -- groupname: Company/Group name (VARCHAR) - Human-readable company name
    -- address1: Primary address (VARCHAR) - Company location
    -- address2: Secondary address (VARCHAR) - Additional location details
    -- CompCode: Company code (VARCHAR) - Short company identifier (e.g., CRYS, AGHO)
    -- AgentCode: Agent code (VARCHAR) - Agent association
    
    -- Key Relationships:
    -- - groupid links to GROUPS.groupid, PA DATA.groupname, CLAIMS DATA.groupname
    -- - ID_Company used for financial software integration
    -- - groupname provides human-readable company identification
    -- - CompCode for quick company reference
    
    -- Business Logic:
    -- - Bridges network system (groupid) with finance system (ID_Company)
    -- - Enables financial analysis by linking network usage to company payments
    -- - Use ID_Company to find groupname for financial queries
    -- - Use groupid for network/healthcare analysis
    -- - Critical for understanding which companies are paying for what services
    
    -- Note: This table contains {len(e_account_group_data):,} rows of company mapping data
    -- This is the bridge between healthcare network and financial systems
    """
    
    # Store documentation in the documentation table
    conn.execute(f"""
        INSERT INTO "AI DRIVEN DATA"."table_documentation" 
        VALUES ('{table_name}', '{documentation.replace("'", "''")}')
    """)

def main():
    """Main function to set up the AI database"""
    print("🎯 Setting up AI DRIVEN DATA database...")
    print("=" * 60)
    
    # Step 1: Clean existing databases
    clean_existing_databases()
    
    # Step 2: Create new database
    conn = create_ai_database()
    
    # Step 3: Load PA DATA
    pa_data = load_pa_data(conn)
    
    # Step 4: Load CLAIMS DATA
    claims_data = load_claims_data(conn)
    
    # Step 5: Load PROVIDERS DATA
    providers_data = load_providers_data(conn)
    
    # Step 6: Load GROUPS DATA
    groups_data = load_groups_data(conn)
    
    # Step 7: Load MEMBERS DATA
    members_data = load_members_data(conn)
    
    # Step 8: Load BENEFITCODES DATA
    benefitcode_data = load_benefitcode_data(conn)
    
    # Step 9: Load BENEFITCODE_PROCEDURES DATA
    benefitcode_proc_data = load_benefitcode_procedure_data(conn)
    
    # Step 10: Load GROUP_PLANS DATA
    group_plan_data = load_group_plan_data(conn)

    # Step 11: Load PA ISSUE REQUEST DATA
    pa_issue_request_data = load_pa_issue_request(conn)
    
    # Step 12: Load PROCEDURE DATA
    procedure_data = load_proceduredata_data(conn)
    
    # Step 13: Load E_ACCOUNT_GROUP DATA
    e_account_group_data = load_e_account_group_data(conn)
    
    # Step 14: Load finance sources and build derived tables
    load_fin_gl(conn)
    load_premium1_schedule(conn)
    load_debit_note(conn)
    build_salary_and_expenses(conn)
    build_client_cash_received(conn)
    build_debit_note_accrued(conn)
    
    if pa_data is not None and claims_data is not None and providers_data is not None and groups_data is not None and members_data is not None and benefitcode_data is not None and benefitcode_proc_data is not None and group_plan_data is not None and pa_issue_request_data is not None and procedure_data is not None and e_account_group_data is not None:
        # Step 10: Verify database
        conn.close()
        verify_database()
        
        print("\n🎉 AI DRIVEN DATA database setup complete!")
        print("📁 Database file: ai_driven_data.duckdb")
        print("📊 Schema: AI DRIVEN DATA")
        print("📋 Tables: PA DATA, CLAIMS DATA, PROVIDERS, GROUPS, MEMBERS, BENEFITCODES, BENEFITCODE_PROCEDURES, GROUP_PLANS, PA ISSUE REQUEST, PROCEDURE DATA, E_ACCOUNT_GROUP, FIN_GL, PREMIUM1_SCHEDULE, DEBIT_NOTE, SALARY_AND_EXPENSES, CLIENT_CASH_RECEIVED, DEBIT_NOTE_ACCRUED")
        print(f"📈 PA DATA: {len(pa_data):,} rows")
        print(f"📈 CLAIMS DATA: {len(claims_data):,} rows")
        print(f"📈 PROVIDERS: {len(providers_data):,} rows")
        print(f"📈 GROUPS: {len(groups_data):,} rows")
        print(f"📈 MEMBERS: {len(members_data):,} rows")
        print(f"📈 BENEFITCODES: {len(benefitcode_data):,} rows")
        print(f"📈 BENEFITCODE_PROCEDURES: {len(benefitcode_proc_data):,} rows")
        print(f"📈 GROUP_PLANS: {len(group_plan_data):,} rows")
        print(f"📈 PA ISSUE REQUEST: {len(pa_issue_request_data):,} rows")
        print(f"📈 PROCEDURE DATA: {len(procedure_data):,} rows")
        print(f"📈 E_ACCOUNT_GROUP: {len(e_account_group_data):,} rows")
        print("\n✅ Ready for AI analysis with all tables!")
        print("🔗 Tables are linked via: panumber, enrollee_id, providerid, groupname, groupid, code, benefitcodeid, ID_Company")
        print("⚠️  IMPORTANT: PA DATA uses groupname as key (not groupid)")
        print("💰 E_ACCOUNT_GROUP: Bridges network (groupid) and finance (ID_Company) systems")
        print("👥 MEMBERS: Active enrollees only (isterminated=0, iscurrent=1)")
        print("💊 BENEFITS: Track enrollee spending by benefit category (inpatient/outpatient)")
        print("📋 GROUP_PLANS: Individual and family plans with pricing and limits per group")
    else:
        print("❌ Failed to set up database")

if __name__ == "__main__":
    main()
