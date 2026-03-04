import dlt
import pandas as pd
import requests
import pyodbc
from datetime import datetime, timedelta

# Replace this with a local or secure config loader in production
import toml
import os

# Load secrets from toml file
def load_secrets():
    secrets_path = "secrets.toml"
    if os.path.exists(secrets_path):
        return toml.load(secrets_path)
    else:
        raise FileNotFoundError(f"Secrets file not found: {secrets_path}")

secrets = load_secrets()

# ---------------------------- DATABASE FETCHERS ----------------------------

def get_sql_driver():
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

def create_eacount_connection():
    """Create connection for EACOUNT database using secrets"""
    driver = get_sql_driver()
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={secrets['eaccount_credentials']['server']},{secrets['eaccount_credentials']['port']};"
        f"DATABASE={secrets['eaccount_credentials']['database']};"
        f"UID={secrets['eaccount_credentials']['username']};"
        f"PWD={secrets['eaccount_credentials']['password']};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    try:
        print("Connection string used:EACCOUNT ", conn_str)
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Connection string used: {conn_str.replace(secrets['eaccount_credentials']['password'], '****')}")
        raise e

# EACOUNT DLT resources
@dlt.resource(write_disposition="replace")
def debit_note():
    try:
        conn = create_eacount_connection()
        query = """
            SELECT *
            FROM dbo.DEBIT_Note
            WHERE [From] >= '2024-01-01' AND [From] <= GETDATE();
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded debit_note from EACOUNT")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load debit_note from EACOUNT: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['From', 'To', 'Description', 'Amount', 'CompanyName'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def fin_gl():
    try:
        conn = create_eacount_connection()
        query = """
            SELECT *
            FROM dbo.FIN_GL;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded fin_gl from EACOUNT")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load fin_gl from EACOUNT: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['GLCode', 'Description', 'Amount', 'Date'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def premium1_schedule():
    try:
        conn = create_eacount_connection()
        query = """
            SELECT *
            FROM dbo.Premium1_schedule;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded premium1_schedule from EACOUNT")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load premium1_schedule from EACOUNT: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['ScheduleID', 'Premium', 'Date'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def fin_accsetup():
    try:
        conn = create_eacount_connection()
        query = """
            SELECT *
            FROM dbo.FIN_AccSetup;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded fin_accsetup from EACOUNT")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load fin_accsetup from EACOUNT: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['AccountID', 'AccountName', 'Type'])
        yield empty_df

# ---------------------------- DATABASE FETCHER ----------------------------

@dlt.resource(write_disposition="replace")
def group_contract():
    conn = create_medicloud_connection()
    query = """
        SELECT 
        gc.groupid,
        gc.startdate,
        gc.enddate,
        gc.iscurrent,
        g.groupname
        FROM dbo.group_contract gc
        JOIN dbo.[group] g ON gc.groupid = g.groupid
        WHERE gc.iscurrent = 1
        AND CAST(gc.enddate AS DATETIME) >= CAST(GETDATE() AS DATETIME);
    """
    df = pd.read_sql(query, conn)
    conn.close()
    yield df

@dlt.resource(write_disposition="replace")
def tariff():
    conn = create_medicloud_connection()
    query = """
        SELECT 
        trf.tariffid,
        trf.tariffname,
        trfp.tariffamount,
        trfp.procedurecode,
        trfp.comments,
        trf.expirydate
        FROM dbo.tariff trf
        JOIN dbo.procedure_tariff trfp ON trf.tariffid = trfp.tariffid
        WHERE CAST (trf.expirydate AS DATETIME) >= CAST(GETDATE() AS DATETIME);
        """
    df = pd.read_sql(query, conn)
    conn.close()
    yield df

@dlt.resource(write_disposition="replace")
def benefit_procedure():
    conn = create_medicloud_connection()
    query = """
        SELECT
        bcf.benefitcodeid,
        bcf.procedurecode,
        bc.benefitcodename,
        bc.benefitcodedesc
        FROM dbo.benefitcode_procedure bcf
        JOIN dbo.benefitcode bc ON bcf.benefitcodeid = bc.benefitcodeid
    """
    df = pd.read_sql(query, conn)
    conn.close()
    yield df

@dlt.resource(write_disposition="replace")
def total_pa_procedures(since_date=None):
    """
    Fetch PA procedures from MediCloud.
    
    Args:
        since_date: If provided, only fetch records with requestdate >= since_date.
                    If None, fetches 2 years (current + previous year).
    """
    conn = create_medicloud_connection()
    
    if since_date:
        # Incremental update: fetch only new records
        if isinstance(since_date, str):
            start_date = since_date
        else:
            start_date = since_date.strftime('%Y-%m-%d') if hasattr(since_date, 'strftime') else str(since_date)
        mode = "incremental"
    else:
        # Full update: fetch 2 years
        current_year = datetime.now().year
        start_date = f'{current_year - 1}-01-01'
        mode = "full (2-year window)"
    
    query = f"""
        SELECT
        txn.panumber,
        txn.groupname,
        txn.divisionname,
        txn.plancode,
        txn.IID,
        txn.providerid,
        txn.requestdate,
        txn.pastatus,
        tbp.code,
        txn.userid,
        txn.totaltariff,
        txn.benefitcode,
        txn.dependantnumber,
        tbp.requested,
        tbp.granted
        FROM dbo.tbPATxn txn
        JOIN dbo.tbPAProcedures tbp ON txn.panumber = tbp.panumber
        WHERE txn.requestdate >= '{start_date}' AND txn.requestdate <= GETDATE();
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"✅ Successfully loaded total_pa_procedures from MediCloud: {len(df):,} rows ({mode}: {start_date} to present)")
    yield df

@dlt.resource(write_disposition="replace")
def claims(since_date=None):
    """
    Fetch claims from MediCloud.
    
    Args:
        since_date: If provided, only fetch records with datesubmitted >= since_date.
                    If None, fetches 2 years (current + previous year).
    """
    try:
        conn = create_medicloud_connection()
        
        if since_date:
            # Incremental update: fetch only new records
            if isinstance(since_date, str):
                start_date = since_date
            else:
                start_date = since_date.strftime('%Y-%m-%d') if hasattr(since_date, 'strftime') else str(since_date)
            mode = "incremental"
        else:
            
            # Full update: fetch 2 years
            current_year = datetime.now().year
            start_date = f'{current_year - 1}-01-01'
            mode = "full (2-year window)"
        
        query = f"""
            SELECT nhislegacynumber, nhisproviderid, nhisgroupid, panumber, encounterdatefrom, encounterdateto, datesubmitted, chargeamount, approvedamount, procedurecode, deniedamount, diagnosiscode, claimnumber, memberid, groupid, providerid, dependantnumber, isinpatient, discount, datereceived, claimstatusid, adjusterid, unitfactor, isapproved, isfinal, ispaid, amountpaid, datepaid, paymentbatchno, dateadded, claimid, nhisdependantnumber, Department, isappeal, comment, wht
            FROM dbo.claims
            WHERE datesubmitted >= '{start_date}' AND datesubmitted <= GETDATE();
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"✅ Successfully loaded claims from MediCloud: {len(df):,} rows ({mode}: {start_date} to present)")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load claims from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns including diagnosiscode
        empty_df = pd.DataFrame(columns=['nhislegacynumber', 'nhisproviderid', 'nhisgroupid', 'panumber', 'encounterdatefrom', 'encounterdateto', 'datesubmitted', 'chargeamount', 'approvedamount', 'procedurecode', 'deniedamount', 'diagnosiscode', 'claimnumber', 'memberid', 'groupid', 'providerid', 'dependantnumber', 'isinpatient', 'discount', 'datereceived', 'claimstatusid', 'adjusterid', 'unitfactor', 'isapproved', 'isfinal', 'ispaid', 'amountpaid', 'datepaid', 'paymentbatchno', 'dateadded', 'claimid', 'nhisdependantnumber', 'Department', 'isappeal', 'comment', 'wht'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def diagnosis():
    """Load diagnosis table from MediCloud - contains diagnosis codes and descriptions"""
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.diagnosis
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded diagnosis from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load diagnosis from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['diagnosiscode', 'diagnosisdesc'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def tbpadiagnosis():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.tbpadiagnosis
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded tbpadiagnosis from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load tbpadiagnosis from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame (no strict columns enforced due to potential schema variance)
        empty_df = pd.DataFrame()
        yield empty_df

@dlt.resource(write_disposition="replace")
def all_providers():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT
            p.providertin as providerid,
            p.providername,
            p.dateadded,
            p.isvisible,
            p.providerid as protariffid,
            l.lganame,
            s.statename,
            pc.categoryname as bands
        FROM
            dbo.provider p
            JOIN dbo.providercategory pc ON p.provcatid = pc.provcatid
        LEFT JOIN
            dbo.lgas l ON p.lgaid = l.lgaid
        LEFT JOIN
            dbo.states s ON p.stateid = s.stateid
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded all_providers from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load all_providers from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['providerid', 'providername', 'dateadded', 'isvisible', 'protariffid', 'lganame', 'statename', 'bands'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def group_coverage():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.group_coverage
            WHERE iscurrent = 1
            AND CAST(terminationdate AS DATE) >= CAST(GETDATE() AS DATE)
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded group_coverage from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load group_coverage from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['groupid', 'coverageid', 'iscurrent', 'terminationdate'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def all_active_member():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT
            mc.memberid,
            m.groupid,
            m.legacycode,
            m.planid,
            mc.iscurrent,
            m.isterminated,
            CAST(m.dob AS DATE) as dob,
            m.genderid,
            m.email1,
            m.email2,
            m.email3,
            m.email4,
            m.phone1,
            m.phone2,
            m.phone3,
            m.phone4,
            m.address1,
            m.address2,
            m.registrationdate,
            mc.effectivedate,
            mc.terminationdate
            FROM dbo.member_coverage mc
            JOIN dbo.member m ON mc.memberid = m.memberid
            WHERE m.isterminated = 0
            AND mc.iscurrent = 1
            AND CAST(mc.terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
            AND m.legacycode LIKE 'CL%';
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded all_active_member from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load all_active_member from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['memberid', 'groupid', 'legacycode', 'planid', 'iscurrent', 'isterminated', 'effectivedate', 'terminationdate'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def member():
    """
    Load full member table from MediCloud.
    
    This is the raw MEMBERS source (all members) and complements all_active_member,
    which only contains the filtered active members currently in coverage.
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.member
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded member from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load member from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected key columns
        empty_df = pd.DataFrame(columns=[
            'memberid', 'groupid', 'legacycode', 'planid', 'isterminated',
            'dob', 'genderid', 'email1', 'phone1', 'address1',
            'registrationdate'
        ])
        yield empty_df

@dlt.resource(write_disposition="replace")
def member_coverage():
    """
    Load full member_coverage table from MediCloud.
    
    This contains coverage periods for members and complements all_active_member,
    which only exposes current, non-terminated coverage.
    """
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT *
            FROM dbo.member_coverage
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded member_coverage from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load member_coverage from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected key columns
        empty_df = pd.DataFrame(columns=[
            'memberid', 'effectivedate', 'terminationdate', 'iscurrent'
        ])
        yield empty_df

@dlt.resource(write_disposition="replace")
def all_group():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                groupid,
                groupname,
                address1,
                address2,
                contactlastname,
                contactfirstname,
                contactemail1,
                contactemail2,
                contactphone1,
                contactphone2,
                contactphone3,
                renewaldate,
                zipcode,
                postalcodeid,
                stateid,
                lgaid,
                countryid,
                billingcycleid,
                billingdueday,
                billinginvoiceday,
                categoryid,
                statuscodeid,
                groupemail1,
                groupemail2,
                officephone1,
                officephone2,
                isgroupapproved,
                contactlastname2,
                contactfirstname2,
                comments,
                logopath,
                groupeffectivedate,
                groupterminationdate,
                approvalpending,
                sagecode,
                dateadded,
                useplanfilter,
                exclusionmargin,
                iscapitated
            FROM dbo.[group]
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded all_group from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load all_group from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['groupid', 'groupname', 'dateadded'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def member_plans():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.member_plan
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded member_plans from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load member_plans from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['memberid', 'planid', 'iscurrent'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def planbenefitcode_limit():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.planbenefitcode_limit
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded planbenefitcode_limit from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load planbenefitcode_limit from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['planid', 'benefitcodeid', 'limit'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def benefitcode():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.benefitcode
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded benefitcode from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load benefitcode from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['benefitcodeid', 'benefitcodename', 'benefitcodedesc'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def benefitcode_procedure():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.benefitcode_procedure
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded benefitcode_procedure from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load benefitcode_procedure from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['benefitcodeid', 'procedurecode'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def group_plan():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.group_plan
               WHERE iscurrent = 1
            AND CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded group_plan from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load group_plan from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['groupid', 'planid', 'iscurrent', 'terminationdate'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def pa_issue_request():
    try:
        conn = create_medicloud_connection()
        # Only fetch 2 years: current year and previous year
        current_year = datetime.now().year
        query = f"""
            SELECT Providerid, RequestDate, ResolutionTime, EncounterDate, PANumber, DateAdded
            FROM dbo.PAIssueRequest
            WHERE YEAR(EncounterDate) >= {current_year - 1} AND YEAR(EncounterDate) <= {current_year}
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"✅ Successfully loaded pa_issue_request from MediCloud: {len(df):,} rows (2-year window: {current_year - 1} and {current_year})")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load pa_issue_request from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['Providerid', 'RequestDate', 'ResolutionTime', 'EncounterDate', 'PANumber', 'DateAdded'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def plans():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT * FROM dbo.plans
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded plans from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load plans from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['planid', 'planname', 'description'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def group_invoice():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                CAST(groupid AS VARCHAR) as groupid, 
                invoicenumber, 
                countofindividual, 
                countoffamily, 
                individualprice, 
                familyprice,
                isapproved, 
                invoicestartdate, 
                invoiceenddate, 
                invoicetype 
            FROM dbo.group_invoice
            WHERE YEAR(invoicestartdate) >= YEAR(GETDATE()) - 1
            AND invoicetype = 'STANDARD'
        """
        df = pd.read_sql(query, conn)
        conn.close()
        row_count = len(df)
        print(f"✅ Successfully loaded group_invoice from MediCloud: {row_count:,} rows")
        if row_count == 0:
            print("   ⚠️ Warning: Query returned 0 rows. This may be expected if no invoices match the criteria.")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load group_invoice from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['groupid', 'invoicenumber', 'countofindividual', 'countoffamily', 'individualprice', 'familyprice', 'isapproved', 'invoicestartdate', 'invoiceenddate', 'invoicetype'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def e_account_group():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                groupid,
                ID_Company,
                CompanyName as groupname,
                address1,
                address2,
                CompCode,
                AgentCode
            FROM dbo.Company
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded e_account_group from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load e_account_group from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['groupid', 'ID_Company', 'groupname', 'address1', 'address2', 'CompCode', 'AgentCode'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def proceduredata():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                procedurecode,
                proceduredesc
            FROM dbo.proceduredata
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded proceduredata from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load proceduredata from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['procedurecode', 'proceduredesc'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def providers_tariff():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                tariffprovid,
                tariffid,
                providerid as protariffid,
                tariffdescr,
                dateadded
            FROM dbo.providers_tariff
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded providers_tariff from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load providers_tariff from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['tariffprovid', 'tariffid', 'protariffid', 'tariffdescr', 'dateadded'])
        yield empty_df

@dlt.resource(write_disposition="replace")
def member_provider():
    try:
        conn = create_medicloud_connection()
        query = """
            SELECT 
                memberprovid,
                memberid,
                providerid,
                coveragetypeid,
                effectivedate,
                terminationdate,
                iscurrent,
                dateadded,
                legacycode,
                depnum,
                providerkey,
                adjusterid,
                Comment
            FROM dbo.member_provider
            WHERE CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
        """
        df = pd.read_sql(query, conn)
        conn.close()
        print("✅ Successfully loaded member_provider from MediCloud")
        yield df
    except Exception as e:
        print(f"⚠️ Warning: Failed to load member_provider from MediCloud: {str(e)}")
        print("   Continuing with empty DataFrame to avoid pipeline failure")
        # Return empty DataFrame with expected columns
        empty_df = pd.DataFrame(columns=['memberprovid', 'memberid', 'providerid', 'coveragetypeid', 'effectivedate', 'terminationdate', 'iscurrent', 'dateadded', 'legacycode', 'depnum', 'providerkey', 'adjusterid', 'Comment'])
        yield empty_df

# ---------------------------- COMBINED SOURCE ----------------------------

@dlt.source
def dashboard_sources():
    yield group_contract()
    yield benefit_procedure()
    yield total_pa_procedures()
    yield claims()
    yield diagnosis()
    yield tbpadiagnosis()
    yield all_providers()
    yield group_coverage()
    yield all_active_member()
    yield member()
    yield member_coverage()
    yield all_group()
    yield member_plans()
    yield planbenefitcode_limit()
    yield benefitcode()
    yield benefitcode_procedure()
    yield group_plan()
    yield pa_issue_request()
    yield proceduredata()
    yield e_account_group()
    yield plans()
    yield group_invoice()
    yield debit_note()
    yield fin_gl()
    yield premium1_schedule()
    yield fin_accsetup()
    yield tariff()
    yield providers_tariff()
    yield member_provider()








