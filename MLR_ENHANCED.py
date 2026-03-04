import os
import pandas as pd
import streamlit as st
import polars as pl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, date, timedelta
import duckdb
import toml
import json
from pathlib import Path
import numpy as np

# ============================================================================
# ENHANCED ALERT SYSTEM WITH COMPREHENSIVE MONITORING
# ============================================================================

st.set_page_config(
    page_title="Enhanced MLR & Risk Monitoring Dashboard",
    page_icon="🚨",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🚨 Enhanced MLR & Risk Monitoring Dashboard")

# Email configuration
SENDER_EMAIL = "leocasey0@gmail.com"
MANAGEMENT_EMAILS = [
    "k.chukwuka@clearlinehmo.com",
    "k.odegbami@clearlinehmo.com",
    "p.osegbue@clearlinehmo.com"
]

# Alert thresholds
ALERT_THRESHOLDS = {
    'mlr_critical': 70.0,  # MLR percentage
    'mlr_warning': 65.0,
    'pmpm_negative_months': 2,  # Consecutive months
    'pmpm_spike': 30.0,  # Percentage increase
    'pmpm_drop': 20.0,  # Percentage decrease
    'member_concentration': 10.0,  # Single member % of total cost
    'provider_concentration': 40.0,  # Single provider % of total cost
    'mlr_acceleration': 10.0,  # Month-over-month MLR increase
    'claim_frequency': 4.0,  # Claims per member
    'low_penetration': 30.0,  # % of members with claims
}

# ============================================================================
# NOTIFICATION MANAGEMENT
# ============================================================================

def load_sent_notifications():
    """Load previously sent notifications"""
    notification_file = 'sent_notifications.json'
    try:
        if os.path.exists(notification_file):
            with open(notification_file, 'r') as f:
                return json.load(f)
        return {}
    except Exception as e:
        st.error(f"Error loading notifications: {e}")
        return {}

def save_notification(groupname, notification_type):
    """Save notification to prevent duplicates"""
    notification_file = 'sent_notifications.json'
    try:
        sent_notifications = load_sent_notifications()
        key = f"{groupname}_{notification_type}"
        sent_notifications[key] = {
            'sent_date': datetime.now().isoformat(),
            'groupname': groupname,
            'notification_type': notification_type
        }
        with open(notification_file, 'w') as f:
            json.dump(sent_notifications, f, indent=2)
    except Exception as e:
        st.error(f"Error saving notification: {e}")

def should_send_notification(groupname, notification_type, cooldown_days=7):
    """Check if notification should be sent based on cooldown period

    Args:
        groupname: The group name
        notification_type: Type of notification (e.g., 'mlr_critical')
        cooldown_days: Number of days to wait before resending same alert (default 7)

    Returns:
        bool: True if notification should be sent, False otherwise
    """
    sent_notifications = load_sent_notifications()
    key = f"{groupname}_{notification_type}"

    if key in sent_notifications:
        try:
            sent_date = datetime.fromisoformat(sent_notifications[key].get('sent_date', '2000-01-01'))
            days_since_sent = (datetime.now() - sent_date).days

            # Don't resend if within cooldown period
            if days_since_sent < cooldown_days:
                return False
        except Exception:
            pass
    return True

def get_notification_cooldown(notification_type):
    """Get cooldown period for different notification types"""
    cooldowns = {
        'mlr_critical': 7,           # Weekly for critical MLR
        'mlr_critical_mgmt': 7,      # Weekly for management
        'negative_pmpm': 7,          # Weekly for negative PMPM
        'negative_pmpm_current': 7,  # Weekly for current negative PMPM
        'negative_pmpm_avg': 7,      # Weekly for structural negative PMPM
        'member_concentration': 14,  # Bi-weekly for concentration
        'provider_concentration': 14 # Bi-weekly for concentration
    }
    return cooldowns.get(notification_type, 7)

# ============================================================================
# EMAIL FUNCTIONS
# ============================================================================

def send_email_alert(to_emails, subject, body, sender_password):
    """Send email alert"""
    try:
        msg = MIMEMultipart()
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(to_emails) if isinstance(to_emails, list) else to_emails
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, sender_password)
        server.sendmail(SENDER_EMAIL, to_emails if isinstance(to_emails, list) else [to_emails], msg.as_string())
        server.quit()
        return True
    except Exception as e:
        st.error(f"Failed to send email: {str(e)}")
        return False

# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_resource
def get_database_connections():
    """Load database configuration"""
    try:
        # Try environment variables first
        if all(os.getenv(key) for key in ['MEDICLOUD_SERVER', 'MEDICLOUD_DATABASE', 'MEDICLOUD_USERNAME', 'MEDICLOUD_PASSWORD', 'MEDICLOUD_PORT']):
            return {
                'credentials': {
                    'server': os.getenv('MEDICLOUD_SERVER'),
                    'database': os.getenv('MEDICLOUD_DATABASE'),
                    'username': os.getenv('MEDICLOUD_USERNAME'),
                    'password': os.getenv('MEDICLOUD_PASSWORD'),
                    'port': os.getenv('MEDICLOUD_PORT')
                }
            }

        # Fallback to secrets.toml
        secrets_path = os.path.join(os.path.dirname(__file__), "secrets.toml")
        if os.path.exists(secrets_path):
            return toml.load(secrets_path)

        return None
    except Exception as e:
        st.error(f"Error loading configuration: {str(e)}")
        return None

@st.cache_data(ttl=10800)  # 3 hours cache
def load_data_from_duckdb():
    """Load data from DuckDB with enhanced error handling"""
    try:
        progress_bar = st.progress(0)
        status_text = st.empty()

        status_text.text("📊 Connecting to DuckDB...")
        progress_bar.progress(5)

        db_path = os.path.join(os.path.dirname(__file__), 'ai_driven_data.duckdb')
        conn = duckdb.connect(db_path, read_only=True)

        # Load GROUP_CONTRACT
        status_text.text("📋 Loading group contracts...")
        progress_bar.progress(10)
        GROUP_CONTRACT = conn.execute(
            'SELECT groupid, startdate, enddate, iscurrent, groupname FROM "AI DRIVEN DATA"."GROUP_CONTRACT"'
        ).fetchdf()

        # Load CLAIMS
        status_text.text("🏥 Loading claims data...")
        progress_bar.progress(20)
        CLAIMS = conn.execute(
            """
            SELECT enrollee_id as nhislegacynumber,
                   nhisproviderid,
                   nhisgroupid,
                   panumber,
                   encounterdatefrom,
                   datesubmitted,
                   chargeamount,
                   approvedamount,
                   code as procedurecode,
                   deniedamount,
                   diagnosiscode
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE datesubmitted >= DATE '2024-01-01'
              AND nhisgroupid IS NOT NULL
            """
        ).fetchdf()

        # Load GROUPS
        status_text.text("👥 Loading groups...")
        progress_bar.progress(30)
        GROUPS = conn.execute('SELECT * FROM "AI DRIVEN DATA"."GROUPS"').fetchdf()

        # Load PA DATA
        status_text.text("📋 Loading PA data...")
        progress_bar.progress(40)
        PA = conn.execute(
            """
            SELECT panumber, groupname, divisionname, plancode, IID, providerid,
                   requestdate, pastatus, code, userid, totaltariff, benefitcode,
                   dependantnumber, requested, granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE requestdate >= TIMESTAMP '2024-01-01'
            """
        ).fetchdf()

        # Load MEMBERS
        status_text.text("👤 Loading members...")
        progress_bar.progress(50)
        MEMBERS = conn.execute(
            'SELECT memberid, groupid, enrollee_id as legacycode, planid, iscurrent, isterminated, effectivedate, terminationdate FROM "AI DRIVEN DATA"."MEMBERS"'
        ).fetchdf()

        # Load PROVIDERS
        status_text.text("🏥 Loading providers...")
        progress_bar.progress(60)
        PROVIDER = conn.execute('SELECT * FROM "AI DRIVEN DATA"."PROVIDERS"').fetchdf()

        # Load DEBIT NOTE
        status_text.text("💰 Loading debit notes...")
        progress_bar.progress(70)
        DEBIT = conn.execute(
            """
            SELECT *
            FROM "AI DRIVEN DATA"."DEBIT_NOTE"
            WHERE "From" >= DATE '2023-01-01'
            """
        ).fetchdf()

        # Load CLIENT CASH RECEIVED
        status_text.text("💵 Loading client cash...")
        progress_bar.progress(80)
        try:
            CLIENT_CASH = conn.execute(
                """
                SELECT Date, groupname, Amount, Year, Month
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
                WHERE Date >= DATE '2023-01-01'
                """
            ).fetchdf()
        except:
            CLIENT_CASH = pd.DataFrame(columns=['Date', 'groupname', 'Amount', 'Year', 'Month'])

        conn.close()

        # Convert to Polars
        status_text.text("🔄 Converting to Polars...")
        progress_bar.progress(90)

        GROUP_CONTRACT = pl.from_pandas(GROUP_CONTRACT)
        CLAIMS = pl.from_pandas(CLAIMS)
        GROUPS = pl.from_pandas(GROUPS)
        DEBIT = pl.from_pandas(DEBIT)
        CLIENT_CASH = pl.from_pandas(CLIENT_CASH)
        PA = pl.from_pandas(PA)
        MEMBERS = pl.from_pandas(MEMBERS)
        PROVIDER = pl.from_pandas(PROVIDER)

        status_text.text("✅ Data loaded successfully!")
        progress_bar.progress(100)

        import time
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()

        return PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, CLIENT_CASH, MEMBERS, PROVIDER

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None, None, None, None, None, None, None, None

@st.cache_data(ttl=3600)
def load_group_agent_emails():
    """Load groupname to agent email mapping"""
    try:
        df = pd.read_csv('EMAIL_MLR_Sheet1.csv')
        mapping = {}
        mapping_upper = {}
        mapping_lower = {}

        for _, row in df.iterrows():
            groupname = str(row['groupname']).strip()
            email = row.get('EMAIL', '')

            if pd.notna(email):
                email = str(email).strip()
                if email and email.lower() not in ['nan', 'none', '', 'not active']:
                    mapping[groupname] = email
                    mapping_upper[groupname.upper()] = email
                    mapping_lower[groupname.lower()] = email

        return mapping, mapping_upper, mapping_lower
    except Exception as e:
        st.error(f"Error loading agent emails: {str(e)}")
        return {}, {}, {}

# ============================================================================
# MLR CALCULATION WITH ENHANCED ERROR HANDLING
# ============================================================================

@st.cache_data(ttl=1800)
def calculate_mlr_with_pmpm(PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, CLIENT_CASH, MEMBERS):
    """Calculate MLR metrics with PMPM tracking and enhanced debit note handling"""
    try:
        # Commission rate exceptions
        commission_rate_expr = (
            pl.when(pl.col('groupname') == 'CARAWAY AFRICA NIGERIA LIMITED').then(0.12)
            .when(pl.col('groupname') == 'POLARIS BANK PLC').then(0.15)
            .when(pl.col('groupname') == 'LORNA NIGERIA LIMITED (GODREJ)').then(0.20)
            .otherwise(0.10)
        )

        # Prepare contract dates
        group_contract_dates = GROUP_CONTRACT.select(['groupname', 'startdate', 'enddate', 'iscurrent']).with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])

        # Get current contracts only (STRICT FILTER)
        current_contracts = group_contract_dates.filter(
            (pl.col('iscurrent') == 1) &
            (pl.col('enddate') >= datetime.now())  # Contract must still be active
        )

        # Get list of current group names
        current_groupnames = current_contracts.select('groupname').unique()

        # ---- CLAIMS PROCESSING ----
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('datesubmitted').cast(pl.Datetime),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])

        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid', right_on='groupid', how='inner'
        )

        # ONLY include claims from groups with current contracts
        claims_with_group = claims_with_group.join(
            current_groupnames,
            on='groupname',
            how='inner'
        )

        claims_with_dates = claims_with_group.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname', how='inner'
        ).filter(
            (pl.col('datesubmitted') >= pl.col('startdate')) &
            (pl.col('datesubmitted') <= pl.col('enddate'))
        )

        # Sum claims by group
        claims_sum = claims_with_dates.group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('claims_amount')
        )

        # Calculate unclaimed PA (only for current contracts)
        PA = PA.with_columns([
            pl.col('panumber').cast(pl.Utf8),
            pl.col('groupname').cast(pl.Utf8, strict=False),
            pl.col('requestdate').cast(pl.Datetime),
            pl.col('granted').cast(pl.Float64, strict=False)
        ])

        # ONLY process PA for groups with current contracts
        PA = PA.join(
            current_groupnames,
            on='groupname',
            how='inner'
        )

        claims_panumbers = claims_with_dates.with_columns([
            pl.col('panumber').cast(pl.Int64, strict=False),
            pl.col('groupname').cast(pl.Utf8)
        ]).filter(
            pl.col('panumber').is_not_null()
        ).select('panumber', 'groupname').unique()

        pa_with_dates = PA.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname', how='inner'
        ).filter(
            (pl.col('requestdate') >= pl.col('startdate')) &
            (pl.col('requestdate') <= pl.col('enddate')) &
            pl.col('panumber').is_not_null()
        ).with_columns([
            pl.col('panumber').cast(pl.Int64, strict=False)
        ])

        unclaimed_pa_rows = pa_with_dates.join(
            claims_panumbers,
            on=['panumber', 'groupname'],
            how='anti'
        )

        unclaimed_pa = unclaimed_pa_rows.group_by('groupname').agg(
            pl.col('granted').sum().alias('unclaimed_pa_amount')
        )

        # Total medical cost = claims + unclaimed PA
        claims_sum = claims_sum.with_columns(pl.col('groupname').cast(pl.Utf8))
        unclaimed_pa = unclaimed_pa.with_columns(pl.col('groupname').cast(pl.Utf8))

        claims_mlr_base = claims_sum.join(unclaimed_pa, on='groupname', how='left').with_columns([
            pl.col('unclaimed_pa_amount').fill_null(0.0),
            pl.col('claims_amount').fill_null(0.0)
        ]).with_columns([
            (pl.col('claims_amount') + pl.col('unclaimed_pa_amount')).alias('total_medical_cost')
        ])

        # ---- ENHANCED DEBIT NOTE PROCESSING ----
        if not isinstance(DEBIT, pd.DataFrame):
            DEBIT = DEBIT.to_pandas()

        DEBIT['From'] = pd.to_datetime(DEBIT['From'])
        DEBIT['CompanyName'] = DEBIT['CompanyName'].str.strip()

        # Filter out TPA
        CURRENT_DEBIT = DEBIT[~DEBIT['Description'].str.contains('tpa', case=False, na=False)].copy()

        # Function to find debit for a group with date adjustment
        def get_debit_for_group(groupname, startdate, enddate):
            """Get debit amount for group, adjusting dates if needed"""
            # Try exact period first
            group_debit = CURRENT_DEBIT[
                (CURRENT_DEBIT['CompanyName'] == groupname) &
                (CURRENT_DEBIT['From'] >= startdate) &
                (CURRENT_DEBIT['From'] <= enddate)
            ]

            if len(group_debit) > 0:
                return group_debit['Amount'].sum()

            # Try adjusting start date to 1st of month
            adjusted_start = startdate.replace(day=1)
            group_debit = CURRENT_DEBIT[
                (CURRENT_DEBIT['CompanyName'] == groupname) &
                (CURRENT_DEBIT['From'] >= adjusted_start) &
                (CURRENT_DEBIT['From'] <= enddate)
            ]

            if len(group_debit) > 0:
                return group_debit['Amount'].sum()

            # No debit found
            return None

        # Build debit amounts with enhanced logic
        debit_results = []
        current_contracts_pd = current_contracts.to_pandas()

        for _, contract in current_contracts_pd.iterrows():
            groupname = contract['groupname']
            startdate = pd.to_datetime(contract['startdate'])
            enddate = pd.to_datetime(contract['enddate'])

            debit_amount = get_debit_for_group(groupname, startdate, enddate)

            if debit_amount is not None:
                debit_results.append({
                    'groupname': groupname,
                    'debit_amount': debit_amount
                })

        DEBIT_BY_CLIENT = pl.from_pandas(pd.DataFrame(debit_results))

        # ---- MEMBER COUNT (for PMPM) ----
        members_per_group = MEMBERS.join(
            GROUPS.select(['groupid', 'groupname']).with_columns(pl.col('groupid').cast(pl.Int64)),
            on='groupid',
            how='inner'
        ).filter(
            (pl.col('iscurrent') == 1) & (pl.col('isterminated') == 0)
        ).group_by('groupname').agg(
            pl.col('memberid').n_unique().alias('member_count')
        )

        # ---- MERGE EVERYTHING ----
        today_year = pl.lit(datetime.now().year)
        today_month = pl.lit(datetime.now().month)
        today_day = pl.lit(datetime.now().day)

        # Start with groups that have claims
        final_df = claims_mlr_base.with_columns(pl.col('groupname').cast(pl.Utf8))

        # Add debit
        final_df = final_df.join(
            DEBIT_BY_CLIENT.select(['groupname', 'debit_amount']),
            on='groupname',
            how='left'
        )

        # Add member count
        final_df = final_df.join(
            members_per_group.with_columns(pl.col('groupname').cast(pl.Utf8)),
            on='groupname',
            how='left'
        )

        # Add contract info
        final_df = final_df.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname',
            how='left'
        )

        # Calculate metrics
        final_df = final_df.with_columns([
            commission_rate_expr.alias('commission_rate'),
            pl.when(pl.col('debit_amount').is_not_null())
            .then(pl.col('debit_amount') * commission_rate_expr)
            .otherwise(pl.lit(0))
            .round(2).alias('commission'),
            (
                ((pl.col('enddate').dt.year() - today_year) * 12 +
                 (pl.col('enddate').dt.month() - today_month) -
                 (pl.col('enddate').dt.day() < today_day).cast(pl.Int64))
                .clip(lower_bound=0)
            ).alias('months_to_contract_end'),
            # Calculate ELAPSED months (from contract start to now)
            (
                ((today_year - pl.col('startdate').dt.year()) * 12 +
                 (today_month - pl.col('startdate').dt.month()) +
                 (today_day >= pl.col('startdate').dt.day()).cast(pl.Int64))
                .clip(lower_bound=1)  # At least 1 month
            ).alias('months_elapsed'),
            # Calculate TOTAL contract duration
            (
                ((pl.col('enddate').dt.year() - pl.col('startdate').dt.year()) * 12 +
                 (pl.col('enddate').dt.month() - pl.col('startdate').dt.month()) + 1)
            ).alias('total_contract_months')
        ])

        # Calculate MLR and PMPM
        final_df = final_df.with_columns([
            # MLR
            pl.when(pl.col('debit_amount').is_not_null())
            .then(
                ((pl.col('total_medical_cost') + pl.col('commission')) /
                 pl.col('debit_amount') * 100).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('MLR (%)'),

            # Contract Average PMPM = Total Medical Cost / (Member Count * Months Elapsed)
            pl.when(
                (pl.col('member_count').is_not_null()) &
                (pl.col('months_elapsed') > 0)
            )
            .then(
                (pl.col('total_medical_cost') /
                 (pl.col('member_count') * pl.col('months_elapsed'))).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Avg_PMPM'),

            # Premium PMPM (based on total contract duration)
            pl.when(
                (pl.col('debit_amount').is_not_null()) &
                (pl.col('member_count').is_not_null()) &
                (pl.col('total_contract_months') > 0)
            )
            .then(
                (pl.col('debit_amount') /
                 (pl.col('member_count') * pl.col('total_contract_months'))).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Premium_PMPM')
        ])

        # Calculate Last Month PMPM and Last 3 Months PMPM
        # Get last 30 days and last 90 days claims
        today = datetime.now()
        last_30_days = today - timedelta(days=30)
        last_90_days = today - timedelta(days=90)

        # Last month claims
        last_month_claims = claims_with_dates.filter(
            pl.col('datesubmitted') >= last_30_days
        ).group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('last_month_cost')
        )

        # Last 3 months claims
        last_3month_claims = claims_with_dates.filter(
            pl.col('datesubmitted') >= last_90_days
        ).group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('last_3month_cost')
        )

        # Join last month and last 3 month data
        final_df = final_df.join(
            last_month_claims.with_columns(pl.col('groupname').cast(pl.Utf8)),
            on='groupname',
            how='left'
        ).join(
            last_3month_claims.with_columns(pl.col('groupname').cast(pl.Utf8)),
            on='groupname',
            how='left'
        )

        # Calculate Last Month PMPM and Last 3 Months PMPM
        final_df = final_df.with_columns([
            # Last Month PMPM
            pl.when(
                (pl.col('last_month_cost').is_not_null()) &
                (pl.col('member_count').is_not_null()) &
                (pl.col('member_count') > 0)
            )
            .then(
                (pl.col('last_month_cost') / pl.col('member_count')).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Last_Month_PMPM'),

            # Last 3 Months PMPM (divide by 3 for average per month)
            pl.when(
                (pl.col('last_3month_cost').is_not_null()) &
                (pl.col('member_count').is_not_null()) &
                (pl.col('member_count') > 0)
            )
            .then(
                (pl.col('last_3month_cost') / pl.col('member_count') / 3).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Last_3Mo_PMPM')
        ])

        # Select final columns
        result = final_df.select([
            'groupname',
            'debit_amount',
            'total_medical_cost',
            'claims_amount',
            'unclaimed_pa_amount',
            'commission',
            'MLR (%)',
            'member_count',
            'months_elapsed',
            'total_contract_months',
            'Avg_PMPM',
            'Last_Month_PMPM',
            'Last_3Mo_PMPM',
            'Premium_PMPM',
            'months_to_contract_end'
        ])

        return result

    except Exception as e:
        st.error(f"Error calculating MLR with PMPM: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return pl.DataFrame()

# ============================================================================
# MONTHLY PMPM TREND CALCULATION
# ============================================================================

@st.cache_data(ttl=1800)
def calculate_monthly_pmpm_trends(CLAIMS, GROUPS, MEMBERS, GROUP_CONTRACT, current_date=None):
    """Calculate monthly PMPM trends WITHIN EACH GROUP'S CONTRACT PERIOD ONLY"""
    try:
        if current_date is None:
            current_date = datetime.now()

        # Get current contracts only
        current_contracts = GROUP_CONTRACT.filter(
            (pl.col('iscurrent') == 1) &
            (pl.col('enddate') >= datetime.now())
        ).select(['groupname', 'startdate', 'enddate']).with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])

        # Prepare data
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('datesubmitted').cast(pl.Datetime),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])

        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid', right_on='groupid', how='inner'
        )

        # Join claims with contract dates to filter within contract period
        claims_with_contracts = claims_with_group.join(
            current_contracts,
            on='groupname',
            how='inner'
        ).filter(
            # Only include claims within the contract period
            (pl.col('datesubmitted') >= pl.col('startdate')) &
            (pl.col('datesubmitted') <= pl.col('enddate'))
        )

        # Get member counts per group (current only)
        current_groupnames = current_contracts.select('groupname').unique()
        members_per_group = MEMBERS.join(
            GROUPS.select(['groupid', 'groupname']).with_columns(pl.col('groupid').cast(pl.Int64)),
            on='groupid',
            how='inner'
        ).join(
            current_groupnames,
            on='groupname',
            how='inner'
        ).filter(
            (pl.col('iscurrent') == 1) & (pl.col('isterminated') == 0)
        ).group_by('groupname').agg(
            pl.col('memberid').n_unique().alias('member_count')
        )

        # Extract year-month from claims and group by it
        claims_with_month = claims_with_contracts.with_columns([
            pl.col('datesubmitted').dt.strftime('%Y-%m').alias('month')
        ])

        # Group by groupname and month
        monthly_costs = claims_with_month.group_by(['groupname', 'month']).agg(
            pl.col('approvedamount').sum().alias('monthly_cost')
        )

        # Join with member counts and calculate PMPM
        monthly_pmpm = monthly_costs.join(
            members_per_group,
            on='groupname',
            how='left'
        ).with_columns([
            pl.when(pl.col('member_count') > 0)
            .then((pl.col('monthly_cost') / pl.col('member_count')).round(2))
            .otherwise(pl.lit(None))
            .alias('pmpm')
        ])

        # Also add contract dates for reference
        monthly_pmpm = monthly_pmpm.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname',
            how='left'
        )

        return monthly_pmpm

    except Exception as e:
        st.error(f"Error calculating monthly PMPM: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return pl.DataFrame()

# ============================================================================
# CONCENTRATION RISK ANALYSIS
# ============================================================================

@st.cache_data(ttl=1800)
def analyze_concentration_risks(CLAIMS, GROUPS, PROVIDER, GROUP_CONTRACT):
    """Analyze member and provider concentration risks (CURRENT CONTRACTS ONLY)"""
    try:
        # Get current contracts only
        current_contracts = GROUP_CONTRACT.filter(
            (pl.col('iscurrent') == 1) &
            (pl.col('enddate') >= datetime.now())
        ).select(['groupname']).unique()

        # Prepare claims
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])

        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid', right_on='groupid', how='inner'
        )

        # ONLY include claims from groups with current contracts
        claims_with_group = claims_with_group.join(
            current_contracts,
            on='groupname',
            how='inner'
        )

        # --- MEMBER CONCENTRATION ---
        # Total cost per group
        group_totals = claims_with_group.group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('total_cost')
        )

        # Top member per group
        member_costs = claims_with_group.group_by(['groupname', 'nhislegacynumber']).agg(
            pl.col('approvedamount').sum().alias('member_cost')
        )

        # Get top member percentage
        member_concentration = member_costs.join(
            group_totals,
            on='groupname',
            how='left'
        ).with_columns([
            (pl.col('member_cost') / pl.col('total_cost') * 100).round(2).alias('pct_of_total')
        ]).sort(['groupname', 'pct_of_total'], descending=[False, True])

        top_members = member_concentration.group_by('groupname').agg([
            pl.col('pct_of_total').max().alias('top_member_pct'),
            pl.col('member_cost').max().alias('top_member_cost')
        ])

        # --- PROVIDER CONCENTRATION ---
        provider_costs = claims_with_group.join(
            PROVIDER.select(['providerid', 'providername']),
            left_on='nhisproviderid',
            right_on='providerid',
            how='left'
        ).group_by(['groupname', 'providername']).agg(
            pl.col('approvedamount').sum().alias('provider_cost')
        )

        provider_concentration = provider_costs.join(
            group_totals,
            on='groupname',
            how='left'
        ).with_columns([
            (pl.col('provider_cost') / pl.col('total_cost') * 100).round(2).alias('pct_of_total')
        ]).sort(['groupname', 'pct_of_total'], descending=[False, True])

        top_providers = provider_concentration.group_by('groupname').agg([
            pl.col('pct_of_total').max().alias('top_provider_pct'),
            pl.col('provider_cost').max().alias('top_provider_cost'),
            pl.col('providername').first().alias('top_provider_name')
        ])

        # Combine results
        concentration_results = top_members.join(
            top_providers,
            on='groupname',
            how='outer'
        )

        return concentration_results

    except Exception as e:
        st.error(f"Error analyzing concentration: {str(e)}")
        return pl.DataFrame()

# ============================================================================
# MEDICAL COST ANALYSIS
# ============================================================================

@st.cache_data(ttl=1800)
def calculate_medical_cost_analysis(
    company_name: str,
    start_date,
    end_date,
    CLAIMS,
    GROUPS,
    PA,
    PLAN,
    PROVIDER
):
    """
    Comprehensive medical cost analysis for a specific company and period
    Uses only CLAIMS as the primary data source

    Returns detailed breakdown including:
    - Total medical cost (Claims + Unclaimed PA)
    - Enrollee counts
    - Hospital and enrollee rankings with monthly details
    - PMPM analysis
    """
    try:
        # Convert dates to datetime
        start_datetime = pd.Timestamp(start_date)
        end_datetime = pd.Timestamp(end_date)

        # Get group ID
        group_info = GROUPS.filter(pl.col('groupname') == company_name)
        if group_info.height == 0:
            return {'error': f'Company "{company_name}" not found in database'}

        group_id = str(group_info.select('groupid').item())

        # === 1. CLAIMS ANALYSIS ===
        # Note: enrollee_id is aliased as nhislegacynumber in the loaded CLAIMS dataframe
        claims_filtered = CLAIMS.filter(
            (pl.col('nhisgroupid').cast(pl.Utf8) == group_id) &
            (pl.col('datesubmitted') >= start_datetime) &
            (pl.col('datesubmitted') <= end_datetime)
        )

        if claims_filtered.height == 0:
            return {'error': 'No claims found for this company and period'}

        total_claims_paid = claims_filtered.select(pl.col('approvedamount').sum()).item() or 0

        # === 2. UNCLAIMED PA ===
        # PA issued in period
        pa_in_period = PA.filter(
            (pl.col('groupname') == company_name) &
            (pl.col('requestdate') >= start_datetime) &
            (pl.col('requestdate') <= end_datetime) &
            (pl.col('panumber').is_not_null()) &
            (pl.col('granted') > 0)
        ).with_columns([
            pl.col('panumber').cast(pl.Utf8, strict=False).str.strip_chars().alias('panumber_str')
        ]).filter(
            (pl.col('panumber_str').is_not_null()) &
            (pl.col('panumber_str') != '') &
            (pl.col('panumber_str') != '0')
        )

        # Claims panumbers from period - cast to string for reliable matching
        claims_panumbers = claims_filtered.filter(
            pl.col('panumber').is_not_null()
        ).with_columns([
            pl.col('panumber').cast(pl.Int64, strict=False).cast(pl.Utf8).alias('panumber_str')
        ]).filter(
            (pl.col('panumber_str').is_not_null()) &
            (pl.col('panumber_str') != '') &
            (pl.col('panumber_str') != '0')
        ).select('panumber_str').unique()

        # Unclaimed PA = PA issued but not claimed
        unclaimed_pa = pa_in_period.join(
            claims_panumbers,
            on='panumber_str',
            how='anti'
        )

        total_unclaimed_pa = unclaimed_pa.select(pl.col('granted').sum()).item() or 0
        total_medical_cost = total_claims_paid + total_unclaimed_pa

        # === 3. ENROLLEE ANALYSIS ===
        # Use nhislegacynumber which is the enrollee_id
        unique_enrollees = claims_filtered.select(pl.col('nhislegacynumber').n_unique()).item() or 0

        # === 4. MONTHLY PMPM ANALYSIS ===
        # Calculate number of months
        months_diff = (end_datetime.year - start_datetime.year) * 12 + (end_datetime.month - start_datetime.month) + 1

        # Monthly breakdown - using claims as proxy for active members
        claims_monthly = claims_filtered.with_columns([
            pl.col('datesubmitted').dt.strftime('%Y-%m').alias('month')
        ]).group_by('month').agg([
            pl.col('approvedamount').sum().alias('monthly_cost'),
            pl.col('nhislegacynumber').n_unique().alias('monthly_enrollees')
        ]).with_columns([
            (pl.col('monthly_cost') / pl.col('monthly_enrollees')).alias('pmpm')
        ]).sort('month')

        # Overall PMPM calculation
        overall_pmpm = total_medical_cost / (unique_enrollees * months_diff) if (unique_enrollees * months_diff) > 0 else 0

        # === 5. TOP 10 PROVIDERS (HOSPITALS) WITH MONTHLY BREAKDOWN ===
        claims_with_provider = claims_filtered.join(
            PROVIDER.select(['providerid', 'providername']),
            left_on='nhisproviderid',
            right_on='providerid',
            how='left'
        )

        # Fill null provider names
        claims_with_provider = claims_with_provider.with_columns([
            pl.when(pl.col('providername').is_null())
            .then(pl.lit('Unknown Provider'))
            .otherwise(pl.col('providername'))
            .alias('providername')
        ])

        # Overall top 10
        top_providers = claims_with_provider.group_by('providername').agg([
            pl.col('approvedamount').sum().alias('total_cost'),
            pl.col('nhislegacynumber').n_unique().alias('unique_enrollees'),
            pl.col('panumber').n_unique().alias('visit_count')
        ]).sort('total_cost', descending=True).head(10)

        # Monthly breakdown for top 10
        top_provider_names = top_providers.select('providername').to_series().to_list()

        provider_monthly = claims_with_provider.filter(
            pl.col('providername').is_in(top_provider_names)
        ).with_columns([
            pl.col('datesubmitted').dt.strftime('%Y-%m').alias('month')
        ]).group_by(['providername', 'month']).agg([
            pl.col('approvedamount').sum().alias('monthly_cost')
        ]).sort(['providername', 'month'])

        # === 6. TOP 10 ENROLLEES WITH MONTHLY BREAKDOWN ===
        top_enrollees = claims_filtered.group_by('nhislegacynumber').agg([
            pl.col('approvedamount').sum().alias('total_cost'),
            pl.col('panumber').n_unique().alias('visit_count')
        ]).with_columns([
            (pl.col('total_cost') / pl.col('visit_count')).alias('avg_cost_per_visit')
        ]).sort('total_cost', descending=True).head(10)

        # Monthly breakdown for top 10
        top_enrollee_ids = top_enrollees.select('nhislegacynumber').to_series().to_list()

        enrollee_monthly = claims_filtered.filter(
            pl.col('nhislegacynumber').is_in(top_enrollee_ids)
        ).with_columns([
            pl.col('datesubmitted').dt.strftime('%Y-%m').alias('month')
        ]).group_by(['nhislegacynumber', 'month']).agg([
            pl.col('approvedamount').sum().alias('monthly_cost')
        ]).sort(['nhislegacynumber', 'month'])

        return {
            'success': True,
            'total_claims_paid': total_claims_paid,
            'total_unclaimed_pa': total_unclaimed_pa,
            'total_medical_cost': total_medical_cost,
            'unique_enrollees': unique_enrollees,
            'months_count': months_diff,
            'pmpm_monthly': claims_monthly,
            'overall_pmpm': overall_pmpm,
            'top_providers': top_providers,
            'provider_monthly': provider_monthly,
            'top_enrollees': top_enrollees,
            'enrollee_monthly': enrollee_monthly
        }

    except Exception as e:
        import traceback
        return {'error': f'{str(e)}\n\nTraceback:\n{traceback.format_exc()}'}

# ============================================================================
# ALERT GENERATION SYSTEM
# ============================================================================

def generate_comprehensive_alerts(mlr_df, monthly_pmpm, concentration_df, sender_password):
    """Generate all types of alerts"""
    alerts_sent = []
    group_agent_emails, group_agent_emails_upper, group_agent_emails_lower = load_group_agent_emails()

    if mlr_df.height == 0:
        return alerts_sent

    mlr_pandas = mlr_df.to_pandas()

    for _, row in mlr_pandas.iterrows():
        groupname = str(row['groupname']).strip()

        if not groupname or groupname.lower() == 'none':
            continue

        mlr_value = row.get('MLR (%)')
        avg_pmpm = row.get('Avg_PMPM')
        last_month_pmpm = row.get('Last_Month_PMPM')
        last_3mo_pmpm = row.get('Last_3Mo_PMPM')
        premium_pmpm = row.get('Premium_PMPM')
        months_left = row.get('months_to_contract_end', 0)

        # Get agent email
        agent_email = (group_agent_emails.get(groupname) or
                      group_agent_emails_upper.get(groupname.upper()) or
                      group_agent_emails_lower.get(groupname.lower()))

        # Skip invalid MLR values
        try:
            if pd.notna(mlr_value):
                mlr_float = float(mlr_value)
                if mlr_float == float('inf') or mlr_float == float('-inf'):
                    continue
            else:
                continue
        except (ValueError, TypeError):
            continue

        # --- ALERT 1: MLR >= 70% ---
        if mlr_float >= ALERT_THRESHOLDS['mlr_critical']:
            cooldown = get_notification_cooldown("mlr_critical")
            if should_send_notification(groupname, "mlr_critical", cooldown):
                subject = f"🚨 CRITICAL: {groupname} MLR at {mlr_float:.1f}%"

                # Format values safely
                avg_pmpm_str = f"₦{avg_pmpm:,.2f}" if pd.notna(avg_pmpm) else "N/A"
                last_month_str = f"₦{last_month_pmpm:,.2f}" if pd.notna(last_month_pmpm) else "N/A"
                last_3mo_str = f"₦{last_3mo_pmpm:,.2f}" if pd.notna(last_3mo_pmpm) else "N/A"
                premium_str = f"₦{premium_pmpm:,.2f}" if pd.notna(premium_pmpm) else "N/A"
                margin = premium_pmpm - last_month_pmpm if pd.notna(last_month_pmpm) and pd.notna(premium_pmpm) else 0

                body = f"""CRITICAL ALERT

Group: {groupname}
MLR: {mlr_float:.1f}%
Months to Contract End: {months_left}

PMPM Analysis:
- Contract Average PMPM: {avg_pmpm_str}
- Last Month PMPM: {last_month_str}
- Last 3 Months PMPM: {last_3mo_str}
- Premium PMPM: {premium_str}

Current Margin: ₦{margin:,.2f} per member

Action Required: Immediate intervention needed.

Dashboard: [Your Dashboard URL]
"""

                if agent_email and send_email_alert([agent_email], subject, body, sender_password):
                    save_notification(groupname, "mlr_critical")
                    alerts_sent.append(f"{groupname} - MLR Critical → Agent")

                mgmt_cooldown = get_notification_cooldown("mlr_critical_mgmt")
                if months_left > 3 and should_send_notification(groupname, "mlr_critical_mgmt", mgmt_cooldown):
                    if send_email_alert(MANAGEMENT_EMAILS, subject, body, sender_password):
                        save_notification(groupname, "mlr_critical_mgmt")
                        alerts_sent.append(f"{groupname} - MLR Critical → Management")

        # --- ALERT 2: NEGATIVE PMPM (Check Last Month first, then Average) ---
        # Check Last Month PMPM first (most urgent - current bleeding)
        if pd.notna(last_month_pmpm) and pd.notna(premium_pmpm):
            if last_month_pmpm > premium_pmpm:
                # Current negative margin!
                cooldown = get_notification_cooldown("negative_pmpm_current")
                if should_send_notification(groupname, "negative_pmpm_current", cooldown):
                    loss_per_member = last_month_pmpm - premium_pmpm
                    avg_pmpm_val = avg_pmpm if pd.notna(avg_pmpm) else 0
                    member_count = row.get('member_count', 0) or 0
                    months_elapsed = row.get('months_elapsed', 0) or 0
                    monthly_loss = loss_per_member * member_count

                    subject = f"🚨 URGENT: CURRENT NEGATIVE MARGIN - {groupname}"
                    body = f"""🚨 URGENT - CURRENT NEGATIVE MARGIN ALERT

Group: {groupname}
Last Month Medical PMPM: ₦{last_month_pmpm:,.2f}
Premium PMPM: ₦{premium_pmpm:,.2f}
CURRENT Loss per Member: ₦{loss_per_member:,.2f}

CONTRACT AVERAGE (for comparison):
Average Medical PMPM: ₦{avg_pmpm_val:,.2f} (over {months_elapsed} months elapsed)

THIS GROUP IS CURRENTLY LOSING MONEY!

Estimated Current Monthly Loss: ₦{monthly_loss:,.2f}

Action Required: IMMEDIATE repricing or utilization review.
This is based on LAST 30 DAYS performance, not contract average!
"""

                    if agent_email and send_email_alert([agent_email], subject, body, sender_password):
                        save_notification(groupname, "negative_pmpm_current")
                        alerts_sent.append(f"{groupname} - Current Negative PMPM → Agent")

        # Also check contract average PMPM (structural issue)
        elif pd.notna(avg_pmpm) and pd.notna(premium_pmpm):
            if avg_pmpm > premium_pmpm:
                # Structural negative margin
                cooldown = get_notification_cooldown("negative_pmpm_avg")
                if should_send_notification(groupname, "negative_pmpm_avg", cooldown):
                    loss_per_member = avg_pmpm - premium_pmpm
                    last_month_val = last_month_pmpm if pd.notna(last_month_pmpm) else 0
                    last_3mo_val = last_3mo_pmpm if pd.notna(last_3mo_pmpm) else 0
                    member_count = row.get('member_count', 0) or 0
                    months_elapsed = row.get('months_elapsed', 0) or 0
                    total_loss = loss_per_member * member_count * months_elapsed

                    subject = f"⚠️ STRUCTURAL NEGATIVE MARGIN: {groupname}"
                    body = f"""STRUCTURAL NEGATIVE MARGIN ALERT

Group: {groupname}
Contract Average Medical PMPM: ₦{avg_pmpm:,.2f}
Premium PMPM: ₦{premium_pmpm:,.2f}
Average Loss per Member per Month: ₦{loss_per_member:,.2f}

Recent Trend:
Last Month PMPM: ₦{last_month_val:,.2f}
Last 3 Months PMPM: ₦{last_3mo_val:,.2f}

This group has been unprofitable on average throughout the contract!

Total Contract Loss: ₦{total_loss:,.2f}

Action Required: Review pricing for renewal.
"""

                    if agent_email and send_email_alert([agent_email], subject, body, sender_password):
                        save_notification(groupname, "negative_pmpm_avg")
                        alerts_sent.append(f"{groupname} - Structural Negative PMPM → Agent")

        # --- ALERT 3: CONCENTRATION RISKS ---
        if concentration_df.height > 0:
            conc_row = concentration_df.filter(pl.col('groupname') == groupname)
            if conc_row.height > 0:
                conc_data = conc_row.to_pandas().iloc[0]

                # Member concentration
                top_member_pct = conc_data.get('top_member_pct', 0)
                if pd.notna(top_member_pct) and top_member_pct > ALERT_THRESHOLDS['member_concentration']:
                    cooldown = get_notification_cooldown("member_concentration")
                    if should_send_notification(groupname, "member_concentration", cooldown):
                        subject = f"⚠️ HIGH MEMBER CONCENTRATION: {groupname}"
                        body = f"""HIGH-COST MEMBER ALERT

Group: {groupname}
Top Member: {top_member_pct:.1f}% of total cost
Amount: ₦{conc_data.get('top_member_cost', 0):,.2f}

Single member is driving {top_member_pct:.1f}% of your costs!

Action Required: Review this member's utilization.
"""

                        if agent_email and send_email_alert([agent_email], subject, body, sender_password):
                            save_notification(groupname, "member_concentration")
                            alerts_sent.append(f"{groupname} - Member Concentration → Agent")

                # Provider concentration
                top_provider_pct = conc_data.get('top_provider_pct', 0)
                if pd.notna(top_provider_pct) and top_provider_pct > ALERT_THRESHOLDS['provider_concentration']:
                    cooldown = get_notification_cooldown("provider_concentration")
                    if should_send_notification(groupname, "provider_concentration", cooldown):
                        subject = f"⚠️ HIGH PROVIDER CONCENTRATION: {groupname}"
                        body = f"""PROVIDER CONCENTRATION ALERT

Group: {groupname}
Top Provider: {conc_data.get('top_provider_name', 'Unknown')}
Percentage: {top_provider_pct:.1f}% of total cost
Amount: ₦{conc_data.get('top_provider_cost', 0):,.2f}

Single provider is dominating this group's costs!

Action Required: Negotiate rates or diversify providers.
"""

                        if agent_email and send_email_alert([agent_email], subject, body, sender_password):
                            save_notification(groupname, "provider_concentration")
                            alerts_sent.append(f"{groupname} - Provider Concentration → Agent")

    # --- ALERT 4: CONSECUTIVE NEGATIVE PMPM MONTHS ---
    if monthly_pmpm.height > 0:
        monthly_pd = monthly_pmpm.to_pandas()
        for groupname in monthly_pd['groupname'].unique():
            group_months = monthly_pd[monthly_pd['groupname'] == groupname].sort_values('month')

            if len(group_months) >= 2:
                # Check for consecutive negative months
                # (This would require premium data per month, simplified here)
                pass

    return alerts_sent

# ============================================================================
# MAIN APP
# ============================================================================

if __name__ == "__main__":
    # Email configuration
    st.sidebar.header("📧 Email Configuration")

    try:
        default_password = st.secrets.get('gmail', {}).get('app_password', '') or \
                          st.secrets.get('email', {}).get('sender_password', '')
    except:
        default_password = ''

    if not default_password:
        default_password = os.getenv('GMAIL_APP_PASSWORD', '')

    if default_password:
        email_password = default_password
        st.sidebar.success("✅ Gmail password loaded")
    else:
        email_password = st.sidebar.text_input("Gmail App Password", type="password")

    email_alerts_enabled = st.sidebar.checkbox("Enable Email Alerts", value=True)

    # Alert thresholds
    st.sidebar.header("⚠️ Alert Thresholds")
    with st.sidebar.expander("Customize Thresholds"):
        ALERT_THRESHOLDS['mlr_critical'] = st.number_input("Critical MLR %", value=70.0, step=5.0)
        ALERT_THRESHOLDS['member_concentration'] = st.number_input("Member Concentration %", value=10.0, step=1.0)
        ALERT_THRESHOLDS['provider_concentration'] = st.number_input("Provider Concentration %", value=40.0, step=5.0)

    # Load data
    st.header("📊 Loading Data...")
    PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, CLIENT_CASH, MEMBERS, PROVIDER = load_data_from_duckdb()

    if all(df is not None for df in [PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, MEMBERS, PROVIDER]):
        # Calculate MLR with PMPM
        st.header("💰 Calculating MLR & PMPM...")
        mlr_df = calculate_mlr_with_pmpm(PA, GROUP_CONTRACT, CLAIMS, GROUPS, DEBIT, CLIENT_CASH, MEMBERS)

        # Calculate monthly PMPM trends
        st.header("📈 Calculating Monthly Trends...")
        monthly_pmpm = calculate_monthly_pmpm_trends(CLAIMS, GROUPS, MEMBERS, GROUP_CONTRACT)

        # Analyze concentration risks
        st.header("🎯 Analyzing Concentration Risks...")
        concentration_df = analyze_concentration_risks(CLAIMS, GROUPS, PROVIDER, GROUP_CONTRACT)

        # Generate alerts
        if email_alerts_enabled and email_password:
            st.header("🚨 Checking for Alerts...")
            with st.spinner("Generating alerts..."):
                alerts_sent = generate_comprehensive_alerts(
                    mlr_df, monthly_pmpm, concentration_df, email_password
                )

                if alerts_sent:
                    st.success(f"✅ Sent {len(alerts_sent)} alerts!")
                    with st.expander("View Alerts Sent"):
                        for alert in alerts_sent:
                            st.write(f"• {alert}")
                else:
                    st.info("ℹ️ No new alerts to send")

        # Display results
        st.markdown("---")
        st.header("📊 Current MLR & PMPM Status")

        if mlr_df.height > 0:
            mlr_pandas = mlr_df.to_pandas()

            # Color coding function
            def highlight_risks(row):
                colors = [''] * len(row)

                mlr_val = row.get('MLR (%)')
                if pd.notna(mlr_val):
                    try:
                        mlr_float = float(mlr_val)
                        if mlr_float >= 85:
                            colors = ['background-color: #ff6b6b; color: white; font-weight: bold'] * len(row)
                        elif mlr_float >= 75:
                            colors = ['background-color: #ffa500; color: white; font-weight: bold'] * len(row)
                        elif mlr_float >= 65:
                            colors = ['background-color: #ffeb3b; color: black; font-weight: bold'] * len(row)
                    except:
                        pass

                return colors

            styled_df = mlr_pandas.style.apply(highlight_risks, axis=1)
            st.dataframe(styled_df, use_container_width=True)

            # Download button
            csv = mlr_pandas.to_csv(index=False)
            st.download_button(
                label="📥 Download MLR Report",
                data=csv,
                file_name=f"mlr_report_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

            # ================================================================
            # FILTERED TABLES SECTION
            # ================================================================
            st.markdown("---")
            st.header("🔍 Filtered Risk Views")

            # Create tabs for different filters
            tab1, tab2, tab3 = st.tabs([
                "⚠️ High MLR (>50%) with 3+ Months Left",
                "📉 Negative PMPM Clients",
                "🎯 Custom Filter"
            ])

            with tab1:
                st.subheader("Clients with MLR > 50% and More Than 3 Months Left")
                st.caption("These clients need intervention before contract renewal")

                # Filter: MLR > 50% AND months_to_contract_end > 3
                high_mlr_actionable = mlr_pandas[
                    (mlr_pandas['MLR (%)'].notna()) &
                    (mlr_pandas['MLR (%)'] > 50) &
                    (mlr_pandas['months_to_contract_end'] > 3)
                ].sort_values('MLR (%)', ascending=False)

                if len(high_mlr_actionable) > 0:
                    # Display key columns
                    display_cols = ['groupname', 'MLR (%)', 'months_to_contract_end',
                                   'total_medical_cost', 'debit_amount', 'member_count',
                                   'Last_Month_PMPM', 'Premium_PMPM']
                    available_cols = [col for col in display_cols if col in high_mlr_actionable.columns]

                    st.dataframe(
                        high_mlr_actionable[available_cols].style.apply(highlight_risks, axis=1),
                        use_container_width=True
                    )

                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Clients", len(high_mlr_actionable))
                    with col2:
                        avg_mlr = high_mlr_actionable['MLR (%)'].mean()
                        st.metric("Average MLR", f"{avg_mlr:.1f}%")
                    with col3:
                        total_cost = high_mlr_actionable['total_medical_cost'].sum()
                        st.metric("Total Medical Cost", f"₦{total_cost:,.0f}")

                    # Download button
                    csv_high_mlr = high_mlr_actionable.to_csv(index=False)
                    st.download_button(
                        label="📥 Download High MLR List",
                        data=csv_high_mlr,
                        file_name=f"high_mlr_actionable_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key="download_high_mlr"
                    )
                else:
                    st.success("✅ No clients with MLR > 50% and more than 3 months left on contract")

            with tab2:
                st.subheader("Clients with Negative PMPM (Losing Money)")
                st.caption("Medical cost per member exceeds premium per member")

                # Filter: Last_Month_PMPM > Premium_PMPM OR Avg_PMPM > Premium_PMPM
                negative_pmpm = mlr_pandas[
                    (
                        (mlr_pandas['Last_Month_PMPM'].notna()) &
                        (mlr_pandas['Premium_PMPM'].notna()) &
                        (mlr_pandas['Last_Month_PMPM'] > mlr_pandas['Premium_PMPM'])
                    ) |
                    (
                        (mlr_pandas['Avg_PMPM'].notna()) &
                        (mlr_pandas['Premium_PMPM'].notna()) &
                        (mlr_pandas['Avg_PMPM'] > mlr_pandas['Premium_PMPM'])
                    )
                ].copy()

                if len(negative_pmpm) > 0:
                    # Calculate loss per member
                    negative_pmpm['Monthly_Loss_Per_Member'] = negative_pmpm.apply(
                        lambda row: (row['Last_Month_PMPM'] - row['Premium_PMPM'])
                        if pd.notna(row['Last_Month_PMPM']) and pd.notna(row['Premium_PMPM'])
                        else (row['Avg_PMPM'] - row['Premium_PMPM'])
                        if pd.notna(row['Avg_PMPM']) and pd.notna(row['Premium_PMPM'])
                        else 0,
                        axis=1
                    )

                    negative_pmpm['Est_Monthly_Loss'] = negative_pmpm['Monthly_Loss_Per_Member'] * negative_pmpm['member_count'].fillna(0)

                    # Sort by estimated monthly loss
                    negative_pmpm = negative_pmpm.sort_values('Est_Monthly_Loss', ascending=False)

                    # Display key columns
                    display_cols = ['groupname', 'Last_Month_PMPM', 'Avg_PMPM', 'Premium_PMPM',
                                   'Monthly_Loss_Per_Member', 'Est_Monthly_Loss', 'member_count',
                                   'months_to_contract_end']
                    available_cols = [col for col in display_cols if col in negative_pmpm.columns]

                    # Color negative losses in red
                    def highlight_losses(row):
                        colors = [''] * len(row)
                        if 'Est_Monthly_Loss' in row.index and pd.notna(row.get('Est_Monthly_Loss', 0)):
                            if row['Est_Monthly_Loss'] > 100000:
                                colors = ['background-color: #ff6b6b; color: white'] * len(row)
                            elif row['Est_Monthly_Loss'] > 50000:
                                colors = ['background-color: #ffa500; color: white'] * len(row)
                            elif row['Est_Monthly_Loss'] > 0:
                                colors = ['background-color: #ffeb3b; color: black'] * len(row)
                        return colors

                    st.dataframe(
                        negative_pmpm[available_cols].style.apply(highlight_losses, axis=1),
                        use_container_width=True
                    )

                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Losing Clients", len(negative_pmpm))
                    with col2:
                        total_monthly_loss = negative_pmpm['Est_Monthly_Loss'].sum()
                        st.metric("Total Monthly Loss", f"₦{total_monthly_loss:,.0f}")
                    with col3:
                        avg_loss_per_member = negative_pmpm['Monthly_Loss_Per_Member'].mean()
                        st.metric("Avg Loss/Member", f"₦{avg_loss_per_member:,.0f}")

                    # Download button
                    csv_negative = negative_pmpm.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Negative PMPM List",
                        data=csv_negative,
                        file_name=f"negative_pmpm_clients_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key="download_negative_pmpm"
                    )
                else:
                    st.success("✅ No clients with negative PMPM (all clients are profitable)")

            with tab3:
                st.subheader("Custom Filter")

                # MLR Range filter
                col1, col2 = st.columns(2)
                with col1:
                    min_mlr = st.number_input("Minimum MLR %", value=0.0, step=5.0, key="min_mlr")
                with col2:
                    max_mlr = st.number_input("Maximum MLR %", value=200.0, step=5.0, key="max_mlr")

                # Months left filter
                col3, col4 = st.columns(2)
                with col3:
                    min_months = st.number_input("Min Months Left", value=0, step=1, key="min_months")
                with col4:
                    max_months = st.number_input("Max Months Left", value=12, step=1, key="max_months")

                # Apply custom filter
                custom_filtered = mlr_pandas[
                    (mlr_pandas['MLR (%)'].notna()) &
                    (mlr_pandas['MLR (%)'] >= min_mlr) &
                    (mlr_pandas['MLR (%)'] <= max_mlr) &
                    (mlr_pandas['months_to_contract_end'] >= min_months) &
                    (mlr_pandas['months_to_contract_end'] <= max_months)
                ].sort_values('MLR (%)', ascending=False)

                if len(custom_filtered) > 0:
                    st.dataframe(
                        custom_filtered.style.apply(highlight_risks, axis=1),
                        use_container_width=True
                    )

                    st.metric("Matching Clients", len(custom_filtered))

                    # Download button
                    csv_custom = custom_filtered.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Filtered List",
                        data=csv_custom,
                        file_name=f"custom_filter_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key="download_custom"
                    )
                else:
                    st.info("No clients match the selected criteria")

        # Monthly PMPM Trends
        st.markdown("---")
        st.header("📈 Monthly PMPM Trends")

        if monthly_pmpm.height > 0:
            # Allow user to select a group
            groups_list = sorted(monthly_pmpm.select('groupname').unique().to_series().to_list())
            selected_group = st.selectbox("Select Group for Trend", groups_list, key="pmpm_trend_group")

            if selected_group:
                group_trend = monthly_pmpm.filter(pl.col('groupname') == selected_group).to_pandas()

                if len(group_trend) > 0:
                    # Sort by month for proper display
                    group_trend = group_trend.sort_values('month')

                    # Get contract period
                    contract_start = group_trend['startdate'].iloc[0] if 'startdate' in group_trend.columns else None
                    contract_end = group_trend['enddate'].iloc[0] if 'enddate' in group_trend.columns else None

                    # Get Premium PMPM for this group from mlr_df
                    premium_pmpm_value = None
                    if mlr_df.height > 0:
                        group_mlr_data = mlr_df.filter(pl.col('groupname') == selected_group)
                        if group_mlr_data.height > 0:
                            premium_pmpm_value = group_mlr_data.select('Premium_PMPM').to_pandas()['Premium_PMPM'].iloc[0]

                    st.subheader(f"PMPM Trend: {selected_group}")

                    # Show contract period
                    if contract_start is not None and contract_end is not None:
                        start_str = pd.to_datetime(contract_start).strftime('%b %Y')
                        end_str = pd.to_datetime(contract_end).strftime('%b %Y')
                        st.caption(f"📅 Contract Period: {start_str} to {end_str}")

                    # Create chart data with both Medical PMPM and Premium PMPM line
                    chart_data = group_trend[['month', 'pmpm']].copy()
                    chart_data = chart_data.set_index('month')
                    chart_data.columns = ['Medical PMPM']

                    # Add Premium PMPM as a constant line for comparison
                    if premium_pmpm_value is not None and pd.notna(premium_pmpm_value):
                        chart_data['Premium PMPM'] = premium_pmpm_value
                        st.caption(f"🎯 Premium PMPM: ₦{premium_pmpm_value:,.2f} (shown as reference line)")

                    st.line_chart(
                        data=chart_data,
                        use_container_width=True
                    )

                    # Show legend explanation
                    if premium_pmpm_value is not None and pd.notna(premium_pmpm_value):
                        st.caption("📊 When Medical PMPM is **above** Premium PMPM = **Losing money** | When **below** = **Profitable**")

                    # Show summary stats
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        avg_pmpm = group_trend['pmpm'].mean()
                        st.metric("Avg Medical PMPM", f"₦{avg_pmpm:,.2f}" if pd.notna(avg_pmpm) else "N/A")
                    with col2:
                        min_pmpm = group_trend['pmpm'].min()
                        st.metric("Min PMPM", f"₦{min_pmpm:,.2f}" if pd.notna(min_pmpm) else "N/A")
                    with col3:
                        max_pmpm = group_trend['pmpm'].max()
                        st.metric("Max PMPM", f"₦{max_pmpm:,.2f}" if pd.notna(max_pmpm) else "N/A")
                    with col4:
                        if premium_pmpm_value is not None and pd.notna(premium_pmpm_value):
                            st.metric("Premium PMPM", f"₦{premium_pmpm_value:,.2f}")
                        else:
                            st.metric("Premium PMPM", "N/A")
                    with col5:
                        num_months = len(group_trend)
                        st.metric("Months", num_months)
                else:
                    st.info(f"No PMPM data available for {selected_group}")
        else:
            st.info("No monthly PMPM data available")

        # Concentration Risks
        st.markdown("---")
        st.header("🎯 Concentration Risk Analysis")

        if concentration_df.height > 0:
            conc_pandas = concentration_df.to_pandas()

            # Filter high risks
            high_member_risk = conc_pandas[conc_pandas['top_member_pct'] > ALERT_THRESHOLDS['member_concentration']]
            high_provider_risk = conc_pandas[conc_pandas['top_provider_pct'] > ALERT_THRESHOLDS['provider_concentration']]

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("High Member Concentration")
                if len(high_member_risk) > 0:
                    st.dataframe(high_member_risk[['groupname', 'top_member_pct', 'top_member_cost']], use_container_width=True)
                else:
                    st.success("✅ No high member concentration risks")

            with col2:
                st.subheader("High Provider Concentration")
                if len(high_provider_risk) > 0:
                    st.dataframe(high_provider_risk[['groupname', 'top_provider_name', 'top_provider_pct']], use_container_width=True)
                else:
                    st.success("✅ No high provider concentration risks")

        # --- MEDICAL COST ANALYSIS SECTION ---
        st.markdown("---")
        st.header("💰 Medical Cost Analysis")
        st.info("Comprehensive medical cost analysis for any company and time period")

        # Input Parameters
        col1, col2 = st.columns(2)

        with col1:
            # Company selection
            unique_companies = GROUPS.select('groupname').unique().sort('groupname').to_series().to_list()
            selected_company_mc = st.selectbox(
                "Select Company",
                options=unique_companies,
                key="medical_cost_company"
            )

            # Date range
            mc_start_date = st.date_input(
                "Start Date",
                value=date.today() - timedelta(days=365),
                key="mc_start_date"
            )

            mc_end_date = st.date_input(
                "End Date",
                value=date.today(),
                key="mc_end_date"
            )

        with col2:
            st.markdown("#### Financial Data Options")

            # Get debit note amount for the period
            if DEBIT is not None and not DEBIT.is_empty():
                debit_pandas = DEBIT.to_pandas()
                debit_pandas['From'] = pd.to_datetime(debit_pandas['From'])

                debit_amount = debit_pandas[
                    (debit_pandas['CompanyName'] == selected_company_mc) &
                    (debit_pandas['From'] >= pd.Timestamp(mc_start_date)) &
                    (debit_pandas['From'] <= pd.Timestamp(mc_end_date)) &
                    (~debit_pandas['Description'].str.contains('tpa', case=False, na=False))
                ]['Amount'].sum()
            else:
                debit_amount = 0

            # Debit Note option
            use_debit = st.radio(
                f"Use Debit Note Amount: ₦{debit_amount:,.2f}?",
                options=["Yes", "No"],
                key="use_debit_mc",
                horizontal=True
            )

            if use_debit == "No":
                custom_debit = st.number_input(
                    "Enter Custom Debit Amount (₦)",
                    min_value=0.0,
                    value=float(debit_amount) if debit_amount > 0 else 0.0,
                    step=1000.0,
                    key="custom_debit_mc"
                )
                final_debit = custom_debit
            else:
                final_debit = debit_amount

            # Get cash received amount
            if CLIENT_CASH is not None and not CLIENT_CASH.is_empty():
                cash_pandas = CLIENT_CASH.to_pandas()
                cash_pandas['Date'] = pd.to_datetime(cash_pandas['Date'])

                cash_amount = cash_pandas[
                    (cash_pandas['groupname'] == selected_company_mc) &
                    (cash_pandas['Date'] >= pd.Timestamp(mc_start_date)) &
                    (cash_pandas['Date'] <= pd.Timestamp(mc_end_date))
                ]['Amount'].sum()
            else:
                cash_amount = 0

            # Cash received option
            use_cash = st.radio(
                f"Use Cash Received: ₦{cash_amount:,.2f}?",
                options=["Yes", "No"],
                key="use_cash_mc",
                horizontal=True
            )

            if use_cash == "No":
                custom_cash = st.number_input(
                    "Enter Custom Cash Amount (₦)",
                    min_value=0.0,
                    value=float(cash_amount) if cash_amount > 0 else 0.0,
                    step=1000.0,
                    key="custom_cash_mc"
                )
                final_cash = custom_cash
            else:
                final_cash = cash_amount

        # Generate Analysis Button
        if st.button("Generate Medical Cost Analysis", type="primary"):
            with st.spinner("Analyzing medical costs..."):
                # Run analysis
                analysis_result = calculate_medical_cost_analysis(
                    selected_company_mc,
                    mc_start_date,
                    mc_end_date,
                    CLAIMS,
                    GROUPS,
                    PA,
                    None,  # PLAN - loaded internally by the function when needed
                    PROVIDER
                )

                if 'error' in analysis_result:
                    st.error(f"Analysis failed: {analysis_result['error']}")
                else:
                    # Display Results
                    st.success("✅ Analysis completed!")

                    # === HEADER ===
                    st.markdown("---")
                    st.subheader(f"📊 Medical Cost Analysis: {selected_company_mc}")
                    st.markdown(f"**Period:** {mc_start_date.strftime('%B %d, %Y')} to {mc_end_date.strftime('%B %d, %Y')}")

                    # === KEY METRICS ===
                    st.markdown("### 1️⃣ Financial Overview")

                    metric_col1, metric_col2, metric_col3, metric_col4 = st.columns(4)

                    with metric_col1:
                        st.metric(
                            "Total Medical Cost",
                            f"₦{analysis_result['total_medical_cost']:,.2f}",
                            help="Claims Paid + Unclaimed PA"
                        )

                    with metric_col2:
                        # Calculate BV MLR
                        bv_mlr = (analysis_result['total_medical_cost'] / final_debit * 100) if final_debit > 0 else 0
                        st.metric(
                            "BV MLR",
                            f"{bv_mlr:.1f}%",
                            delta=f"{bv_mlr - 75:.1f}% vs 75% threshold",
                            delta_color="inverse"
                        )

                    with metric_col3:
                        # Calculate Cash MLR
                        cash_mlr = (analysis_result['total_medical_cost'] / final_cash * 100) if final_cash > 0 else 0
                        st.metric(
                            "Cash MLR",
                            f"{cash_mlr:.1f}%",
                            delta=f"{cash_mlr - 75:.1f}% vs 75% threshold",
                            delta_color="inverse"
                        )

                    with metric_col4:
                        st.metric(
                            "Unique Enrollees",
                            f"{analysis_result['unique_enrollees']:,}",
                            help="Number of enrollees who claimed"
                        )

                    # Breakdown
                    breakdown_col1, breakdown_col2, breakdown_col3 = st.columns(3)

                    with breakdown_col1:
                        st.info(f"**Claims Paid:** ₦{analysis_result['total_claims_paid']:,.2f}")

                    with breakdown_col2:
                        st.info(f"**Unclaimed PA:** ₦{analysis_result['total_unclaimed_pa']:,.2f}")

                    with breakdown_col3:
                        unclaimed_pct = (analysis_result['total_unclaimed_pa'] / analysis_result['total_medical_cost'] * 100) if analysis_result['total_medical_cost'] > 0 else 0
                        st.info(f"**Unclaimed %:** {unclaimed_pct:.1f}%")

                    # === PMPM ANALYSIS ===
                    st.markdown("---")
                    st.markdown("### 2️⃣ PMPM Monthly Analysis")

                    if analysis_result['pmpm_monthly'].height > 0:
                        pmpm_df = analysis_result['pmpm_monthly'].to_pandas()

                        # Display chart
                        st.line_chart(
                            data=pmpm_df.set_index('month')['pmpm'],
                            use_container_width=True
                        )

                        # Display table
                        st.dataframe(
                            pmpm_df.rename(columns={
                                'month': 'Month',
                                'monthly_cost': 'Monthly Cost (₦)',
                                'monthly_enrollees': 'Active Enrollees',
                                'pmpm': 'PMPM (₦)'
                            }).style.format({
                                'Monthly Cost (₦)': '₦{:,.2f}',
                                'PMPM (₦)': '₦{:,.2f}'
                            }),
                            use_container_width=True
                        )

                        # Overall PMPM
                        avg_pmpm_claims = pmpm_df['pmpm'].mean()

                        pmpm_col1, pmpm_col2 = st.columns(2)
                        with pmpm_col1:
                            st.metric("Average PMPM (Claims Only)", f"₦{avg_pmpm_claims:,.2f}")
                        with pmpm_col2:
                            st.metric("Overall PMPM (Incl. Unclaimed PA)", f"₦{analysis_result['overall_pmpm']:,.2f}")
                    else:
                        st.warning("No monthly data available")

                    # === TOP 10 HOSPITALS ===
                    st.markdown("---")
                    st.markdown("### 3️⃣ Top 10 Hospitals (Providers)")

                    if analysis_result['top_providers'].height > 0:
                        providers_df = analysis_result['top_providers'].to_pandas()

                        st.dataframe(
                            providers_df.rename(columns={
                                'providername': 'Provider Name',
                                'total_cost': 'Total Cost (₦)',
                                'unique_enrollees': 'Unique Enrollees'
                            }).style.format({
                                'Total Cost (₦)': '₦{:,.2f}'
                            }),
                            use_container_width=True
                        )

                        # Monthly breakdown
                        st.markdown("#### Monthly Breakdown by Hospital")

                        if analysis_result['provider_monthly'].height > 0:
                            provider_monthly_df = analysis_result['provider_monthly'].to_pandas()

                            # Pivot for display
                            provider_pivot = provider_monthly_df.pivot(
                                index='providername',
                                columns='month',
                                values='monthly_cost'
                            ).fillna(0)

                            st.dataframe(
                                provider_pivot.style.format('₦{:,.2f}'),
                                use_container_width=True
                            )
                    else:
                        st.warning("No provider data available")

                    # === TOP 10 ENROLLEES ===
                    st.markdown("---")
                    st.markdown("### 4️⃣ Top 10 Enrollees")

                    if analysis_result['top_enrollees'].height > 0:
                        enrollees_df = analysis_result['top_enrollees'].to_pandas()

                        st.dataframe(
                            enrollees_df.rename(columns={
                                'nhislegacynumber': 'Enrollee ID',
                                'total_cost': 'Total Cost (₦)',
                                'visit_count': 'Visit Count',
                                'avg_cost_per_visit': 'Avg Cost/Visit (₦)'
                            }).style.format({
                                'Total Cost (₦)': '₦{:,.2f}',
                                'Avg Cost/Visit (₦)': '₦{:,.2f}'
                            }),
                            use_container_width=True
                        )

                        # Monthly breakdown
                        st.markdown("#### Monthly Breakdown by Enrollee")

                        if analysis_result['enrollee_monthly'].height > 0:
                            enrollee_monthly_df = analysis_result['enrollee_monthly'].to_pandas()

                            # Pivot for display
                            enrollee_pivot = enrollee_monthly_df.pivot(
                                index='nhislegacynumber',
                                columns='month',
                                values='monthly_cost'
                            ).fillna(0)

                            enrollee_pivot.index.name = 'Enrollee ID'

                            st.dataframe(
                                enrollee_pivot.style.format('₦{:,.2f}'),
                                use_container_width=True
                            )
                    else:
                        st.warning("No enrollee data available")

    else:
        st.error("❌ Failed to load data. Please check your database connection.")
