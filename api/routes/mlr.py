"""
MLR Analysis Endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Optional, List
from datetime import datetime, date, timedelta
import polars as pl
import pandas as pd
import numpy as np
from core.database import get_db_connection
import io

router = APIRouter()

# Simple in-process cache for heavy MLR base data
_MLR_DATA_CACHE = None
_MLR_DATA_CACHE_TS: Optional[datetime] = None
_MLR_DATA_TTL_SECONDS = 600  # 10 minutes (increased from 5 for better performance)
_MLR_DATA_STALE_TTL_SECONDS = 900  # Serve stale data for up to 15 minutes while refreshing

def clean_for_json(df):
    """Clean DataFrame for JSON serialization by replacing inf/nan with None"""
    # Replace infinity values
    df = df.replace([np.inf, -np.inf], None)
    # Replace NaN values
    df = df.replace({np.nan: None})
    # Final pass to ensure all non-finite values are None
    for col in df.columns:
        if df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].apply(lambda x: None if (pd.isna(x) or np.isinf(x)) else x)
    return df

def load_mlr_data(force_refresh: bool = False):
    """Load data needed for MLR calculations, with a small in-memory cache."""
    global _MLR_DATA_CACHE, _MLR_DATA_CACHE_TS

    # Return cached data if still fresh and not forced
    if (
        not force_refresh
        and _MLR_DATA_CACHE is not None
        and _MLR_DATA_CACHE_TS is not None
    ):
        age = (datetime.now() - _MLR_DATA_CACHE_TS).total_seconds()
        if age < _MLR_DATA_TTL_SECONDS:
            return _MLR_DATA_CACHE
    try:
        conn = get_db_connection()

        # Load required tables
        GROUP_CONTRACT = conn.execute(
            'SELECT groupid, startdate, enddate, iscurrent, groupname FROM "AI DRIVEN DATA"."GROUP_CONTRACT"'
        ).fetchdf()

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
            WHERE encounterdatefrom >= DATE '2024-01-01'
              AND nhisgroupid IS NOT NULL
            """
        ).fetchdf()

        GROUPS = conn.execute('SELECT * FROM "AI DRIVEN DATA"."GROUPS"').fetchdf()

        PA = conn.execute(
            """
            SELECT panumber, groupname, divisionname, plancode, IID, providerid,
                   requestdate, pastatus, code, userid, totaltariff, benefitcode,
                   dependantnumber, requested, granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE requestdate >= TIMESTAMP '2024-01-01'
            """
        ).fetchdf()

        MEMBERS = conn.execute(
            'SELECT memberid, groupid, enrollee_id as legacycode, planid, iscurrent, isterminated, effectivedate, terminationdate FROM "AI DRIVEN DATA"."MEMBERS"'
        ).fetchdf()

        DEBIT = conn.execute(
            """
            SELECT *
            FROM "AI DRIVEN DATA"."DEBIT_NOTE"
            WHERE "From" >= DATE '2023-01-01'
            """
        ).fetchdf()

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
        
        # Convert to Polars
        data = {
            'GROUP_CONTRACT': pl.from_pandas(GROUP_CONTRACT),
            'CLAIMS': pl.from_pandas(CLAIMS),
            'GROUPS': pl.from_pandas(GROUPS),
            'PA': pl.from_pandas(PA),
            'MEMBERS': pl.from_pandas(MEMBERS),
            'DEBIT': pl.from_pandas(DEBIT),
            'CLIENT_CASH': pl.from_pandas(CLIENT_CASH)
        }

        _MLR_DATA_CACHE = data
        _MLR_DATA_CACHE_TS = datetime.now()

        return data

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")


def calculate_mlr_basic(data_dict):
    """
    Calculate basic MLR metrics.

    MLR = (claims + unclaimed PA) / total debit cost (all within client current contract).
    - Claims: approvedamount where encounterdatefrom is within client current contract.
    - Unclaimed PA: granted amount where requestdate is within client current contract
      and panumber not yet claimed (no claim with that panumber and encounter in contract).
    - Denominator: total debit note amount for the client in the contract period.
    """
    try:
        GROUP_CONTRACT = data_dict['GROUP_CONTRACT']
        CLAIMS = data_dict['CLAIMS']
        GROUPS = data_dict['GROUPS']
        PA = data_dict['PA']
        MEMBERS = data_dict['MEMBERS']
        DEBIT = data_dict['DEBIT']

        # Commission rate logic
        commission_rate_expr = (
            pl.when(pl.col('groupname') == 'CARAWAY AFRICA NIGERIA LIMITED').then(0.12)
            .when(pl.col('groupname') == 'POLARIS BANK PLC').then(0.15)
            .when(pl.col('groupname') == 'LORNA NIGERIA LIMITED (GODREJ)').then(0.20)
            .otherwise(0.10)
        )

        # Get current contracts: contract in force today (startdate <= today <= enddate)
        group_contract_dates = GROUP_CONTRACT.select(
            ['groupname', 'startdate', 'enddate', 'iscurrent']
        ).with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime),
        ])

        today = datetime.now()
        current_contracts = group_contract_dates.filter(
            (pl.col('startdate') <= today) &
            (pl.col('enddate') >= today)
        )

        current_groupnames = current_contracts.select('groupname').unique()

        # Process claims: use encounterdatefrom within client current contract (not datesubmitted)
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('encounterdatefrom').cast(pl.Datetime),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])

        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid', right_on='groupid', how='inner'
        )

        claims_with_group = claims_with_group.join(
            current_groupnames,
            on='groupname',
            how='inner'
        )

        # Claims: only those with encounterdatefrom within client current contract period
        claims_with_dates = claims_with_group.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname', how='inner'
        ).filter(
            (pl.col('encounterdatefrom') >= pl.col('startdate')) &
            (pl.col('encounterdatefrom') <= pl.col('enddate'))
        )

        claims_sum = claims_with_dates.group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('claims_amount')
        )

        # Process unclaimed PA: only PA with requestdate within client current contract (already filtered below)
        PA = PA.with_columns([
            pl.col('panumber').cast(pl.Utf8),
            pl.col('groupname').cast(pl.Utf8, strict=False),
            pl.col('requestdate').cast(pl.Datetime),
            pl.col('granted').cast(pl.Float64, strict=False)
        ])

        PA = PA.join(current_groupnames, on='groupname', how='inner')

        # Normalize panumber so claims (e.g. 12345.0) and PA (e.g. "12345") match — avoid double-counting
        # Claimed = PAs that already have a claim; unclaimed = PA granted not yet in claims
        claims_panumbers = claims_with_dates.with_columns([
            pl.col('panumber').cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8).alias('panumber_norm'),
            pl.col('groupname').cast(pl.Utf8)
        ]).filter(
            pl.col('panumber_norm').is_not_null() &
            (pl.col('panumber_norm') != '') &
            (pl.col('panumber_norm') != '0')
        ).select('panumber_norm', 'groupname').unique()

        pa_with_dates = PA.join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname', how='inner'
        ).filter(
            (pl.col('requestdate') >= pl.col('startdate')) &
            (pl.col('requestdate') <= pl.col('enddate')) &
            pl.col('panumber').is_not_null() &
            (pl.col('panumber') != '')
        ).with_columns([
            # Same normalization as claims: float->int->string so "12345.0" and 12345 match
            pl.col('panumber').cast(pl.Utf8, strict=False).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8).alias('panumber_norm')
        ]).filter(
            pl.col('panumber_norm').is_not_null() & (pl.col('panumber_norm') != '') & (pl.col('panumber_norm') != '0')
        )

        unclaimed_pa_rows = pa_with_dates.join(
            claims_panumbers,
            on=['panumber_norm', 'groupname'],
            how='anti'
        )

        unclaimed_pa = unclaimed_pa_rows.group_by('groupname').agg(
            pl.col('granted').sum().alias('unclaimed_pa_amount')
        )

        # Combine claims and unclaimed PA
        claims_mlr_base = claims_sum.join(unclaimed_pa, on='groupname', how='left').with_columns([
            pl.col('unclaimed_pa_amount').fill_null(0.0),
            pl.col('claims_amount').fill_null(0.0)
        ]).with_columns([
            (pl.col('claims_amount') + pl.col('unclaimed_pa_amount')).alias('total_medical_cost')
        ])

        # Process debit notes
        DEBIT_pd = DEBIT.to_pandas()
        DEBIT_pd['From'] = pd.to_datetime(DEBIT_pd['From'])
        DEBIT_pd['CompanyName'] = DEBIT_pd['CompanyName'].str.strip()
        CURRENT_DEBIT = DEBIT_pd[~DEBIT_pd['Description'].str.contains('tpa', case=False, na=False)].copy()

        debit_results = []
        current_contracts_pd = current_contracts.to_pandas()

        for _, contract in current_contracts_pd.iterrows():
            groupname = contract['groupname']
            startdate = pd.to_datetime(contract['startdate'])
            enddate = pd.to_datetime(contract['enddate'])

            group_debit = CURRENT_DEBIT[
                (CURRENT_DEBIT['CompanyName'] == groupname) &
                (CURRENT_DEBIT['From'] >= startdate) &
                (CURRENT_DEBIT['From'] <= enddate)
            ]

            if len(group_debit) > 0:
                debit_results.append({
                    'groupname': groupname,
                    'debit_amount': group_debit['Amount'].sum()
                })

        DEBIT_BY_CLIENT = pl.from_pandas(pd.DataFrame(debit_results))

        # Get member counts
        members_per_group = MEMBERS.join(
            GROUPS.select(['groupid', 'groupname']).with_columns(pl.col('groupid').cast(pl.Int64)),
            on='groupid',
            how='inner'
        ).filter(
            (pl.col('iscurrent') == 1) & (pl.col('isterminated') == 0)
        ).group_by('groupname').agg(
            pl.col('memberid').n_unique().alias('member_count')
        )

        # Merge everything
        final_df = claims_mlr_base.join(
            DEBIT_BY_CLIENT.select(['groupname', 'debit_amount']),
            on='groupname',
            how='left'
        ).join(
            members_per_group,
            on='groupname',
            how='left'
        ).join(
            current_contracts.select(['groupname', 'startdate', 'enddate']),
            on='groupname',
            how='left'
        )

        # Calculate metrics
        today_year = pl.lit(datetime.now().year)
        today_month = pl.lit(datetime.now().month)
        today_day = pl.lit(datetime.now().day)

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
            (
                ((today_year - pl.col('startdate').dt.year()) * 12 +
                 (today_month - pl.col('startdate').dt.month()) +
                 (today_day >= pl.col('startdate').dt.day()).cast(pl.Int64))
                .clip(lower_bound=1)
            ).alias('months_elapsed'),
            (
                ((pl.col('enddate').dt.year() - pl.col('startdate').dt.year()) * 12 +
                 (pl.col('enddate').dt.month() - pl.col('startdate').dt.month()) + 1)
            ).alias('total_contract_months')
        ])

        # MLR = (claims by encounterdatefrom in contract + unclaimed PA by requestdate in contract) / total debit cost
        final_df = final_df.with_columns([
            pl.when(
                (pl.col('debit_amount').is_not_null()) &
                (pl.col('debit_amount') > 0)
            )
            .then(
                (pl.col('total_medical_cost') / pl.col('debit_amount') * 100).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('MLR (%)'),
            pl.when(
                (pl.col('member_count').is_not_null()) &
                (pl.col('member_count') > 0) &
                (pl.col('months_elapsed') > 0)
            )
            .then(
                (pl.col('total_medical_cost') /
                 (pl.col('member_count') * pl.col('months_elapsed'))).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Avg_PMPM'),
            pl.when(
                (pl.col('debit_amount').is_not_null()) &
                (pl.col('debit_amount') > 0) &
                (pl.col('member_count').is_not_null()) &
                (pl.col('member_count') > 0) &
                (pl.col('total_contract_months') > 0)
            )
            .then(
                (pl.col('debit_amount') /
                 (pl.col('member_count') * pl.col('total_contract_months'))).round(2)
            )
            .otherwise(pl.lit(None))
            .alias('Premium_PMPM')
        ])

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
            'Premium_PMPM',
            'months_to_contract_end'
        ])

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating MLR: {str(e)}")


def calculate_claims_paid_mlr(data_dict):
    """
    Calculate \"claims paid during policy\" MLR metrics.

    Basis:
    - Numerator: approvedamount where datesubmitted is within the client's current contract.
    - Denominator: total debit note amount for the client in the contract period (same as calculate_mlr_basic).
    """
    try:
        GROUP_CONTRACT = data_dict["GROUP_CONTRACT"]
        CLAIMS = data_dict["CLAIMS"]
        GROUPS = data_dict["GROUPS"]
        DEBIT = data_dict["DEBIT"]

        # Current contracts: in-force today (startdate <= today <= enddate)
        group_contract_dates = GROUP_CONTRACT.select(
            ["groupname", "startdate", "enddate", "iscurrent"]
        ).with_columns(
            [
                pl.col("startdate").cast(pl.Datetime),
                pl.col("enddate").cast(pl.Datetime),
            ]
        )

        today = datetime.now()
        current_contracts = group_contract_dates.filter(
            (pl.col("startdate") <= today) & (pl.col("enddate") >= today)
        )
        current_groupnames = current_contracts.select("groupname").unique()

        # Claims by datesubmitted in contract
        claims_datesub = CLAIMS.with_columns(
            [
                pl.col("approvedamount").cast(pl.Float64),
                pl.col("datesubmitted").cast(pl.Datetime),
                pl.col("nhisgroupid").cast(pl.Utf8),
            ]
        )

        groups_norm = GROUPS.select(["groupid", "groupname"]).with_columns(
            pl.col("groupid").cast(pl.Utf8)
        )

        claims_with_group = claims_datesub.join(
            groups_norm, left_on="nhisgroupid", right_on="groupid", how="inner"
        )

        claims_with_group = claims_with_group.join(
            current_groupnames, on="groupname", how="inner"
        )

        claims_with_dates = claims_with_group.join(
            current_contracts.select(["groupname", "startdate", "enddate"]),
            on="groupname",
            how="inner",
        ).filter(
            (pl.col("datesubmitted") >= pl.col("startdate"))
            & (pl.col("datesubmitted") <= pl.col("enddate"))
        )

        claims_paid_sum = claims_with_dates.group_by("groupname").agg(
            pl.col("approvedamount").sum().alias("claims_paid_amount")
        )

        # Debit notes in contract period (same logic as calculate_mlr_basic)
        DEBIT_pd = DEBIT.to_pandas()
        DEBIT_pd["From"] = pd.to_datetime(DEBIT_pd["From"])
        DEBIT_pd["CompanyName"] = DEBIT_pd["CompanyName"].str.strip()
        CURRENT_DEBIT = DEBIT_pd[
            ~DEBIT_pd["Description"].str.contains("tpa", case=False, na=False)
        ].copy()

        debit_results = []
        current_contracts_pd = current_contracts.to_pandas()

        for _, contract in current_contracts_pd.iterrows():
            groupname = contract["groupname"]
            startdate = pd.to_datetime(contract["startdate"])
            enddate = pd.to_datetime(contract["enddate"])

            group_debit = CURRENT_DEBIT[
                (CURRENT_DEBIT["CompanyName"] == groupname)
                & (CURRENT_DEBIT["From"] >= startdate)
                & (CURRENT_DEBIT["From"] <= enddate)
            ]

            if len(group_debit) > 0:
                debit_results.append(
                    {
                        "groupname": groupname,
                        "debit_amount": group_debit["Amount"].sum(),
                    }
                )

        if not debit_results:
            # No debit at all
            return pl.DataFrame(
                {"groupname": [], "debit_amount": [], "claims_paid_amount": []}
            )

        debit_by_client = pl.from_pandas(pd.DataFrame(debit_results))

        claims_paid_mlr = debit_by_client.join(
            claims_paid_sum, on="groupname", how="left"
        ).with_columns(
            [
                pl.col("claims_paid_amount").fill_null(0.0),
                pl.col("debit_amount").fill_null(0.0),
            ]
        ).with_columns(
            [
                pl.when(
                    (pl.col("debit_amount") > 0)
                    & pl.col("claims_paid_amount").is_not_null()
                )
                .then(
                    (pl.col("claims_paid_amount") / pl.col("debit_amount") * 100).round(
                        2
                    )
                )
                .otherwise(pl.lit(None))
                .alias("ClaimsPaid_MLR (%)")
            ]
        )

        return claims_paid_mlr
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error calculating claims-paid MLR: {str(e)}"
        )


@router.get("/companies")
async def get_companies(
    limit: Optional[int] = Query(None, description="Number of companies to return (default: all if not specified)", ge=1, le=10000),
    offset: int = Query(0, description="Number of companies to skip (default: 0)", ge=0)
):
    """
    Get list of companies with current contracts.
    If limit is not specified, returns ALL companies (for search functionality).
    If limit is specified, returns paginated results.
    """
    try:
        conn = get_db_connection()

        # Get total count for pagination info
        total_count_result = conn.execute(
            """
            SELECT COUNT(DISTINCT groupname) as total
            FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
            WHERE iscurrent = 1 AND enddate >= CURRENT_DATE
            """
        ).fetchone()
        total_count = total_count_result[0] if total_count_result else 0

        # Get companies (all if limit not specified, paginated if limit specified)
        if limit is None:
            # Return all companies for search
            companies = conn.execute(
                """
                SELECT DISTINCT groupname
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                WHERE iscurrent = 1 AND enddate >= CURRENT_DATE
                ORDER BY groupname
                """
            ).fetchdf()
        else:
            # Get paginated companies
            companies = conn.execute(
                f"""
                SELECT DISTINCT groupname
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                WHERE iscurrent = 1 AND enddate >= CURRENT_DATE
                ORDER BY groupname
                LIMIT {limit} OFFSET {offset}
                """
            ).fetchdf()

        # Note: Don't close connection - it's pooled and reused for performance

        return {
            "companies": companies['groupname'].tolist(),
            "pagination": {
                "limit": limit if limit else total_count,
                "offset": offset,
                "total": total_count,
                "has_more": (limit is not None) and ((offset + limit) < total_count)
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/calculate")
async def calculate_mlr(
    mlr_type: str = Query("claims", description="Type of MLR calculation: claims or cash"),
    limit: int = Query(10, description="Number of companies to return (default: 10)", ge=1, le=100),
    offset: int = Query(0, description="Number of companies to skip (default: 0)", ge=0),
    mlr_min: Optional[float] = Query(None, description="Filter: MLR (%) >= this value"),
    mlr_max: Optional[float] = Query(None, description="Filter: MLR (%) <= this value"),
):
    """
    Calculate MLR for companies with pagination and optional MLR range filter.
    Returns 10 companies at a time by default for faster loading.

    Parameters:
    - mlr_type: "claims" for claims-based MLR, "cash" for cash-based MLR
    - limit: Number of companies to return (default: 10, max: 100)
    - offset: Number of companies to skip (default: 0)
    - mlr_min: If set, only companies with MLR (%) >= this value
    - mlr_max: If set, only companies with MLR (%) <= this value (can combine with mlr_min for a range)
    """
    try:
        # Load data
        data_dict = load_mlr_data()

        # Calculate MLR for all companies first
        result_df = calculate_mlr_basic(data_dict)

        # Convert to pandas for pagination
        result_pandas = result_df.to_pandas()

        # Apply MLR range filter if requested
        if mlr_min is not None:
            result_pandas = result_pandas[result_pandas["MLR (%)"].notna() & (result_pandas["MLR (%)"] >= mlr_min)]
        if mlr_max is not None:
            result_pandas = result_pandas[result_pandas["MLR (%)"].notna() & (result_pandas["MLR (%)"] <= mlr_max)]

        # Get total count after filter, before pagination
        total_count = len(result_pandas)

        # Apply pagination
        paginated_data = result_pandas.iloc[offset:offset + limit]

        # Clean for JSON serialization
        paginated_data = clean_for_json(paginated_data)

        return {
            "mlr_type": mlr_type,
            "timestamp": datetime.now().isoformat(),
            "total_companies": total_count,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "returned": len(paginated_data),
                "has_more": (offset + limit) < total_count
            },
            "data": paginated_data.to_dict(orient='records')
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/calculate-claims-paid")
async def calculate_claims_paid_mlr_endpoint(
    limit: int = Query(10, description="Number of companies to return (default: 10)", ge=1, le=100),
    offset: int = Query(0, description="Number of companies to skip (default: 0)", ge=0),
    mlr_min: Optional[float] = Query(None, description="Filter: ClaimsPaid_MLR (%) >= this value"),
    mlr_max: Optional[float] = Query(None, description="Filter: ClaimsPaid_MLR (%) <= this value"),
):
    """
    Calculate \"claims paid during policy\" MLR for companies with pagination and optional range filter.

    Basis: claims by datesubmitted in current contract / total debit in contract.
    """
    try:
        data_dict = load_mlr_data()
        result_df = calculate_claims_paid_mlr(data_dict)

        # Convert to pandas for filtering/pagination
        result_pandas = result_df.to_pandas()

        # Apply MLR range filter if requested
        if mlr_min is not None:
            result_pandas = result_pandas[
                result_pandas["ClaimsPaid_MLR (%)"].notna()
                & (result_pandas["ClaimsPaid_MLR (%)"] >= mlr_min)
            ]
        if mlr_max is not None:
            result_pandas = result_pandas[
                result_pandas["ClaimsPaid_MLR (%)"].notna()
                & (result_pandas["ClaimsPaid_MLR (%)"] <= mlr_max)
            ]

        total_count = len(result_pandas)
        paginated_data = result_pandas.iloc[offset : offset + limit]
        paginated_data = clean_for_json(paginated_data)

        return {
            "timestamp": datetime.now().isoformat(),
            "total_companies": total_count,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "returned": len(paginated_data),
                "has_more": (offset + limit) < total_count,
            },
            "data": paginated_data.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error calculating claims-paid MLR: {str(e)}"
        )


@router.get("/export")
async def export_mlr_csv():
    """
    Export MLR for ALL companies as a CSV file.

    Uses the same current-contract MLR basis as the UI, and adds an "as_at"
    column with the date/time the file was generated.
    """
    try:
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)

        # Convert to pandas and add as_at timestamp
        result_pandas = result_df.to_pandas()
        as_at_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        result_pandas["as_at"] = as_at_str

        # Order columns: as_at first, then core metrics
        preferred_order = [
            "as_at",
            "groupname",
            "debit_amount",
            "total_medical_cost",
            "claims_amount",
            "unclaimed_pa_amount",
            "commission",
            "MLR (%)",
            "member_count",
            "Avg_PMPM",
            "Premium_PMPM",
        ]
        cols = [c for c in preferred_order if c in result_pandas.columns] + [
            c for c in result_pandas.columns if c not in preferred_order
        ]
        result_pandas = result_pandas[cols]

        # Build CSV in-memory
        output = io.StringIO()
        result_pandas.to_csv(output, index=False)
        output.seek(0)

        filename = f"mlr_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting MLR CSV: {str(e)}")


@router.get("/summary")
async def get_mlr_summary():
    """
    Get MLR summary statistics
    """
    try:
        # Load data and calculate
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)
        result_pandas = result_df.to_pandas()

        # Calculate summary stats
        mlr_values = result_pandas['MLR (%)'].dropna()

        summary = {
            "timestamp": datetime.now().isoformat(),
            "total_companies": len(result_pandas),
            "companies_with_mlr": len(mlr_values),
            "average_mlr": round(mlr_values.mean(), 2) if len(mlr_values) > 0 else None,
            "median_mlr": round(mlr_values.median(), 2) if len(mlr_values) > 0 else None,
            "min_mlr": round(mlr_values.min(), 2) if len(mlr_values) > 0 else None,
            "max_mlr": round(mlr_values.max(), 2) if len(mlr_values) > 0 else None,
            "high_risk_count": len(mlr_values[mlr_values >= 70]),
            "medium_risk_count": len(mlr_values[(mlr_values >= 50) & (mlr_values < 70)]),
            "low_risk_count": len(mlr_values[mlr_values < 50])
        }

        return summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary-stats")
async def get_summary_stats():
    """
    Get overall summary statistics for ALL companies (for dashboard cards).
    Returns: total_companies, average_mlr, high_risk_count, total_medical_cost
    """
    try:
        # Load data and calculate for ALL companies
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)
        result_pandas = result_df.to_pandas()

        # Calculate overall summary metrics
        total_companies = len(result_pandas)

        mlr_series = result_pandas["MLR (%)"].dropna()
        average_mlr = round(mlr_series.mean(), 2) if len(mlr_series) > 0 else 0

        # Existing high-risk count (for backward compatibility)
        high_risk_count = len(mlr_series[mlr_series > 70])

        # New distribution counts for dashboard cards
        above_75_count = len(mlr_series[mlr_series > 75])
        above_100_count = len(mlr_series[mlr_series >= 100])
        below_50_count = len(mlr_series[mlr_series < 50])

        # New metric: clients with 3–6 months remaining and MLR > 75%
        three_to_six_mask = (
            result_pandas["MLR (%)"].notna()
            & result_pandas["months_to_contract_end"].notna()
            & (result_pandas["MLR (%)"] > 75)
            & (result_pandas["months_to_contract_end"] >= 3)
            & (result_pandas["months_to_contract_end"] <= 6)
        )
        three_to_six_months_high_mlr_count = int(three_to_six_mask.sum())

        total_medical_cost = (
            result_pandas["total_medical_cost"].sum()
            if "total_medical_cost" in result_pandas.columns
            else 0
        )

        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "total_companies": total_companies,
                "average_mlr": average_mlr,
                "high_risk_count": high_risk_count,
                "above_75_count": int(above_75_count),
                "above_100_count": int(above_100_count),
                "below_50_count": int(below_50_count),
                "three_to_six_months_high_mlr_count": three_to_six_months_high_mlr_count,
                "total_medical_cost": float(total_medical_cost)
                if total_medical_cost
                else 0,
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/debit-bands")
async def get_debit_bands():
    """
    Aggregate MLR by debit bands for dashboard bar chart.

    Bands:
    - 0 - 5,000,000
    - 5,000,001 - 15,000,000
    - 15,000,001 - 30,000,000
    - 30,000,001 - 60,000,000
    - 60,000,001 - 100,000,000
    - 100,000,001 and above
    """
    try:
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)
        pdf = result_df.to_pandas()

        # Keep only rows with valid debit and MLR
        pdf = pdf[
            pdf["debit_amount"].notna()
            & (pdf["debit_amount"] > 0)
            & pdf["MLR (%)"].notna()
        ].copy()

        if pdf.empty:
            return {"bands": []}

        def band_label(amount: float) -> str:
            if amount <= 5_000_000:
                return "0 – 5,000,000"
            if amount <= 15_000_000:
                return "5,000,001 – 15,000,000"
            if amount <= 30_000_000:
                return "15,000,001 – 30,000,000"
            if amount <= 60_000_000:
                return "30,000,001 – 60,000,000"
            if amount <= 100_000_000:
                return "60,000,001 – 100,000,000"
            return "100,000,001 and above"

        pdf["band"] = pdf["debit_amount"].astype(float).apply(band_label)

        # Tier flags for distribution within each band
        pdf["over_75"] = pdf["MLR (%)"] > 75
        pdf["between_50_75"] = (pdf["MLR (%)"] >= 50) & (pdf["MLR (%)"] <= 75)
        pdf["below_50"] = pdf["MLR (%)"] < 50

        grouped = (
            pdf.groupby("band")
            .agg(
                avg_mlr=("MLR (%)", "mean"),
                company_count=("groupname", "count"),
                total_debit=("debit_amount", "sum"),
                over_75_count=("over_75", "sum"),
                between_50_75_count=("between_50_75", "sum"),
                below_50_count=("below_50", "sum"),
            )
            .reset_index()
        )

        # Order bands logically
        band_order = [
            "0 – 5,000,000",
            "5,000,001 – 15,000,000",
            "15,000,001 – 30,000,000",
            "30,000,001 – 60,000,000",
            "60,000,001 – 100,000,000",
            "100,000,001 and above",
        ]
        grouped["band_index"] = grouped["band"].apply(
            lambda b: band_order.index(b) if b in band_order else len(band_order)
        )
        grouped = grouped.sort_values("band_index")

        # Round avg_mlr for presentation
        grouped["avg_mlr"] = grouped["avg_mlr"].round(2)

        # Ensure integer counts
        for col in ["company_count", "over_75_count", "between_50_75_count", "below_50_count"]:
            grouped[col] = grouped[col].fillna(0).astype(int)

        # Clean for JSON
        out = grouped[
            [
                "band",
                "band_index",
                "avg_mlr",
                "company_count",
                "total_debit",
                "over_75_count",
                "between_50_75_count",
                "below_50_count",
            ]
        ]
        out = clean_for_json(out)
        return {"bands": out.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error calculating debit-band MLR: {str(e)}"
        )


@router.get("/company/{company_name}")
async def get_company_mlr(company_name: str):
    """
    Get MLR data for a specific company
    """
    try:
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)

        # Filter for specific company (case-insensitive, trimmed match)
        # Normalize the incoming name once
        target = company_name.strip().upper()
        company_data = result_df.filter(
            pl.col('groupname').str.strip_chars().str.to_uppercase() == target
        )

        if company_data.height == 0:
            raise HTTPException(status_code=404, detail=f"Company '{company_name}' not found")

        # Convert to pandas and then dict
        company_pandas = company_data.to_pandas()
        company_pandas = clean_for_json(company_pandas)

        return company_pandas.to_dict(orient='records')[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/critical-alerts")
async def get_mlr_critical_alerts():
    """
    Critical MLR alerts for clients (companies).
    
    Uses current-contract MLR data from calculate_mlr_basic and applies the
    following filters:
    
    1) Contracts expiring within 90 days (~3 months) AND MLR > 70%.
    2) Contracts expiring within 180 days (~6 months) AND MLR > 70%.
    3) MLR bands:
       - > 75 and < 85
       - >= 85 and < 100
       - >= 100
    4) Clients with debit_amount < 5,000,000 AND MLR >= 70%.
    """
    try:
        data_dict = load_mlr_data()
        result_df = calculate_mlr_basic(data_dict)  # Polars DataFrame

        # Work in Polars for filtering, then convert to JSON-safe dicts
        df = result_df

        mlr_col = pl.col('MLR (%)')
        debit_col = pl.col('debit_amount')
        months_to_end = pl.col('months_to_contract_end')

        # Common base filter: has a numeric MLR
        base_mlr_filter = mlr_col.is_not_null()

        # 1) Contracts expiring within 90 days (~3 months) AND MLR > 70%
        near_90 = (
            df.filter(
                base_mlr_filter
                & (mlr_col > 70)
                & (months_to_end >= 0)
                & (months_to_end <= 3)
            )
            .to_pandas()
        )
        near_90 = clean_for_json(near_90)

        # 2) Contracts expiring within 180 days (~6 months) AND MLR > 70%
        near_180 = (
            df.filter(
                base_mlr_filter
                & (mlr_col > 70)
                & (months_to_end >= 0)
                & (months_to_end <= 6)
            )
            .to_pandas()
        )
        near_180 = clean_for_json(near_180)

        # 3) MLR bands
        band_75_85 = (
            df.filter(base_mlr_filter & (mlr_col > 75) & (mlr_col < 85))
            .to_pandas()
        )
        band_75_85 = clean_for_json(band_75_85)

        band_85_100 = (
            df.filter(base_mlr_filter & (mlr_col >= 85) & (mlr_col < 100))
            .to_pandas()
        )
        band_85_100 = clean_for_json(band_85_100)

        band_100_plus = (
            df.filter(base_mlr_filter & (mlr_col >= 100))
            .to_pandas()
        )
        band_100_plus = clean_for_json(band_100_plus)

        # 4) Small debit (< 5,000,000) AND MLR >= 70%
        small_debit_high_mlr = (
            df.filter(
                base_mlr_filter
                & (mlr_col >= 70)
                & debit_col.is_not_null()
                & (debit_col < 5_000_000)
            )
            .to_pandas()
        )
        small_debit_high_mlr = clean_for_json(small_debit_high_mlr)

        # 5) Claimed paid during policy: claims (datesubmitted in contract) / debit >= 60%
        # Uses claims data only; date = datesubmitted (not encounterdatefrom). Use pandas for debit to avoid Polars join/schema issues.
        GROUP_CONTRACT = data_dict['GROUP_CONTRACT']
        CLAIMS = data_dict['CLAIMS']
        GROUPS = data_dict['GROUPS']
        DEBIT = data_dict['DEBIT']

        # Use the contract that is actually in force today: startdate <= today <= enddate.
        # This avoids accidentally picking a future contract that has no debit yet.
        today = datetime.now()
        current_contracts_pd = (
            GROUP_CONTRACT.filter(
                (pl.col('startdate').cast(pl.Datetime) <= today)
                & (pl.col('enddate').cast(pl.Datetime) >= today)
            )
            .select(['groupname', 'groupid', 'startdate', 'enddate'])
            .to_pandas()
        )
        current_contracts_pd['startdate'] = pd.to_datetime(current_contracts_pd['startdate'])
        current_contracts_pd['enddate'] = pd.to_datetime(current_contracts_pd['enddate'])

        # Claims by datesubmitted in contract (Polars then to pandas)
        claims_pl = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('datesubmitted').cast(pl.Datetime),
            pl.col('nhisgroupid').cast(pl.Utf8),
        ])
        groups_pd = GROUPS.select(['groupid', 'groupname']).with_columns(pl.col('groupid').cast(pl.Utf8)).to_pandas()
        claims_pd = claims_pl.to_pandas()
        claims_pd = claims_pd.merge(groups_pd, left_on='nhisgroupid', right_on='groupid', how='inner')
        claims_submitted_list = []
        for _, row in current_contracts_pd.iterrows():
            gn, start, end = row['groupname'], row['startdate'], row['enddate']
            mask = (claims_pd['groupname'] == gn) & (claims_pd['datesubmitted'] >= start) & (claims_pd['datesubmitted'] <= end)
            total = claims_pd.loc[mask, 'approvedamount'].sum()
            claims_submitted_list.append({'groupname': gn, 'total_claims_cost': float(total or 0)})
        claims_submitted_pd = pd.DataFrame(claims_submitted_list)

        # Debit by client in contract period (pandas, match existing MLR debit logic)
        debit_pd = DEBIT.to_pandas()
        debit_pd['From'] = pd.to_datetime(debit_pd['From'])
        debit_pd['CompanyName'] = debit_pd['CompanyName'].astype(str).str.strip()
        debit_pd['Amount'] = pd.to_numeric(debit_pd['Amount'], errors='coerce').fillna(0)
        if 'Description' in debit_pd.columns:
            debit_pd = debit_pd[~debit_pd['Description'].astype(str).str.lower().str.contains('tpa', na=False)]
        debit_list = []
        for _, row in current_contracts_pd.iterrows():
            gn, start, end = row['groupname'], row['startdate'], row['enddate']
            mask = (debit_pd['CompanyName'] == gn) & (debit_pd['From'] >= start) & (debit_pd['From'] <= end)
            total = debit_pd.loc[mask, 'Amount'].sum()
            debit_list.append({'groupname': gn, 'total_debit_cost': float(total or 0)})
        debit_by_client_pd = pd.DataFrame(debit_list)

        merged = claims_submitted_pd.merge(debit_by_client_pd, on='groupname', how='inner')
        merged['pct'] = (merged['total_claims_cost'] / merged['total_debit_cost'].replace(0, np.nan) * 100).round(2)
        claimed_paid_pandas = merged[(merged['total_debit_cost'] > 0) & (merged['pct'] >= 60)].copy()
        claimed_paid_pandas = claimed_paid_pandas.rename(columns={'groupname': 'client_name'})[['client_name', 'total_claims_cost', 'total_debit_cost', 'pct']]
        claimed_paid_pandas = clean_for_json(claimed_paid_pandas)

        return {
            "success": True,
            "timestamp": datetime.now().isoformat(),
            "data": {
                "near_contract_90": near_90.to_dict(orient='records'),
                "near_contract_180": near_180.to_dict(orient='records'),
                "mlr_bands": {
                    "gt_75_lt_85": band_75_85.to_dict(orient='records'),
                    "ge_85_lt_100": band_85_100.to_dict(orient='records'),
                    "ge_100": band_100_plus.to_dict(orient='records'),
                },
                "small_debit_high_mlr": small_debit_high_mlr.to_dict(orient='records'),
                "claimed_paid_during_policy": claimed_paid_pandas.to_dict(orient='records'),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/claimed-paid-during-policy/{company_name}")
async def get_claimed_paid_during_policy_for_company(company_name: str):
    """
    Get \"claimed paid during policy\" metrics for a single client, even if
    their percentage is below 60%.

    - Numerator: sum of approvedamount from CLAIMS DATA where datesubmitted is
      within the client's current contract period.
    - Denominator: sum of Amount from DEBIT_NOTE for that client within the
      same contract period (excluding TPA rows).
    - Returns total_claims_cost, total_debit_cost, pct.
    """
    try:
        data_dict = load_mlr_data()
        GROUP_CONTRACT = data_dict["GROUP_CONTRACT"]
        CLAIMS = data_dict["CLAIMS"]
        GROUPS = data_dict["GROUPS"]
        DEBIT = data_dict["DEBIT"]

        # Current contract for this client: contract that is in force today
        today = datetime.now()
        contracts = (
            GROUP_CONTRACT.filter(
                (pl.col("groupname") == company_name)
                & (pl.col("startdate").cast(pl.Datetime) <= today)
                & (pl.col("enddate").cast(pl.Datetime) >= today)
            )
            .select(["groupname", "groupid", "startdate", "enddate"])
            .to_pandas()
        )
        if contracts.empty:
            raise HTTPException(status_code=404, detail=f"No current contract found for '{company_name}'")

        row = contracts.iloc[0]
        gn = row["groupname"]
        start = pd.to_datetime(row["startdate"])
        end = pd.to_datetime(row["enddate"])

        # Claims: datesubmitted in contract for this client
        claims_pl = CLAIMS.with_columns(
            [
                pl.col("approvedamount").cast(pl.Float64),
                pl.col("datesubmitted").cast(pl.Datetime),
                pl.col("nhisgroupid").cast(pl.Utf8),
            ]
        )
        groups_pd = (
            GROUPS.select(["groupid", "groupname"])
            .with_columns(pl.col("groupid").cast(pl.Utf8))
            .to_pandas()
        )
        claims_pd = claims_pl.to_pandas()
        claims_pd = claims_pd.merge(
            groups_pd, left_on="nhisgroupid", right_on="groupid", how="inner"
        )
        mask_claims = (
            (claims_pd["groupname"] == gn)
            & (claims_pd["datesubmitted"] >= start)
            & (claims_pd["datesubmitted"] <= end)
        )
        total_claims = float(claims_pd.loc[mask_claims, "approvedamount"].sum() or 0.0)

        # Debit: contract-period debit for this client (exclude TPA)
        debit_pd = DEBIT.to_pandas()
        debit_pd["From"] = pd.to_datetime(debit_pd["From"])
        debit_pd["CompanyName"] = debit_pd["CompanyName"].astype(str).str.strip()
        debit_pd["Amount"] = pd.to_numeric(debit_pd["Amount"], errors="coerce").fillna(0)
        if "Description" in debit_pd.columns:
            debit_pd = debit_pd[
                ~debit_pd["Description"]
                .astype(str)
                .str.lower()
                .str.contains("tpa", na=False)
            ]
        mask_debit = (
            (debit_pd["CompanyName"] == gn)
            & (debit_pd["From"] >= start)
            & (debit_pd["From"] <= end)
        )
        total_debit = float(debit_pd.loc[mask_debit, "Amount"].sum() or 0.0)

        pct = (total_claims / total_debit * 100) if total_debit > 0 else None

        return {
            "success": True,
            "client_name": gn,
            "total_claims_cost": total_claims,
            "total_debit_cost": total_debit,
            "pct": round(pct, 2) if pct is not None else None,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating claimed paid during policy for '{company_name}': {str(e)}",
        )
