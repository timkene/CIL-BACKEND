"""
Utilization Analysis Endpoints
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime, date, timedelta
import polars as pl
import pandas as pd
import numpy as np
from core.database import get_db_connection

router = APIRouter()

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

def load_utilization_data():
    """Load data needed for utilization analysis"""
    try:
        conn = get_db_connection()

        GROUPS = conn.execute('SELECT * FROM "AI DRIVEN DATA"."GROUPS"').fetchdf()

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

        MEMBERS = conn.execute(
            'SELECT memberid, groupid, enrollee_id as legacycode, planid, iscurrent, isterminated FROM "AI DRIVEN DATA"."MEMBERS"'
        ).fetchdf()

        PROVIDER = conn.execute('SELECT * FROM "AI DRIVEN DATA"."PROVIDERS"').fetchdf()

        conn.close()

        return {
            'GROUPS': pl.from_pandas(GROUPS),
            'CLAIMS': pl.from_pandas(CLAIMS),
            'MEMBERS': pl.from_pandas(MEMBERS),
            'PROVIDER': pl.from_pandas(PROVIDER)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading utilization data: {str(e)}")


@router.get("/groups")
async def get_utilization_groups():
    """
    Get utilization statistics by group
    """
    try:
        data_dict = load_utilization_data()
        CLAIMS = data_dict['CLAIMS']
        GROUPS = data_dict['GROUPS']
        MEMBERS = data_dict['MEMBERS']

        # Prepare data
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('nhisgroupid').cast(pl.Utf8)
        ])

        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

        # Join claims with groups
        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid',
            right_on='groupid',
            how='inner'
        )

        # Calculate utilization metrics
        utilization = claims_with_group.group_by('groupname').agg([
            pl.col('approvedamount').sum().alias('total_claims'),
            pl.col('nhislegacynumber').n_unique().alias('unique_members'),
            pl.col('panumber').n_unique().alias('total_visits'),
            pl.col('approvedamount').mean().alias('avg_claim_amount')
        ])

        # Get total members per group
        members_per_group = MEMBERS.join(
            GROUPS.select(['groupid', 'groupname']).with_columns(pl.col('groupid').cast(pl.Int64)),
            on='groupid',
            how='inner'
        ).filter(
            (pl.col('iscurrent') == 1) & (pl.col('isterminated') == 0)
        ).group_by('groupname').agg(
            pl.col('memberid').n_unique().alias('total_members')
        )

        # Join and calculate utilization rate
        result = utilization.join(
            members_per_group,
            on='groupname',
            how='left'
        ).with_columns([
            pl.when(
                (pl.col('total_members').is_not_null()) &
                (pl.col('total_members') > 0)
            )
            .then((pl.col('unique_members') / pl.col('total_members') * 100).round(2))
            .otherwise(pl.lit(None))
            .alias('utilization_rate')
        ])

        # Convert to pandas for JSON
        result_pandas = result.to_pandas()
        result_pandas = clean_for_json(result_pandas)

        return {
            "timestamp": datetime.now().isoformat(),
            "total_groups": len(result_pandas),
            "data": result_pandas.to_dict(orient='records')
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/providers")
async def get_top_providers(limit: int = Query(10, ge=1, le=100)):
    """
    Get top providers by total claims
    """
    try:
        data_dict = load_utilization_data()
        CLAIMS = data_dict['CLAIMS']
        PROVIDER = data_dict['PROVIDER']

        # Prepare data
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64)
        ])

        # Join with providers
        claims_with_provider = CLAIMS.join(
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

        # Aggregate by provider
        top_providers = claims_with_provider.group_by('providername').agg([
            pl.col('approvedamount').sum().alias('total_claims'),
            pl.col('nhislegacynumber').n_unique().alias('unique_patients'),
            pl.col('panumber').n_unique().alias('total_visits'),
            pl.col('approvedamount').mean().alias('avg_claim_amount')
        ]).sort('total_claims', descending=True).head(limit)

        # Convert to pandas
        result_pandas = top_providers.to_pandas()
        result_pandas = clean_for_json(result_pandas)

        return {
            "timestamp": datetime.now().isoformat(),
            "limit": limit,
            "data": result_pandas.to_dict(orient='records')
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def get_utilization_trends(
    months: int = Query(12, ge=1, le=24, description="Number of months to analyze")
):
    """
    Get monthly utilization trends
    """
    try:
        data_dict = load_utilization_data()
        CLAIMS = data_dict['CLAIMS']

        # Prepare data
        CLAIMS = CLAIMS.with_columns([
            pl.col('approvedamount').cast(pl.Float64),
            pl.col('datesubmitted').cast(pl.Datetime)
        ])

        # Filter for specified months
        cutoff_date = datetime.now() - timedelta(days=months * 30)
        CLAIMS = CLAIMS.filter(pl.col('datesubmitted') >= cutoff_date)

        # Extract month and aggregate
        monthly_trends = CLAIMS.with_columns([
            pl.col('datesubmitted').dt.strftime('%Y-%m').alias('month')
        ]).group_by('month').agg([
            pl.col('approvedamount').sum().alias('total_claims'),
            pl.col('nhislegacynumber').n_unique().alias('unique_members'),
            pl.col('panumber').n_unique().alias('total_visits'),
            pl.col('approvedamount').mean().alias('avg_claim_amount')
        ]).sort('month')

        # Convert to pandas
        result_pandas = monthly_trends.to_pandas()
        result_pandas = clean_for_json(result_pandas)

        return {
            "timestamp": datetime.now().isoformat(),
            "months_analyzed": months,
            "data": result_pandas.to_dict(orient='records')
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
