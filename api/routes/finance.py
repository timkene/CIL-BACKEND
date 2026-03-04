"""
Finance Module API Routes
==========================

Provides financial analytics including:
- Payment schedule metrics
- Installment tracking
- Invoice status monitoring
- Financial metrics and MLR analysis
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Dict, Any
import logging
import httpx
from datetime import datetime
from core.database import get_db_connection

logger = logging.getLogger(__name__)
router = APIRouter()


def calculate_finance_metrics_basic(year: int) -> Dict[str, Any]:
  """
  Core finance metrics computation for a given year.

  This is analogous to calculate_mlr_basic / calculate_client_summary_basic and
  is the single place to change when we later move to a nightly batch job that
  writes into a FINANCE_METRICS_SUMMARY table.
  """
  conn = get_db_connection()
  try:
    # Total cash received for the year: use year-specific table if it exists,
    # otherwise fall back to combined table filtered by year.
    cash_received = 0.0
    try:
      cash_query = f'''
                SELECT COALESCE(SUM(amount), 0) as total_cash_received
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_{year}"
            '''
      cash_received = conn.execute(cash_query).fetchone()[0] or 0
    except Exception:
      # Year-specific table may not exist yet; use combined table filtered by year
      cash_query = f'''
                SELECT COALESCE(SUM(amount), 0) as total_cash_received
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
                WHERE EXTRACT(YEAR FROM Date) = {year}
            '''
      try:
        cash_received = conn.execute(cash_query).fetchone()[0] or 0
      except Exception:
        cash_received = 0.0

    # Total PA granted for the year
    pa_granted = conn.execute(
      f'''
            SELECT COALESCE(SUM(granted), 0) as total_pa_granted
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE EXTRACT(YEAR FROM requestdate) = {year}
        '''
    ).fetchone()[0] or 0

    # Total claims approved amount for the year (encounterdatefrom)
    claims_encounter = conn.execute(
      f'''
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_encounter
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE EXTRACT(YEAR FROM encounterdatefrom) = {year}
        '''
    ).fetchone()[0] or 0

    # Total claims approved amount for the year (datesubmitted)
    claims_submitted = conn.execute(
      f'''
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_submitted
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE EXTRACT(YEAR FROM datesubmitted) = {year}
        '''
    ).fetchone()[0] or 0

    return {
      "year": year,
      "cash_received": float(cash_received),
      "pa_granted": float(pa_granted),
      "claims_encounter": float(claims_encounter),
      "claims_submitted": float(claims_submitted),
    }
  finally:
    try:
      conn.close()
    except Exception:
      pass


@router.get("/installment-metrics")
async def get_installment_metrics(
    month: Optional[str] = Query(None, description="Month in YYYY-MM format (e.g., 2025-12)")
):
    """
    Get installment payment schedules and metrics for a specific month.
    Proxies the external invoice API to avoid CORS issues.
    
    Args:
        month: Month in YYYY-MM format. Defaults to current month if not provided.
    
    Returns:
        Installment metrics data with summary statistics
    """
    try:
        # If no month provided, use current month
        if not month:
            month = datetime.now().strftime("%Y-%m")
        
        # Validate month format
        try:
            datetime.strptime(month, "%Y-%m")
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Invalid month format. Use YYYY-MM (e.g., 2025-12)"
            )
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(
                f"https://invoice-backend.clearlinehmo.com/api/v1/coverage-payment-schedule/installment/metrics/public",
                params={"month": month}
            )
            response.raise_for_status()
            return response.json()
            
    except httpx.TimeoutException:
        logger.error(f"Timeout fetching installment metrics for month {month}")
        raise HTTPException(status_code=504, detail="Timeout fetching installment metrics")
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching installment metrics: {e}")
        raise HTTPException(
            status_code=e.response.status_code, 
            detail=f"External API error: {str(e)}"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching installment metrics: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_finance_metrics(
    year: int = Query(datetime.now().year, description="Year to calculate metrics for (e.g., 2025, 2026)")
):
    """
    Get financial metrics for a given year including:
    - Total cash received
    - Total PA granted
    - Total claims (by encounter date)
    - Total claims (by submission date)

    Internally this uses calculate_finance_metrics_basic(), which is the same
    core summary that a future nightly batch job can write into a summary table.
    """
    try:
        data = calculate_finance_metrics_basic(year)
        return {
            "success": True,
            "data": data,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting finance metrics for year {year}: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics-2025")
async def get_finance_metrics_2025_legacy():
    """
    Legacy endpoint kept for backward compatibility.
    Delegates to the generic /metrics endpoint with year=2025.
    """
    return await get_finance_metrics(2025)


@router.get("/mlr-comparison")
async def get_mlr_comparison():
    """
    Get MLR comparison data across years with categories:
    - Claims
    - Salary
    - Commission
    - Welfare
    - Subscription
    - Others
    
    Note: Only includes years where we have both cash received and claims data.
    Currently supports 2025-2026 (2-year window).
    """
    try:
        conn = get_db_connection()
        # Dynamic years: Get current year and previous year (2-year window)
        current_year = datetime.now().year
        years = [current_year - 1, current_year]  # e.g., [2025, 2026]
        chart_data = []
        
        for year in years:
            # Get cash received for this year from the combined CLIENT_CASH_RECEIVED table
            # This avoids hard-coding year-specific tables like CLIENT_CASH_RECEIVED_2026
            cash_query = f'''
                SELECT COALESCE(SUM(amount), 0) AS total_cash
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
                WHERE EXTRACT(YEAR FROM Date) = {year}
            '''
            cash_result = conn.execute(cash_query).fetchone()[0] or 0
            
            # Get claims by encounter date
            claims_encounter_query = f'''
                SELECT COALESCE(SUM(approvedamount), 0) as total_claims
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE EXTRACT(YEAR FROM encounterdatefrom) = {year}
            '''
            claims_encounter = conn.execute(claims_encounter_query).fetchone()[0] or 0
            
            # Get claims by submission date
            claims_submitted_query = f'''
                SELECT COALESCE(SUM(approvedamount), 0) as total_claims
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE EXTRACT(YEAR FROM datesubmitted) = {year}
            '''
            claims_submitted = conn.execute(claims_submitted_query).fetchone()[0] or 0
            
            # Get salary & palliative
            try:
                salary_query = f'SELECT COALESCE(SUM(Amount), 0) as total FROM "AI DRIVEN DATA"."SALARY_AND_PALLIATIVE_{year}"'
                salary_total = conn.execute(salary_query).fetchone()[0] or 0
            except:
                salary_total = 0
            
            # Get expense categories
            try:
                expense_query = f'''
                    SELECT 
                        Category,
                        COALESCE(SUM(Amount), 0) as total_amount
                    FROM "AI DRIVEN DATA"."EXPENSE_AND_COMMISSION_{year}"
                    GROUP BY Category
                '''
                expense_df = conn.execute(expense_query).fetchdf()
                
                # Extract specific categories
                commission = 0
                welfare = 0
                subscription = 0
                others = 0
                
                if not expense_df.empty:
                    for _, row in expense_df.iterrows():
                        category = row['Category']
                        amount = float(row['total_amount'])
                        if category == 'COMMISSION':
                            commission = amount
                        elif category == 'WELFARE':
                            welfare = amount
                        elif category == 'SUBSCRIPTION':
                            subscription = amount
                        else:
                            others += amount
            except:
                commission = 0
                welfare = 0
                subscription = 0
                others = 0
            
            # Calculate MLRs
            cash_result = float(cash_result) if cash_result else 0
            claims_encounter = float(claims_encounter) if claims_encounter else 0
            claims_submitted = float(claims_submitted) if claims_submitted else 0
            salary_total = float(salary_total) if salary_total else 0
            commission = float(commission) if commission else 0
            welfare = float(welfare) if welfare else 0
            subscription = float(subscription) if subscription else 0
            others = float(others) if others else 0
            
            # Calculate MLRs for encounter date
            chart_data.append({
                'Year': year,
                'Category': 'Claims',
                'MLR_Encounter': (claims_encounter / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (claims_submitted / cash_result * 100) if cash_result > 0 else 0
            })
            
            chart_data.append({
                'Year': year,
                'Category': 'Salary',
                'MLR_Encounter': (salary_total / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (salary_total / cash_result * 100) if cash_result > 0 else 0
            })
            
            chart_data.append({
                'Year': year,
                'Category': 'Commission',
                'MLR_Encounter': (commission / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (commission / cash_result * 100) if cash_result > 0 else 0
            })
            
            chart_data.append({
                'Year': year,
                'Category': 'Welfare',
                'MLR_Encounter': (welfare / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (welfare / cash_result * 100) if cash_result > 0 else 0
            })
            
            chart_data.append({
                'Year': year,
                'Category': 'Subscription',
                'MLR_Encounter': (subscription / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (subscription / cash_result * 100) if cash_result > 0 else 0
            })
            
            chart_data.append({
                'Year': year,
                'Category': 'Others',
                'MLR_Encounter': (others / cash_result * 100) if cash_result > 0 else 0,
                'MLR_Submitted': (others / cash_result * 100) if cash_result > 0 else 0
            })
        
        conn.close()
        
        return {
            'success': True,
            'data': chart_data
        }
    except Exception as e:
        logger.error(f"Error getting MLR comparison data: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monthly-cash-received")
async def get_monthly_cash_received(
    month: Optional[str] = Query(None, description="Month in YYYY-MM format (e.g., 2025-12)")
):
    """
    Get monthly cash received by company with New/Existing status and debit note amounts.
    
    Args:
        month: Month in YYYY-MM format. Defaults to current month if not provided.
    
    Returns:
        List of companies with:
        - company_name: Company/group name
        - cash_received: Total cash received for the month
        - status: "New" or "Existing" (based on PA/claims history in last 6 months)
        - debit_note_amount: Total debit note amount for the month
    """
    try:
        # If no month provided, use current month
        if not month:
            month = datetime.now().strftime("%Y-%m")
        
        # Validate month format
        try:
            month_date = datetime.strptime(month, "%Y-%m")
            year = month_date.year
            month_num = month_date.month
        except ValueError:
            raise HTTPException(
                status_code=400, 
                detail="Invalid month format. Use YYYY-MM (e.g., 2025-12)"
            )
        
        conn = get_db_connection()
        
        # Get cash received for the month
        # Try to use year-specific table first, fallback to combined table
        cash_query = f'''
            SELECT 
                groupname,
                COALESCE(SUM(amount), 0) as cash_received
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_{year}"
            WHERE Year = {year} AND Month = {month_num}
            GROUP BY groupname
        '''
        
        try:
            cash_df = conn.execute(cash_query).fetchdf()
        except:
            # Fallback: try combined table or other year tables
            cash_query_fallback = f'''
                SELECT 
                    groupname,
                    COALESCE(SUM(amount), 0) as cash_received
                FROM (
                    SELECT groupname, amount, Year, Month 
                    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2023"
                    WHERE Year = {year} AND Month = {month_num}
                    UNION ALL
                    SELECT groupname, amount, Year, Month 
                    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2024"
                    WHERE Year = {year} AND Month = {month_num}
                    UNION ALL
                    SELECT groupname, amount, Year, Month 
                    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2025"
                    WHERE Year = {year} AND Month = {month_num}
                )
                GROUP BY groupname
            '''
            try:
                cash_df = conn.execute(cash_query_fallback).fetchdf()
            except:
                cash_df = None
        
        if cash_df is None or cash_df.empty:
            return {
                'success': True,
                'data': [],
                'month': month
            }
        
        # Get debit note amounts for the month
        debit_query = f'''
            SELECT 
                groupname,
                COALESCE(SUM(amount), 0) as debit_note_amount
            FROM "AI DRIVEN DATA"."DEBIT_NOTE_ACCRUED"
            WHERE Year = {year} AND Month = {month_num}
            GROUP BY groupname
        '''
        
        try:
            debit_df = conn.execute(debit_query).fetchdf()
        except:
            debit_df = None
        
        # Calculate 6 months ago date for New/Existing check
        from datetime import timedelta
        six_months_ago = month_date - timedelta(days=180)  # Approximately 6 months
        
        # Check PA and Claims history for each company
        companies_data = []
        for _, row in cash_df.iterrows():
            company_name = row['groupname']
            cash_received = float(row['cash_received']) if row['cash_received'] else 0
            
            # Get debit note amount for this company
            debit_amount = 0
            if debit_df is not None and not debit_df.empty:
                company_debit = debit_df[debit_df['groupname'] == company_name]
                if not company_debit.empty:
                    debit_amount = float(company_debit.iloc[0]['debit_note_amount']) if company_debit.iloc[0]['debit_note_amount'] else 0
            
            # Check if company has PA or Claims history in last 6 months
            has_history = False
            
            # Check PA history using groupname directly
            pa_check_query = f'''
                SELECT COUNT(*) as count
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE groupname = ?
                  AND requestdate >= ?
            '''
            try:
                pa_count = conn.execute(pa_check_query, [company_name, six_months_ago]).fetchone()[0]
                if pa_count and pa_count > 0:
                    has_history = True
            except:
                pass
            
            # Check Claims history if no PA history found
            # Claims uses nhisgroupid, so we need to join with GROUPS to get groupname
            if not has_history:
                claims_check_query = f'''
                    SELECT COUNT(*) as count
                    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                    JOIN "AI DRIVEN DATA"."GROUPS" g ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                    WHERE g.groupname = ?
                      AND (c.encounterdatefrom >= ? OR c.datesubmitted >= ?)
                '''
                try:
                    claims_count = conn.execute(claims_check_query, [company_name, six_months_ago, six_months_ago]).fetchone()[0]
                    if claims_count and claims_count > 0:
                        has_history = True
                except:
                    pass
            
            companies_data.append({
                'company_name': company_name,
                'cash_received': cash_received,
                'status': 'Existing' if has_history else 'New',
                'debit_note_amount': debit_amount
            })
        
        # Sort by cash received descending
        companies_data.sort(key=lambda x: x['cash_received'], reverse=True)
        
        conn.close()
        
        return {
            'success': True,
            'data': companies_data,
            'month': month
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting monthly cash received: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

