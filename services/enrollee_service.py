"""
Enrollee Analytics Service
===========================

Comprehensive enrollee analysis including:
- Top enrollees by cost
- Top enrollees by visits
- Benefit limit violations
- Enrollment statistics
- Data quality metrics
"""

import polars as pl
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class EnrolleeAnalyticsService:
    """Service for enrollee analytics and monitoring"""

    @staticmethod
    def get_top_enrollees_by_cost(
        CLAIMS: pl.DataFrame,
        PA: pl.DataFrame,
        MEMBERS: pl.DataFrame,
        GROUPS: pl.DataFrame,
        limit: int = 50,
        groupname: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get top enrollees by total cost (Claims + Unclaimed PA)

        Args:
            CLAIMS: Claims DataFrame
            PA: PA DataFrame
            MEMBERS: Members DataFrame
            GROUPS: Groups DataFrame
            limit: Number of top enrollees to return
            groupname: Filter by company (optional)
            start_date: Filter start date (optional)
            end_date: Filter end date (optional)

        Returns:
            Dictionary with top enrollees and statistics
        """
        try:
            # Prepare data (claims by encounterdatefrom, PA by requestdate)
            CLAIMS = CLAIMS.with_columns([
                pl.col('approvedamount').cast(pl.Float64),
                pl.col('encounterdatefrom').cast(pl.Datetime),
                pl.col('nhisgroupid').cast(pl.Utf8)
            ])

            PA = PA.with_columns([
                pl.col('granted').cast(pl.Float64, strict=False),
                pl.col('requestdate').cast(pl.Datetime),
                pl.col('panumber').cast(pl.Utf8, strict=False)
            ])

            GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

            # Filter by date if provided (claims by encounter date, PA by request date)
            if start_date:
                CLAIMS = CLAIMS.filter(pl.col('encounterdatefrom') >= pd.Timestamp(start_date))
                PA = PA.filter(pl.col('requestdate') >= pd.Timestamp(start_date))

            if end_date:
                CLAIMS = CLAIMS.filter(pl.col('encounterdatefrom') <= pd.Timestamp(end_date))
                PA = PA.filter(pl.col('requestdate') <= pd.Timestamp(end_date))

            # Join claims with groups
            claims_with_group = CLAIMS.join(
                GROUPS.select(['groupid', 'groupname']),
                left_on='nhisgroupid',
                right_on='groupid',
                how='inner'
            )

            # Filter by groupname if provided
            if groupname:
                claims_with_group = claims_with_group.filter(pl.col('groupname') == groupname)
                PA = PA.filter(pl.col('groupname') == groupname)

            # Calculate claims cost per enrollee
            claims_cost = claims_with_group.group_by('nhislegacynumber').agg([
                pl.col('approvedamount').sum().alias('claims_cost'),
                pl.col('panumber').n_unique().alias('visit_count'),
                pl.col('groupname').first().alias('groupname')
            ])

            # Get claimed PA numbers
            # PA numbers in claims are FLOAT64 (e.g., 410667.0), PA numbers in PA table are STRING (e.g., "410667")
            # Convert claims panumber to string, removing .0 suffix for proper matching
            claimed_pa_numbers = claims_with_group.filter(
                pl.col('panumber').is_not_null()
            ).with_columns([
                # Convert to int first (handles float), then to string to remove .0
                pl.col('panumber').cast(pl.Int64, strict=False).cast(pl.Utf8).alias('panumber_str')
            ]).filter(
                (pl.col('panumber_str').is_not_null()) &
                (pl.col('panumber_str') != '') &
                (pl.col('panumber_str') != '0')
            ).select([
                'panumber_str',
                'nhislegacynumber'
            ]).unique()

            # Calculate unclaimed PA per enrollee
            # Normalize PA panumber same as claims (float->int->string) so "12345.0" and 12345.0 match
            unclaimed_pa = PA.filter(
                pl.col('panumber').is_not_null() &
                (pl.col('panumber') != '') &
                (pl.col('panumber') != '0')
            ).with_columns([
                pl.col('panumber').cast(pl.Utf8, strict=False).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).cast(pl.Utf8).alias('panumber_str')
            ]).filter(
                (pl.col('panumber_str').is_not_null()) & (pl.col('panumber_str') != '') & (pl.col('panumber_str') != '0')
            ).join(
                claimed_pa_numbers.select('panumber_str'),
                on='panumber_str',
                how='anti'
            ).group_by('IID').agg([
                pl.col('granted').sum().alias('unclaimed_pa_cost')
            ]).rename({'IID': 'nhislegacynumber'})

            # Combine claims and unclaimed PA
            enrollee_costs = claims_cost.join(
                unclaimed_pa,
                on='nhislegacynumber',
                how='outer'
            ).with_columns([
                pl.col('claims_cost').fill_null(0).cast(pl.Float64),
                pl.col('unclaimed_pa_cost').fill_null(0).cast(pl.Float64),
                pl.col('visit_count').fill_null(0).cast(pl.Int64),
                pl.col('groupname').fill_null('Unknown')
            ]).with_columns([
                (pl.col('claims_cost') + pl.col('unclaimed_pa_cost')).alias('total_cost')
            ])

            # Join with member info
            MEMBERS_info = MEMBERS.select([
                'enrollee_id',
                'firstname',
                'surname',
                'dateofbirth',
                'email',
                'phone'
            ]).rename({'enrollee_id': 'nhislegacynumber'})

            enrollee_costs_with_info = enrollee_costs.join(
                MEMBERS_info,
                on='nhislegacynumber',
                how='left'
            )

            # Get top enrollees
            top_enrollees = enrollee_costs_with_info.sort(
                'total_cost',
                descending=True
            ).head(limit)

            # Calculate statistics
            total_enrollees = enrollee_costs.height
            total_cost = enrollee_costs.select(pl.col('total_cost').sum()).item() or 0
            avg_cost = enrollee_costs.select(pl.col('total_cost').mean()).item() or 0

            return {
                'success': True,
                'top_enrollees': top_enrollees.to_pandas().to_dict('records'),
                'statistics': {
                    'total_enrollees': total_enrollees,
                    'total_cost': total_cost,
                    'average_cost': avg_cost,
                    'limit': limit
                },
                'filters': {
                    'groupname': groupname,
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            }

        except Exception as e:
            logger.error(f"Error getting top enrollees by cost: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def get_top_enrollees_by_visits(
        CLAIMS: pl.DataFrame,
        MEMBERS: pl.DataFrame,
        GROUPS: pl.DataFrame,
        limit: int = 50,
        groupname: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get top enrollees by visit count

        Args:
            CLAIMS: Claims DataFrame
            MEMBERS: Members DataFrame
            GROUPS: Groups DataFrame
            limit: Number of top enrollees to return
            groupname: Filter by company (optional)
            start_date: Filter start date (optional)
            end_date: Filter end date (optional)

        Returns:
            Dictionary with top enrollees by visits
        """
        try:
            # Prepare data (claims by encounterdatefrom)
            CLAIMS = CLAIMS.with_columns([
                pl.col('approvedamount').cast(pl.Float64),
                pl.col('encounterdatefrom').cast(pl.Datetime),
                pl.col('nhisgroupid').cast(pl.Utf8)
            ])

            GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))

            # Filter by date if provided
            if start_date:
                CLAIMS = CLAIMS.filter(pl.col('encounterdatefrom') >= pd.Timestamp(start_date))

            if end_date:
                CLAIMS = CLAIMS.filter(pl.col('encounterdatefrom') <= pd.Timestamp(end_date))

            # Join claims with groups
            claims_with_group = CLAIMS.join(
                GROUPS.select(['groupid', 'groupname']),
                left_on='nhisgroupid',
                right_on='groupid',
                how='inner'
            )

            # Filter by groupname if provided
            if groupname:
                claims_with_group = claims_with_group.filter(pl.col('groupname') == groupname)

            # Calculate visits per enrollee
            enrollee_visits = claims_with_group.group_by('nhislegacynumber').agg([
                pl.col('panumber').n_unique().alias('visit_count'),
                pl.col('approvedamount').sum().alias('total_cost'),
                pl.col('approvedamount').count().alias('claim_count'),
                pl.col('groupname').first().alias('groupname')
            ]).with_columns([
                pl.col('total_cost').cast(pl.Float64),
                pl.col('visit_count').cast(pl.Int64),
                pl.col('claim_count').cast(pl.Int64),
                (pl.col('total_cost') / pl.col('visit_count').cast(pl.Float64)).alias('avg_cost_per_visit')
            ])

            # Join with member info
            MEMBERS_info = MEMBERS.select([
                'enrollee_id',
                'firstname',
                'surname',
                'dateofbirth',
                'email',
                'phone'
            ]).rename({'enrollee_id': 'nhislegacynumber'})

            enrollee_visits_with_info = enrollee_visits.join(
                MEMBERS_info,
                on='nhislegacynumber',
                how='left'
            )

            # Get top enrollees
            top_enrollees = enrollee_visits_with_info.sort(
                'visit_count',
                descending=True
            ).head(limit)

            # Calculate statistics
            total_enrollees = enrollee_visits.height
            total_visits = enrollee_visits.select(pl.col('visit_count').sum()).item() or 0
            avg_visits = enrollee_visits.select(pl.col('visit_count').mean()).item() or 0

            return {
                'success': True,
                'top_enrollees': top_enrollees.to_pandas().to_dict('records'),
                'statistics': {
                    'total_enrollees': total_enrollees,
                    'total_visits': total_visits,
                    'average_visits': avg_visits,
                    'limit': limit
                },
                'filters': {
                    'groupname': groupname,
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            }

        except Exception as e:
            logger.error(f"Error getting top enrollees by visits: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def get_top_clients_by_total_cost(
        CLAIMS: pl.DataFrame,
        PA: pl.DataFrame,
        GROUPS: pl.DataFrame,
        GROUP_CONTRACT: Optional[pl.DataFrame] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Get top clients (companies) by total medical cost **within their current contract period**.

        For each client, only claims and PA within that client's current contract
        (from GROUP_CONTRACT where iscurrent = 1) are counted. PA already linked
        to claims via panumber is excluded to avoid double counting.
        """
        try:
            has_contracts = GROUP_CONTRACT is not None and GROUP_CONTRACT.height > 0

            CLAIMS = CLAIMS.with_columns(
                [
                    pl.col("approvedamount").cast(pl.Float64),
                    pl.col("encounterdatefrom").cast(pl.Datetime),
                    pl.col("nhisgroupid").cast(pl.Utf8),
                ]
            )

            PA = PA.with_columns(
                [
                    pl.col("granted").cast(pl.Float64, strict=False),
                    pl.col("requestdate").cast(pl.Datetime),
                    pl.col("panumber").cast(pl.Utf8, strict=False),
                    pl.col("groupname").cast(pl.Utf8, strict=False),
                ]
            )

            GROUPS = GROUPS.with_columns(pl.col("groupid").cast(pl.Utf8))

            # Base mapping: groupid + groupname, optionally with contract dates
            if has_contracts:
                gc = GROUP_CONTRACT.with_columns(
                    [
                        pl.col("groupid").cast(pl.Utf8),
                        pl.col("startdate").cast(pl.Datetime).alias("contract_start"),
                        pl.col("enddate").cast(pl.Datetime).alias("contract_end"),
                        pl.col("iscurrent").cast(pl.Int64),
                    ]
                )
                # Only current contracts
                gc_current = gc.filter(pl.col("iscurrent") == 1)

                groups_with_contract = GROUPS.join(
                    gc_current.select(["groupid", "contract_start", "contract_end"]),
                    on="groupid",
                    how="inner",
                )
            else:
                # No contract info: fall back to groups only
                groups_with_contract = GROUPS.with_columns(
                    [
                        pl.lit(None).cast(pl.Datetime).alias("contract_start"),
                        pl.lit(None).cast(pl.Datetime).alias("contract_end"),
                    ]
                )

            # Claims joined with group + contract
            claims_with_group = CLAIMS.join(
                groups_with_contract,
                left_on="nhisgroupid",
                right_on="groupid",
                how="inner",
            )

            if has_contracts:
                # Filter claims by encounterdatefrom within each client's current contract period
                claims_with_group = claims_with_group.filter(
                    (pl.col("encounterdatefrom") >= pl.col("contract_start"))
                    & (pl.col("encounterdatefrom") <= pl.col("contract_end"))
                )

            # PA joined with group + contract via groupname
            pa_with_group = PA.join(
                groups_with_contract.select(["groupid", "groupname", "contract_start", "contract_end"]),
                on="groupname",
                how="inner",
            )

            if has_contracts:
                pa_with_group = pa_with_group.filter(
                    (pl.col("requestdate") >= pl.col("contract_start"))
                    & (pl.col("requestdate") <= pl.col("contract_end"))
                )

            # Claims cost per client
            claims_by_client = (
                claims_with_group.group_by("groupname")
                .agg(
                    [
                        pl.col("approvedamount")
                        .sum()
                        .alias("claims_cost"),
                        pl.col("panumber")
                        .n_unique()
                        .alias("visit_count"),
                        pl.col("nhislegacynumber")
                        .n_unique()
                        .alias("unique_enrollees"),
                    ]
                )
                .with_columns(
                    [
                        pl.col("claims_cost").cast(pl.Float64),
                        pl.col("visit_count").cast(pl.Int64),
                        pl.col("unique_enrollees").cast(pl.Int64),
                    ]
                )
            )

            # Determine PA numbers already claimed (same logic as enrollee-level)
            claimed_pa_numbers = (
                claims_with_group.filter(pl.col("panumber").is_not_null())
                .with_columns(
                    pl.col("panumber")
                    .cast(pl.Int64, strict=False)
                    .cast(pl.Utf8)
                    .alias("panumber_str")
                )
                .filter(
                    (pl.col("panumber_str").is_not_null())
                    & (pl.col("panumber_str") != "")
                    & (pl.col("panumber_str") != "0")
                )
                .select(["panumber_str"])
                .unique()
            )

            # Unclaimed PA per client (exclude PAs already linked to claims)
            # Normalize PA panumber same as claims so "12345.0" and 12345.0 match
            unclaimed_pa_by_client = (
                pa_with_group.filter(
                    pl.col("panumber").is_not_null()
                    & (pl.col("panumber") != "")
                    & (pl.col("panumber") != "0")
                )
                .with_columns(
                    pl.col("panumber")
                    .cast(pl.Utf8, strict=False)
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .cast(pl.Int64, strict=False)
                    .cast(pl.Utf8)
                    .alias("panumber_str")
                )
                .filter(
                    (pl.col("panumber_str").is_not_null())
                    & (pl.col("panumber_str") != "")
                    & (pl.col("panumber_str") != "0")
                )
                .join(
                    claimed_pa_numbers.select("panumber_str"),
                    on="panumber_str",
                    how="anti",
                )
                .group_by("groupname")
                .agg(pl.col("granted").sum().alias("unclaimed_pa_cost"))
            )

            # Combine claims and unclaimed PA at client level
            client_costs = claims_by_client.join(
                unclaimed_pa_by_client,
                on="groupname",
                how="outer",
            ).with_columns(
                [
                    pl.col("claims_cost").fill_null(0).cast(pl.Float64),
                    pl.col("unclaimed_pa_cost")
                    .fill_null(0)
                    .cast(pl.Float64),
                    pl.col("visit_count").fill_null(0).cast(pl.Int64),
                    pl.col("unique_enrollees").fill_null(0).cast(pl.Int64),
                ]
            )

            client_costs = client_costs.with_columns(
                (pl.col("claims_cost") + pl.col("unclaimed_pa_cost")).alias(
                    "total_cost"
                )
            )

            # Top clients by total cost
            top_clients = client_costs.sort(
                "total_cost", descending=True
            ).head(limit)

            total_clients = client_costs.height
            total_cost = client_costs.select(pl.col("total_cost").sum()).item() or 0.0

            return {
                "success": True,
                "clients": top_clients.to_pandas().to_dict("records"),
                "statistics": {
                    "total_clients": total_clients,
                    "total_cost": total_cost,
                    "limit": limit,
                },
            }

        except Exception as e:
            logger.error(f"Error getting top clients by total cost: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @staticmethod
    def get_top_clients_by_active_members(
        MEMBERS: pl.DataFrame,
        GROUPS: pl.DataFrame,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """
        Get top clients by number of active members.
        """
        try:
            # Ensure groupid is available on members and has compatible type
            members = MEMBERS.with_columns(
                [
                    pl.col("groupid").cast(pl.Utf8)
                ]
            )

            # Active members only
            active_members = members.filter(pl.col("iscurrent") == True)

            GROUPS_prepared = GROUPS.with_columns(pl.col("groupid").cast(pl.Utf8))

            # Join with groups to get groupname
            active_with_group = active_members.join(
                GROUPS_prepared.select(["groupid", "groupname"]),
                left_on="groupid",
                right_on="groupid",
                how="left",
            )

            # Aggregate by client
            client_members = (
                active_with_group.group_by("groupid", "groupname")
                .agg(pl.count().alias("active_members"))
                .with_columns(
                    [
                        pl.col("active_members").cast(pl.Int64),
                    ]
                )
            )

            top_clients = client_members.sort(
                "active_members", descending=True
            ).head(limit)

            total_clients = client_members.height
            total_active_members = (
                client_members.select(pl.col("active_members").sum()).item() or 0
            )

            return {
                "success": True,
                "clients": top_clients.to_pandas().to_dict("records"),
                "statistics": {
                    "total_clients": total_clients,
                    "total_active_members": total_active_members,
                    "limit": limit,
                },
            }

        except Exception as e:
            logger.error(f"Error getting top clients by active members: {e}")
            return {
                "success": False,
                "error": str(e),
            }

    @staticmethod
    def get_benefit_limit_violations(
        CLAIMS: pl.DataFrame,
        PA: pl.DataFrame,
        MEMBERS: pl.DataFrame,
        MEMBER_PLANS: pl.DataFrame,
        GROUP_PLANS: pl.DataFrame,
        BENEFIT: pl.DataFrame,
        limit: int = 20,
        groupname: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get enrollees who have met or exceeded their benefit limits

        Returns violations grouped by benefit type

        Args:
            CLAIMS: Claims DataFrame
            PA: PA DataFrame
            MEMBERS: Members DataFrame
            MEMBER_PLANS: Member plans DataFrame
            GROUP_PLANS: Group plans DataFrame
            BENEFIT: Benefit codes DataFrame
            limit: Number of top violators per benefit
            groupname: Filter by company (optional)

        Returns:
            Dictionary with violations per benefit
        """
        try:
            # This is a simplified version - you'll need to adjust based on your
            # actual benefit limit structure in the database

            # Get benefit limits from GROUP_PLANS
            # Assuming GROUP_PLANS has benefit limits per plan

            # Join claims with benefits
            CLAIMS = CLAIMS.with_columns([
                pl.col('approvedamount').cast(pl.Float64),
                pl.col('code').cast(pl.Utf8)
            ])

            # This requires understanding your benefit limit structure
            # For now, return a structure showing the approach

            violations_by_benefit = []

            # Example structure - you'll need to customize based on actual schema
            # For each benefit code, calculate usage vs limit

            return {
                'success': True,
                'violations_by_benefit': violations_by_benefit,
                'message': 'Benefit limit checking requires specific limit configuration'
            }

        except Exception as e:
            logger.error(f"Error getting benefit violations: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def get_enrollment_statistics(
        MEMBERS: pl.DataFrame,
        reference_month: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get enrollment statistics for a specific month

        Args:
            MEMBERS: Members DataFrame
            reference_month: Month to analyze (defaults to current month)

        Returns:
            Dictionary with enrollment statistics
        """
        try:
            if reference_month is None:
                reference_month = datetime.now()

            # Get start and end of reference month
            month_start = reference_month.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if reference_month.month == 12:
                month_end = month_start.replace(year=month_start.year + 1, month=1)
            else:
                month_end = month_start.replace(month=month_start.month + 1)

            # Convert to timestamps
            month_start_ts = pd.Timestamp(month_start)
            month_end_ts = pd.Timestamp(month_end)

            # Prepare data
            MEMBERS = MEMBERS.with_columns([
                pl.col('effectivedate').cast(pl.Datetime),
                pl.col('terminationdate').cast(pl.Datetime, strict=False)
            ])

            # Count enrollees added this month
            added_this_month = MEMBERS.filter(
                (pl.col('effectivedate') >= month_start_ts) &
                (pl.col('effectivedate') < month_end_ts)
            ).height

            # Count enrollees terminated this month
            terminated_this_month = MEMBERS.filter(
                (pl.col('terminationdate').is_not_null()) &
                (pl.col('terminationdate') >= month_start_ts) &
                (pl.col('terminationdate') < month_end_ts)
            ).height

            # Count currently active enrollees
            active_enrollees = MEMBERS.filter(
                (pl.col('iscurrent') == True) |
                (
                    (pl.col('effectivedate') <= month_end_ts) &
                    (
                        (pl.col('terminationdate').is_null()) |
                        (pl.col('terminationdate') > month_end_ts)
                    )
                )
            ).height

            return {
                'success': True,
                'statistics': {
                    'added_this_month': added_this_month,
                    'terminated_this_month': terminated_this_month,
                    'net_change': added_this_month - terminated_this_month,
                    'active_enrollees': active_enrollees,
                    'reference_month': reference_month.strftime('%Y-%m')
                }
            }

        except Exception as e:
            logger.error(f"Error getting enrollment statistics: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def get_gender_count(
        MEMBERS: pl.DataFrame
    ) -> Dict[str, Any]:
        """
        Get gender count for active enrollees

        Args:
            MEMBERS: Members DataFrame

        Returns:
            Dictionary with gender count statistics
        """
        try:
            # Filter to active enrollees only
            active_members = MEMBERS.filter(pl.col('iscurrent') == True)
            
            # Check if genderid column exists
            if 'genderid' not in active_members.columns:
                logger.warning("genderid column not found in MEMBERS DataFrame")
                return {
                    'success': True,
                    'gender_count': {
                        'male': 0,
                        'female': 0,
                        'other': 0,
                        'unknown': 0,
                        'total': 0
                    }
                }
            
            # Count by genderid (handle null values)
            gender_counts = active_members.filter(
                pl.col('genderid').is_not_null()
            ).group_by('genderid').agg([
                pl.count().alias('count')
            ])
            
            # Convert to dictionary
            gender_dict = {}
            total = 0
            
            # Handle null/unknown genderids separately
            null_count = active_members.filter(pl.col('genderid').is_null()).height
            
            for row in gender_counts.iter_rows(named=True):
                gender_id = row['genderid']
                count = row['count']
                total += count
                
                # Handle different data types (int, float, string)
                gender_id_int = int(gender_id) if gender_id is not None else None
                
                if gender_id_int == 1:
                    gender_dict['male'] = count
                elif gender_id_int == 2:
                    gender_dict['female'] = count
                elif gender_id_int == 3:
                    gender_dict['other'] = count
                else:
                    gender_dict['unknown'] = gender_dict.get('unknown', 0) + count
            
            # Add null counts to unknown
            if null_count > 0:
                gender_dict['unknown'] = gender_dict.get('unknown', 0) + null_count
                total += null_count
            
            # Ensure all keys exist
            gender_dict['male'] = gender_dict.get('male', 0)
            gender_dict['female'] = gender_dict.get('female', 0)
            gender_dict['other'] = gender_dict.get('other', 0)
            gender_dict['unknown'] = gender_dict.get('unknown', 0)
            gender_dict['total'] = total
            
            return {
                'success': True,
                'gender_count': gender_dict
            }
            
        except Exception as e:
            logger.error(f"Error getting gender count: {e}")
            return {
                'success': False,
                'error': str(e)
            }

    @staticmethod
    def get_data_quality_metrics(
        MEMBERS: pl.DataFrame
    ) -> Dict[str, Any]:
        """
        Get data quality metrics for active enrollees

        Counts enrollees missing critical information:
        - Date of birth
        - Phone number
        - Email
        - Address

        Args:
            MEMBERS: Members DataFrame

        Returns:
            Dictionary with data quality metrics
        """
        try:
            # Filter to active enrollees only
            active_members = MEMBERS.filter(pl.col('iscurrent') == True)

            total_active = active_members.height

            # Count missing data
            missing_dob = active_members.filter(
                pl.col('dateofbirth').is_null()
            ).height

            missing_phone = active_members.filter(
                (pl.col('phone').is_null()) |
                (pl.col('phone') == '') |
                (pl.col('phone').str.len_chars() < 10)
            ).height

            missing_email = active_members.filter(
                (pl.col('email').is_null()) |
                (pl.col('email') == '') |
                (~pl.col('email').str.contains('@'))
            ).height

            # Check for address field - adjust column name as needed
            address_col = None
            for col in ['address', 'contactaddress', 'residentialaddress']:
                if col in active_members.columns:
                    address_col = col
                    break

            if address_col:
                missing_address = active_members.filter(
                    (pl.col(address_col).is_null()) |
                    (pl.col(address_col) == '')
                ).height
            else:
                missing_address = 0

            # Calculate percentages
            missing_dob_pct = (missing_dob / total_active * 100) if total_active > 0 else 0
            missing_phone_pct = (missing_phone / total_active * 100) if total_active > 0 else 0
            missing_email_pct = (missing_email / total_active * 100) if total_active > 0 else 0
            missing_address_pct = (missing_address / total_active * 100) if total_active > 0 else 0

            # Count enrollees with all data complete
            if address_col:
                complete_data = active_members.filter(
                    (pl.col('dateofbirth').is_not_null()) &
                    (pl.col('phone').is_not_null()) &
                    (pl.col('phone') != '') &
                    (pl.col('email').is_not_null()) &
                    (pl.col('email').str.contains('@')) &
                    (pl.col(address_col).is_not_null()) &
                    (pl.col(address_col) != '')
                ).height
            else:
                complete_data = active_members.filter(
                    (pl.col('dateofbirth').is_not_null()) &
                    (pl.col('phone').is_not_null()) &
                    (pl.col('phone') != '') &
                    (pl.col('email').is_not_null()) &
                    (pl.col('email').str.contains('@'))
                ).height

            data_completeness_pct = (complete_data / total_active * 100) if total_active > 0 else 0

            return {
                'success': True,
                'total_active_enrollees': total_active,
                'missing_data': {
                    'date_of_birth': {
                        'count': missing_dob,
                        'percentage': round(missing_dob_pct, 2)
                    },
                    'phone_number': {
                        'count': missing_phone,
                        'percentage': round(missing_phone_pct, 2)
                    },
                    'email': {
                        'count': missing_email,
                        'percentage': round(missing_email_pct, 2)
                    },
                    'address': {
                        'count': missing_address,
                        'percentage': round(missing_address_pct, 2)
                    }
                },
                'data_quality': {
                    'complete_profiles': complete_data,
                    'completeness_percentage': round(data_completeness_pct, 2)
                }
            }

        except Exception as e:
            logger.error(f"Error getting data quality metrics: {e}")
            return {
                'success': False,
                'error': str(e)
            }


    @staticmethod
    def get_enrollee_profile(
        enrollee_id: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        conn=None
    ) -> Dict[str, Any]:
        """
        Get comprehensive profile for a specific enrollee
        
        Args:
            enrollee_id: Enrollee ID to search for
            start_date: Start date for period (optional)
            end_date: End date for period (optional)
            conn: Database connection (optional, will create if not provided)
            
        Returns:
            Dictionary with comprehensive enrollee profile data
        """
        try:
            import duckdb
            from core.database import get_db_connection
            
            if conn is None:
                conn = get_db_connection()
            
            # Convert dates to strings for SQL
            start_date_str = start_date.strftime('%Y-%m-%d') if start_date else None
            end_date_str = end_date.strftime('%Y-%m-%d') if end_date else None
            
            # 1. Get enrollee basic info and current contract period
            member_query = f"""
            WITH enrollee_info AS (
                SELECT 
                    m.enrollee_id,
                    m.memberid,
                    m.groupid,
                    COALESCE(mem.firstname, '') as firstname,
                    COALESCE(mem.lastname, '') as surname,
                    COALESCE(mem.dob, m.dob) as dateofbirth,
                    COALESCE(mem.phone1, m.phone1) as phone,
                    COALESCE(mem.email1, m.email1) as email,
                    COALESCE(mem.address1, m.address1) as address,
                    m.effectivedate,
                    m.terminationdate,
                    m.iscurrent,
                    g.groupname as company_name
                FROM "AI DRIVEN DATA"."MEMBERS" m
                LEFT JOIN "AI DRIVEN DATA"."MEMBER" mem ON CAST(m.memberid AS BIGINT) = mem.memberid
                LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON m.groupid = g.groupid
                WHERE m.enrollee_id = '{enrollee_id}'
                LIMIT 1
            ),
            current_contract AS (
                SELECT 
                    gc.startdate as contract_start,
                    gc.enddate as contract_end
                FROM enrollee_info ei
                INNER JOIN "AI DRIVEN DATA"."GROUP_CONTRACT" gc ON ei.groupid = gc.groupid
                WHERE gc.iscurrent = 1
                LIMIT 1
            )
            SELECT 
                ei.*,
                cc.contract_start,
                cc.contract_end
            FROM enrollee_info ei
            LEFT JOIN current_contract cc ON 1=1
            """
            
            member_df = conn.execute(member_query).fetchdf()
            if member_df.empty:
                return {'success': False, 'error': f'Enrollee {enrollee_id} not found'}
            
            member_info = member_df.iloc[0].to_dict()
            
            # Get contract period for benefit limits
            contract_start = member_info.get('contract_start')
            contract_end = member_info.get('contract_end')
            
            # Initialize contract dates as None if not found
            if contract_start and pd.notna(contract_start):
                try:
                    contract_start = pd.to_datetime(contract_start)
                except:
                    contract_start = None
            else:
                contract_start = None
                
            if contract_end and pd.notna(contract_end):
                try:
                    contract_end = pd.to_datetime(contract_end)
                except:
                    contract_end = None
            else:
                contract_end = None
            
            # Debug logging for date usage (after contract dates are initialized)
            contract_start_str = contract_start.strftime('%Y-%m-%d') if contract_start and pd.notna(contract_start) else None
            contract_end_str = contract_end.strftime('%Y-%m-%d') if contract_end and pd.notna(contract_end) else None
            logger.info(f"Enrollee {enrollee_id} - Provided dates: start={start_date_str}, end={end_date_str}, Contract dates: start={contract_start_str}, end={contract_end_str}")
            
            # Calculate age
            from datetime import date
            if member_info.get('dateofbirth'):
                dob = pd.to_datetime(member_info['dateofbirth']).date()
                today = date.today()
                age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
            else:
                age = None
            
            # Effective period: use provided dates or enrollee's client current contract
            eff_start = start_date_str or contract_start_str
            eff_end = end_date_str or contract_end_str

            # Claims: filter by encounterdatefrom within effective period (enrollee's client current contract when no dates given)
            claims_date_filter = ""
            if eff_start and eff_end:
                claims_date_filter = f"AND encounterdatefrom >= DATE '{eff_start}' AND encounterdatefrom <= DATE '{eff_end}'"
            elif eff_start:
                claims_date_filter = f"AND encounterdatefrom >= DATE '{eff_start}'"
            elif eff_end:
                claims_date_filter = f"AND encounterdatefrom <= DATE '{eff_end}'"

            # Unclaimed PA: filter by requestdate within effective period (enrollee's client current contract when no dates given)
            pa_date_filter = ""
            if eff_start and eff_end:
                pa_date_filter = f"AND requestdate >= TIMESTAMP '{eff_start}' AND requestdate <= TIMESTAMP '{eff_end}'"
            elif eff_start:
                pa_date_filter = f"AND requestdate >= TIMESTAMP '{eff_start}'"
            elif eff_end:
                pa_date_filter = f"AND requestdate <= TIMESTAMP '{eff_end}'"

            # 2. Total medical cost (claims by encounterdatefrom in contract + unclaimed PA by requestdate in contract)
            cost_query = f"""
            WITH claims_cost AS (
                SELECT COALESCE(SUM(approvedamount), 0) as claims_total
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE enrollee_id = '{enrollee_id}' {claims_date_filter}
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE enrollee_id = '{enrollee_id}'
                    AND panumber IS NOT NULL
                    AND panumber != 0
                    {claims_date_filter}
            ),
            unclaimed_pa_cost AS (
                SELECT COALESCE(SUM(pa.granted), 0) as unclaimed_total
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.IID = '{enrollee_id}'
                    {pa_date_filter}
                    AND CAST(pa.panumber AS INT64) NOT IN (
                        SELECT panumber_int FROM claimed_pa_numbers
                    )
            )
            SELECT 
                cc.claims_total,
                up.unclaimed_total,
                (cc.claims_total + up.unclaimed_total) as total_medical_cost
            FROM claims_cost cc
            CROSS JOIN unclaimed_pa_cost up
            """
            
            cost_df = conn.execute(cost_query).fetchdf()
            cost_data = cost_df.iloc[0].to_dict() if not cost_df.empty else {'claims_total': 0, 'unclaimed_total': 0, 'total_medical_cost': 0}
            
            # 3. Total visits count (by encounterdatefrom in period)
            visits_query = f"""
            SELECT COUNT(DISTINCT panumber) as visit_count
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE enrollee_id = '{enrollee_id}'
                AND panumber IS NOT NULL
                {claims_date_filter}
            """
            visits_df = conn.execute(visits_query).fetchdf()
            try:
                visit_count_val = visits_df.iloc[0]['visit_count'] if not visits_df.empty else 0
                visit_count = int(visit_count_val) if visit_count_val is not None and visit_count_val == visit_count_val else 0
            except (ValueError, TypeError, IndexError):
                visit_count = 0
            
            # 4. Top 3 hospitals and their bands (claims by encounterdatefrom in period)
            hospitals_query = f"""
            WITH claims_with_provider AS (
                SELECT 
                    c.panumber,
                    c.approvedamount,
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(c.nhisproviderid AS VARCHAR)) AS INTEGER) as provider_id_clean
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.enrollee_id = '{enrollee_id}'
                    AND c.nhisproviderid IS NOT NULL
                    {claims_date_filter}
            ),
            providers_clean AS (
                SELECT 
                    p.providerid,
                    p.providername,
                    p.bands,
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(p.providerid AS VARCHAR)) AS INTEGER) as provider_id_clean
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                WHERE p.providerid IS NOT NULL
            )
            SELECT 
                COALESCE(pr.providername, 'Unknown') as providername,
                pr.bands as band,
                COUNT(DISTINCT cp.panumber) as visit_count,
                SUM(cp.approvedamount) as total_cost
            FROM claims_with_provider cp
            LEFT JOIN providers_clean pr ON cp.provider_id_clean = pr.provider_id_clean
            WHERE cp.provider_id_clean IS NOT NULL
            GROUP BY pr.providername, pr.bands
            ORDER BY total_cost DESC
            LIMIT 3
            """
            hospitals_df = conn.execute(hospitals_query).fetchdf()
            top_hospitals = hospitals_df.to_dict('records') if not hospitals_df.empty else []
            
            # 5. Top diagnosis by cost (claims by encounterdatefrom in period)
            diagnosis_cost_query = f"""
            SELECT 
                c.diagnosiscode,
                d.diagnosisdesc,
                SUM(c.approvedamount) as total_cost,
                COUNT(DISTINCT c.panumber) as visit_count
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
            WHERE c.enrollee_id = '{enrollee_id}'
                AND c.diagnosiscode IS NOT NULL
                {claims_date_filter}
            GROUP BY c.diagnosiscode, d.diagnosisdesc
            ORDER BY total_cost DESC
            LIMIT 10
            """
            diagnosis_cost_df = conn.execute(diagnosis_cost_query).fetchdf()
            top_diagnosis_by_cost = diagnosis_cost_df.to_dict('records') if not diagnosis_cost_df.empty else []
            
            # 6. Top diagnosis by visit count (claims by encounterdatefrom in period)
            diagnosis_visit_query = f"""
            SELECT 
                c.diagnosiscode,
                d.diagnosisdesc,
                COUNT(DISTINCT c.panumber) as visit_count,
                SUM(c.approvedamount) as total_cost
            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
            WHERE c.enrollee_id = '{enrollee_id}'
                AND c.diagnosiscode IS NOT NULL
                {claims_date_filter}
            GROUP BY c.diagnosiscode, d.diagnosisdesc
            ORDER BY visit_count DESC
            LIMIT 10
            """
            diagnosis_visit_df = conn.execute(diagnosis_visit_query).fetchdf()
            top_diagnosis_by_visit = diagnosis_visit_df.to_dict('records') if not diagnosis_visit_df.empty else []
            
            # 7. Medical history (last 3 visits from PA and Claims)
            # Claims by encounterdatefrom in period; PA by requestdate in period. "Claimed" panumbers = those in claims with encounter in period.
            medical_history_query = f"""
            WITH claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE enrollee_id = '{enrollee_id}'
                    AND panumber IS NOT NULL
                    {claims_date_filter}
            ),
            pa_with_provider AS (
                SELECT 
                    pa.panumber,
                    pa.requestdate as encounter_date,
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(pa.providerid AS VARCHAR)) AS INTEGER) as provider_id_clean,
                    SUM(pa.granted) as cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.IID = '{enrollee_id}'
                    {pa_date_filter}
                    AND CAST(pa.panumber AS INT64) NOT IN (SELECT panumber_int FROM claimed_pa_numbers)
                    AND pa.providerid IS NOT NULL
                GROUP BY pa.panumber, pa.requestdate, pa.providerid
            ),
            providers_lookup AS (
                SELECT 
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(p.providerid AS VARCHAR)) AS INTEGER) as provider_id_clean,
                    p.providername
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                WHERE p.providerid IS NOT NULL
            ),
            pa_encounters AS (
                SELECT 
                    pwp.panumber,
                    pwp.encounter_date,
                    COALESCE(pl.providername, 'Unknown') as providername,
                    pwp.cost
                FROM pa_with_provider pwp
                LEFT JOIN providers_lookup pl ON pwp.provider_id_clean = pl.provider_id_clean
            ),
            claims_with_provider AS (
                SELECT 
                    COALESCE(CAST(c.panumber AS VARCHAR), 'NO_PA_' || CAST(c.encounterdatefrom AS VARCHAR)) as panumber,
                    COALESCE(c.encounterdatefrom, c.datesubmitted) as encounter_date,
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(c.nhisproviderid AS VARCHAR)) AS INTEGER) as provider_id_clean,
                    SUM(c.approvedamount) as cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.enrollee_id = '{enrollee_id}'
                    {claims_date_filter}
                GROUP BY c.panumber, c.encounterdatefrom, c.datesubmitted, c.nhisproviderid
            ),
            claim_encounters AS (
                SELECT 
                    cwp.panumber,
                    cwp.encounter_date,
                    COALESCE(pl.providername, 'Unknown') as providername,
                    cwp.cost
                FROM claims_with_provider cwp
                LEFT JOIN providers_lookup pl ON cwp.provider_id_clean = pl.provider_id_clean
            ),
            all_encounters AS (
                SELECT panumber, encounter_date, providername, cost FROM pa_encounters
                UNION ALL
                SELECT panumber, encounter_date, providername, cost FROM claim_encounters
            )
            SELECT 
                panumber,
                encounter_date,
                providername,
                SUM(cost) as total_cost
            FROM all_encounters
            GROUP BY panumber, encounter_date, providername
            ORDER BY encounter_date DESC
            LIMIT 3
            """
            history_df = conn.execute(medical_history_query).fetchdf()
            medical_history = history_df.to_dict('records') if not history_df.empty else []
            
            # Get diagnoses for each visit in medical history
            for visit in medical_history:
                panumber = visit.get('panumber')
                encounter_date = visit.get('encounter_date')
                
                if panumber:
                    try:
                        # Check if this is a NO_PA visit (claims without panumber)
                        if str(panumber).startswith('NO_PA_'):
                            # For claims without PA, match by enrollee_id and encounter_date
                            encounter_date_str = encounter_date.strftime('%Y-%m-%d') if encounter_date else None
                            if encounter_date_str:
                                diag_query = f"""
                                SELECT DISTINCT
                                    c.diagnosiscode,
                                    d.diagnosisdesc
                                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                                LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
                                WHERE c.enrollee_id = '{enrollee_id}'
                                    AND c.panumber IS NULL
                                    AND DATE(COALESCE(c.encounterdatefrom, c.datesubmitted)) = DATE '{encounter_date_str}'
                                    AND c.diagnosiscode IS NOT NULL
                                """
                            else:
                                visit['diagnoses'] = []
                                continue
                        else:
                            # For visits with panumber, match by panumber, enrollee_id, AND encounter_date
                            # This ensures we only get diagnoses for this specific visit
                            encounter_date_str = encounter_date.strftime('%Y-%m-%d') if encounter_date else None
                            
                            # First try to get from TBPADIAGNOSIS (PA diagnoses)
                            pa_diag_query = f"""
                            SELECT DISTINCT
                                tpa.code as diagnosiscode,
                                d.diagnosisdesc
                            FROM "AI DRIVEN DATA"."TBPADIAGNOSIS" tpa
                            LEFT JOIN "AI DRIVEN DATA"."PA DATA" pa ON CAST(tpa.panumber AS INT64) = CAST(pa.panumber AS INT64)
                            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON tpa.code = d.diagnosiscode
                            WHERE CAST(tpa.panumber AS INT64) = CAST({panumber} AS INT64)
                                AND pa.IID = '{enrollee_id}'
                            """
                            
                            # Then get from CLAIMS DATA for this specific visit
                            claims_diag_query = f"""
                            SELECT DISTINCT
                                c.diagnosiscode,
                                d.diagnosisdesc
                            FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                            LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON c.diagnosiscode = d.diagnosiscode
                            WHERE c.enrollee_id = '{enrollee_id}'
                                AND CAST(c.panumber AS INT64) = CAST({panumber} AS INT64)
                                AND c.diagnosiscode IS NOT NULL
                            """
                            
                            # Add encounter_date filter if available
                            if encounter_date_str:
                                claims_diag_query += f" AND DATE(COALESCE(c.encounterdatefrom, c.datesubmitted)) = DATE '{encounter_date_str}'"
                            
                            # Combine both queries
                            diag_query = f"""
                            {pa_diag_query}
                            UNION
                            {claims_diag_query}
                            """
                        
                        diag_df = conn.execute(diag_query).fetchdf()
                        visit['diagnoses'] = diag_df.to_dict('records') if not diag_df.empty else []
                    except Exception as e:
                        logger.warning(f"Could not fetch diagnoses for panumber {panumber}: {e}")
                        visit['diagnoses'] = []
                else:
                    visit['diagnoses'] = []
            
            # 8. Benefit limits and usage
            # Fix: Use provided start_date/end_date if available, otherwise fall back to contract period
            # Also fix double counting by properly excluding claimed PA
            # Use provided dates if available, otherwise use contract dates
            contract_start_str = contract_start.strftime('%Y-%m-%d') if contract_start and pd.notna(contract_start) else None
            contract_end_str = contract_end.strftime('%Y-%m-%d') if contract_end and pd.notna(contract_end) else None
            benefit_start_str = start_date_str if start_date_str else contract_start_str
            benefit_end_str = end_date_str if end_date_str else contract_end_str
            
            logger.info(f"Enrollee {enrollee_id} - Benefit calculation using dates: start={benefit_start_str}, end={benefit_end_str}")
            
            contract_date_filter = ""
            contract_pa_date_filter = ""
            if benefit_start_str and benefit_end_str:
                contract_date_filter = f"AND encounterdatefrom >= DATE '{benefit_start_str}' AND encounterdatefrom <= DATE '{benefit_end_str}'"
                contract_pa_date_filter = f"AND requestdate >= TIMESTAMP '{benefit_start_str}' AND requestdate <= TIMESTAMP '{benefit_end_str}'"
            
            benefit_query = f"""
            WITH member_info AS (
                SELECT memberid, groupid, planid as member_planid
                FROM "AI DRIVEN DATA"."MEMBERS"
                WHERE enrollee_id = '{enrollee_id}'
                LIMIT 1
            ),
            member_plan_from_table AS (
                SELECT planid
                FROM "AI DRIVEN DATA"."MEMBER_PLANS"
                WHERE memberid = (SELECT memberid FROM member_info LIMIT 1)
                    AND iscurrent = 1
                LIMIT 1
            ),
            group_plan_fallback AS (
                SELECT gp.planid
                FROM member_info mi
                INNER JOIN "AI DRIVEN DATA"."GROUP_PLANS" gp ON mi.groupid = gp.groupid
                WHERE NOT EXISTS (SELECT 1 FROM member_plan_from_table)
                    AND mi.member_planid IS NULL
                LIMIT 1
            ),
            resolved_plan AS (
                SELECT planid FROM member_plan_from_table
                UNION ALL
                SELECT member_planid as planid FROM member_info WHERE member_planid IS NOT NULL AND NOT EXISTS (SELECT 1 FROM member_plan_from_table)
                UNION ALL
                SELECT planid FROM group_plan_fallback WHERE NOT EXISTS (SELECT 1 FROM member_plan_from_table)
                LIMIT 1
            ),
            plan_benefits AS (
                SELECT 
                    pl.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc,
                    pl.maxlimit,
                    pl.countperannum,
                    pl.daysallowed,
                    pl.countperlifetime,
                    bc.benefitcodename
                FROM resolved_plan rp
                INNER JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" pl ON rp.planid = pl.planid
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON pl.benefitcodeid = bc.benefitcodeid
                WHERE (pl.maxlimit IS NOT NULL AND pl.maxlimit > 0)
                   OR (pl.daysallowed IS NOT NULL AND pl.daysallowed > 0)
                   OR (pl.countperannum IS NOT NULL AND pl.countperannum > 0)
                   OR (pl.countperlifetime IS NOT NULL AND pl.countperlifetime > 0)
            ),
            claimed_pa_for_benefits AS (
                SELECT DISTINCT CAST(panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE enrollee_id = '{enrollee_id}' 
                    AND panumber IS NOT NULL
                    AND panumber != 0
                    {contract_date_filter}
            ),
            enrollee_utilization AS (
                SELECT 
                    c.code as procedurecode,
                    SUM(c.approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.enrollee_id = '{enrollee_id}' 
                    AND c.code IS NOT NULL
                    {contract_date_filter}
                GROUP BY c.code
                
                UNION ALL
                
                SELECT 
                    pa.code as procedurecode,
                    SUM(pa.granted) as claims_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.IID = '{enrollee_id}' 
                    {contract_pa_date_filter}
                    AND CAST(pa.panumber AS INT64) NOT IN (
                        SELECT panumber_int FROM claimed_pa_for_benefits
                    )
                    AND pa.code IS NOT NULL
                GROUP BY pa.code
            ),
            benefit_mapping AS (
                SELECT 
                    u.procedurecode,
                    u.claims_cost,
                    b.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc
                FROM enrollee_utilization u
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" b ON u.procedurecode = b.procedurecode
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON b.benefitcodeid = bc.benefitcodeid
                WHERE b.benefitcodeid IS NOT NULL
            ),
            benefit_utilization AS (
                SELECT 
                    bm.benefitcodeid,
                    bm.benefitdesc,
                    SUM(bm.claims_cost) as used_amount,
                    COUNT(DISTINCT bm.procedurecode) as used_count
                FROM benefit_mapping bm
                GROUP BY bm.benefitcodeid, bm.benefitdesc
            ),
            benefit_limits AS (
                -- Get plan benefits with their usage
                SELECT 
                    pb.benefitcodeid,
                    pb.benefitdesc,
                    pb.benefitcodename,
                    COALESCE(bu.used_amount, 0) as used_amount,
                    COALESCE(bu.used_count, 0) as used_count,
                    pb.maxlimit,
                    pb.countperannum,
                    pb.daysallowed,
                    pb.countperlifetime
                FROM plan_benefits pb
                LEFT JOIN benefit_utilization bu ON pb.benefitcodeid = bu.benefitcodeid
            )
            SELECT * FROM benefit_limits
            ORDER BY used_amount DESC, benefitdesc ASC
            """
            benefit_df = conn.execute(benefit_query).fetchdf()
            
            # Clean NaN/Infinity values before converting to dict (JSON serialization issue)
            if not benefit_df.empty:
                # Replace NaN/Infinity with None for numeric columns
                numeric_cols = benefit_df.select_dtypes(include=['float64', 'float32', 'int64', 'int32']).columns
                for col in numeric_cols:
                    benefit_df[col] = benefit_df[col].where(pd.notnull(benefit_df[col]), None)
                    # Replace infinity values
                    benefit_df[col] = benefit_df[col].replace([float('inf'), float('-inf')], None)
                # Replace NaN in string columns with None
                string_cols = benefit_df.select_dtypes(include=['object']).columns
                for col in string_cols:
                    benefit_df[col] = benefit_df[col].where(pd.notnull(benefit_df[col]), None)
            
            benefit_limits = benefit_df.to_dict('records') if not benefit_df.empty else []
            
            # 9. Calculate Average Cost Per Visit (instead of PMPM)
            # Average Cost = Total medical cost / number of visits
            avg_cost_per_visit = cost_data['total_medical_cost'] / visit_count if visit_count > 0 else 0
            
            # Helper function to safely convert to float, handling NaN and Infinity
            def safe_float(value):
                try:
                    if value is None:
                        return 0.0
                    val = float(value)
                    if not (val == val):  # Check for NaN
                        return 0.0
                    if val == float('inf') or val == float('-inf'):
                        return 0.0
                    return val
                except (ValueError, TypeError):
                    return 0.0
            
            return {
                'success': True,
                'enrollee_id': enrollee_id,
                'member_info': {
                    **member_info,
                    'age': age
                },
                'cost_summary': {
                    'claims_cost': safe_float(cost_data.get('claims_total', 0)),
                    'unclaimed_pa_cost': safe_float(cost_data.get('unclaimed_total', 0)),
                    'total_medical_cost': safe_float(cost_data.get('total_medical_cost', 0)),
                    'avg_cost_per_visit': safe_float(avg_cost_per_visit)
                },
                'visit_count': visit_count,
                'top_hospitals': [
                    {
                        **h,
                        'total_cost': safe_float(h.get('total_cost', 0)),
                        'visit_count': int(safe_float(h.get('visit_count', 0)))
                    }
                    for h in top_hospitals
                ] if top_hospitals else [],
                'top_diagnosis_by_cost': [
                    {
                        **d,
                        'total_cost': safe_float(d.get('total_cost', 0))
                    }
                    for d in top_diagnosis_by_cost
                ] if top_diagnosis_by_cost else [],
                'top_diagnosis_by_visit': [
                    {
                        **d,
                        'total_cost': safe_float(d.get('total_cost', 0)),
                        'visit_count': int(safe_float(d.get('visit_count', 0)))
                    }
                    for d in top_diagnosis_by_visit
                ] if top_diagnosis_by_visit else [],
                'medical_history': [
                    {
                        **m,
                        'total_cost': safe_float(m.get('total_cost', 0))
                    }
                    for m in medical_history
                ] if medical_history else [],
                'benefit_limits': [
                    {
                        **b,
                        'used_amount': safe_float(b.get('used_amount', 0)),
                        'used_count': int(safe_float(b.get('used_count', 0))),
                        'maxlimit': safe_float(b.get('maxlimit')) if b.get('maxlimit') is not None else None,
                        'countperannum': int(safe_float(b.get('countperannum'))) if b.get('countperannum') is not None else None,
                        'daysallowed': safe_float(b.get('daysallowed')) if b.get('daysallowed') is not None else None,
                        'countperlifetime': int(safe_float(b.get('countperlifetime'))) if b.get('countperlifetime') is not None else None,
                        'benefitcodename': b.get('benefitcodename') or None
                    }
                    for b in benefit_limits
                ] if benefit_limits else [],
                'period': {
                    'start_date': start_date_str,
                    'end_date': end_date_str
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting enrollee profile: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }


    @staticmethod
    def get_client_profile(
        groupname: str,
        conn=None
    ) -> Dict[str, Any]:
        """
        Get comprehensive profile for a specific client/company
        
        Args:
            groupname: Client/company name
            conn: Database connection (optional, will create if not provided)
            
        Returns:
            Dictionary with comprehensive client profile data
        """
        try:
            import duckdb
            from core.database import get_db_connection
            
            if conn is None:
                conn = get_db_connection()
            
            # 1. Get client basic info and current contract
            client_query = f"""
            WITH current_contract AS (
                SELECT 
                    gc.groupid,
                    gc.groupname,
                    gc.startdate as contract_start,
                    gc.enddate as contract_end
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
                WHERE gc.groupname = '{groupname.replace("'", "''")}'
                    AND gc.iscurrent = 1
                LIMIT 1
            )
            SELECT 
                g.groupid,
                g.groupname,
                cc.contract_start,
                cc.contract_end
            FROM "AI DRIVEN DATA"."GROUPS" g
            INNER JOIN current_contract cc ON g.groupid = cc.groupid
            WHERE g.groupname = '{groupname.replace("'", "''")}'
            LIMIT 1
            """
            
            client_df = conn.execute(client_query).fetchdf()
            if client_df.empty:
                return {'success': False, 'error': f'Client {groupname} not found or has no active contract'}
            
            client_info = client_df.iloc[0].to_dict()
            groupid = client_info['groupid']
            contract_start = client_info['contract_start']
            contract_end = client_info['contract_end']
            
            contract_start_str = contract_start.strftime('%Y-%m-%d') if contract_start else None
            contract_end_str = contract_end.strftime('%Y-%m-%d') if contract_end else None
            
            # 2. Number of active lives
            active_lives_query = f"""
            SELECT COUNT(DISTINCT m.memberid) as active_lives
            FROM "AI DRIVEN DATA"."MEMBERS" m
            WHERE m.groupid = {groupid}
                AND m.iscurrent = 1
            """
            active_lives_df = conn.execute(active_lives_query).fetchdf()
            active_lives = int(active_lives_df.iloc[0]['active_lives']) if not active_lives_df.empty else 0
            
            # 3a. Gender split and data completeness for this client
            gender_completeness_query = f"""
            SELECT 
                COALESCE(mem.genderid, m.genderid) as genderid,
                COALESCE(mem.email1, m.email1) as email,
                COALESCE(mem.dob, m.dob) as dob,
                COALESCE(mem.address1, m.address1) as address
            FROM "AI DRIVEN DATA"."MEMBERS" m
            LEFT JOIN "AI DRIVEN DATA"."MEMBER" mem 
                ON CAST(m.memberid AS BIGINT) = mem.memberid
            WHERE m.groupid = {groupid}
                AND m.iscurrent = 1
            """
            gc_df = conn.execute(gender_completeness_query).fetchdf()
            
            gender_split = {
                'male': 0,
                'female': 0,
                'other': 0,
                'unknown': 0,
                'total': 0
            }
            completeness = {
                'email': {'with': 0, 'without': 0},
                'dob': {'with': 0, 'without': 0},
                'address': {'with': 0, 'without': 0}
            }
            
            if not gc_df.empty:
                for _, row in gc_df.iterrows():
                    gender_id = row.get('genderid')
                    if gender_id == 1:
                        gender_split['male'] += 1
                    elif gender_id == 2:
                        gender_split['female'] += 1
                    elif gender_id == 3:
                        gender_split['other'] += 1
                    else:
                        gender_split['unknown'] += 1
                    
                    email = str(row.get('email') or '').strip()
                    dob = row.get('dob')
                    address = str(row.get('address') or '').strip()
                    
                    if email:
                        completeness['email']['with'] += 1
                    else:
                        completeness['email']['without'] += 1
                    
                    if pd.notna(dob):
                        completeness['dob']['with'] += 1
                    else:
                        completeness['dob']['without'] += 1
                    
                    if address:
                        completeness['address']['with'] += 1
                    else:
                        completeness['address']['without'] += 1
                
                gender_split['total'] = (
                    gender_split['male'] +
                    gender_split['female'] +
                    gender_split['other'] +
                    gender_split['unknown']
                )
            
            # 3. Number of plans and plan names from GROUP_PLANS
            plans_query = f"""
            SELECT 
                COUNT(DISTINCT gp.planid) as plan_count,
                STRING_AGG(DISTINCT p.planname, ', ') as plan_names
            FROM "AI DRIVEN DATA"."GROUP_PLANS" gp
            LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON gp.planid = p.planid
            WHERE gp.groupid = {groupid}
                AND gp.iscurrent = 1
            """
            plans_df = conn.execute(plans_query).fetchdf()
            plan_count = int(plans_df.iloc[0]['plan_count']) if not plans_df.empty else 0
            plan_names = plans_df.iloc[0]['plan_names'] if not plans_df.empty and plans_df.iloc[0]['plan_names'] else 'N/A'
            
            # 4. Get MLR data using MLR API calculation functions
            # Import MLR functions to reuse existing calculation logic
            from api.routes.mlr import load_mlr_data, calculate_mlr_basic
            
            # Initialize variables
            mlr_value = None
            pmpm_value = None
            premium_pmpm_value = None
            debit_amount = None
            
            try:
                mlr_data = load_mlr_data()
                mlr_result = calculate_mlr_basic(mlr_data)
                client_mlr = mlr_result.filter(pl.col('groupname') == groupname)
                
                if client_mlr.height > 0:
                    mlr_row = client_mlr.to_pandas().iloc[0]
                    mlr_value = float(mlr_row['MLR (%)']) if pd.notna(mlr_row.get('MLR (%)')) else None
                    pmpm_value = float(mlr_row['Avg_PMPM']) if pd.notna(mlr_row.get('Avg_PMPM')) else None
                    premium_pmpm_value = float(mlr_row['Premium_PMPM']) if pd.notna(mlr_row.get('Premium_PMPM')) else None
                    debit_amount = float(mlr_row['debit_amount']) if pd.notna(mlr_row.get('debit_amount')) else 0.0
                else:
                    # Fallback: calculate basic values if MLR calculation doesn't return data
                    debit_query = f"""
                    SELECT COALESCE(SUM(Amount), 0) as debit_total
                    FROM "AI DRIVEN DATA"."DEBIT_NOTE"
                    WHERE CompanyName = '{groupname.replace("'", "''")}'
                        AND "From" >= DATE '{contract_start_str}'
                        AND "From" <= DATE '{contract_end_str}'
                        AND Description NOT LIKE '%tpa%'
                    """
                    debit_df = conn.execute(debit_query).fetchdf()
                    debit_amount = float(debit_df.iloc[0]['debit_total']) if not debit_df.empty else 0.0
                    mlr_value = None
                    pmpm_value = None
                    premium_pmpm_value = None
            except Exception as e:
                logger.warning(f"Error getting MLR data for {groupname}: {e}, using fallback calculation")
                # Fallback: get debit amount only
                debit_query = f"""
                SELECT COALESCE(SUM(Amount), 0) as debit_total
                FROM "AI DRIVEN DATA"."DEBIT_NOTE"
                WHERE CompanyName = '{groupname.replace("'", "''")}'
                    AND "From" >= DATE '{contract_start_str}'
                    AND "From" <= DATE '{contract_end_str}'
                    AND Description NOT LIKE '%tpa%'
                """
                debit_df = conn.execute(debit_query).fetchdf()
                debit_amount = float(debit_df.iloc[0]['debit_total']) if not debit_df.empty else 0.0
                mlr_value = None
                pmpm_value = None
                premium_pmpm_value = None
            
            # 5. Total PA cost within contract period
            pa_cost_query = f"""
            SELECT COALESCE(SUM(granted), 0) as total_pa_cost
            FROM "AI DRIVEN DATA"."PA DATA"
            WHERE groupname = '{groupname.replace("'", "''")}'
                AND requestdate >= TIMESTAMP '{contract_start_str}'
                AND requestdate <= TIMESTAMP '{contract_end_str}'
            """
            pa_cost_df = conn.execute(pa_cost_query).fetchdf()
            total_pa_cost = float(pa_cost_df.iloc[0]['total_pa_cost']) if not pa_cost_df.empty else 0.0

            # 6. Total claims cost by encounterdatefrom within contract period
            claims_encounter_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_encounter
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE nhisgroupid = '{groupid}'
                AND encounterdatefrom >= DATE '{contract_start_str}'
                AND encounterdatefrom <= DATE '{contract_end_str}'
            """
            claims_encounter_df = conn.execute(claims_encounter_query).fetchdf()
            total_claims_encounter = float(claims_encounter_df.iloc[0]['total_claims_encounter']) if not claims_encounter_df.empty else 0.0

            # 7. Total claims cost by datesubmitted within contract period
            claims_submitted_query = f"""
            SELECT COALESCE(SUM(approvedamount), 0) as total_claims_submitted
            FROM "AI DRIVEN DATA"."CLAIMS DATA"
            WHERE nhisgroupid = '{groupid}'
                AND datesubmitted >= DATE '{contract_start_str}'
                AND datesubmitted <= DATE '{contract_end_str}'
            """
            claims_submitted_df = conn.execute(claims_submitted_query).fetchdf()
            total_claims_submitted = float(claims_submitted_df.iloc[0]['total_claims_submitted']) if not claims_submitted_df.empty else 0.0

            # 8. Total unclaimed PA within contract period (claimed = claim with encounterdatefrom in contract)
            # Use BIGINT for panumber so 12345.0 (claims) and "12345" (PA) match
            unclaimed_pa_total_query = f"""
            WITH claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber_bigint
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisgroupid = '{groupid}'
                    AND panumber IS NOT NULL
                    AND encounterdatefrom >= DATE '{contract_start_str}'
                    AND encounterdatefrom <= DATE '{contract_end_str}'
            )
            SELECT COALESCE(SUM(pa.granted), 0) as total_unclaimed_pa
            FROM "AI DRIVEN DATA"."PA DATA" pa
            WHERE pa.groupname = '{groupname.replace("'", "''")}'
                AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                AND pa.panumber IS NOT NULL
                AND CAST(pa.panumber AS BIGINT) IS NOT NULL
                AND CAST(pa.panumber AS BIGINT) NOT IN (SELECT panumber_bigint FROM claimed_pa_numbers)
            """
            unclaimed_pa_total_df = conn.execute(unclaimed_pa_total_query).fetchdf()
            total_unclaimed_pa = float(unclaimed_pa_total_df.iloc[0]['total_unclaimed_pa']) if not unclaimed_pa_total_df.empty else 0.0

            # 9. Total cash received (within contract period)
            cash_query = f"""
            SELECT COALESCE(SUM(Amount), 0) as total_cash_received
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
            WHERE groupname = '{groupname.replace("'", "''")}'
                AND Date >= DATE '{contract_start_str}'
                AND Date <= DATE '{contract_end_str}'
            """
            cash_df = conn.execute(cash_query).fetchdf()
            total_cash_received = float(cash_df.iloc[0]['total_cash_received']) if not cash_df.empty else 0.0
            
            # 9b. Cash received before contract (one month before contract start)
            from datetime import datetime, timedelta
            contract_start_date = datetime.strptime(contract_start_str, '%Y-%m-%d')
            one_month_before = contract_start_date - timedelta(days=30)
            one_month_before_str = one_month_before.strftime('%Y-%m-%d')
            
            # Use only the combined table to avoid double counting
            cash_before_query = f"""
            SELECT COALESCE(SUM(Amount), 0) as total_cash_before_contract
            FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
            WHERE groupname = '{groupname.replace("'", "''")}'
                AND Date >= DATE '{one_month_before_str}'
                AND Date < DATE '{contract_start_str}'
            """
            cash_before_df = conn.execute(cash_before_query).fetchdf()
            total_cash_before_contract = float(cash_before_df.iloc[0]['total_cash_before_contract']) if not cash_before_df.empty else 0.0
            
            # Get detailed breakdown of cash before contract (deduplicated from all tables)
            cash_before_details_query = f"""
            SELECT DISTINCT Date, Amount
            FROM (
                SELECT Date, Amount
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
                WHERE groupname = '{groupname.replace("'", "''")}'
                    AND Date >= DATE '{one_month_before_str}'
                    AND Date < DATE '{contract_start_str}'
                UNION
                SELECT Date, Amount
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2023"
                WHERE groupname = '{groupname.replace("'", "''")}'
                    AND Date >= DATE '{one_month_before_str}'
                    AND Date < DATE '{contract_start_str}'
                UNION
                SELECT Date, Amount
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2024"
                WHERE groupname = '{groupname.replace("'", "''")}'
                    AND Date >= DATE '{one_month_before_str}'
                    AND Date < DATE '{contract_start_str}'
                UNION
                SELECT Date, Amount
                FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED_2025"
                WHERE groupname = '{groupname.replace("'", "''")}'
                    AND Date >= DATE '{one_month_before_str}'
                    AND Date < DATE '{contract_start_str}'
            )
            ORDER BY Date DESC
            """
            cash_before_details_df = conn.execute(cash_before_details_query).fetchdf()
            cash_before_details = []
            if not cash_before_details_df.empty:
                for _, row in cash_before_details_df.iterrows():
                    cash_before_details.append({
                        'date': str(row['Date']),
                        'amount': float(row['Amount'])
                    })
            
            # 10. Expected cash (debit - cash received)
            expected_cash = (debit_amount or 0.0) - total_cash_received
            
            # 11. Monthly medical cost (for line graph): claims by encounterdatefrom, PA by requestdate in contract
            monthly_cost_query = f"""
            WITH claims_monthly AS (
                SELECT 
                    DATE_TRUNC('month', encounterdatefrom) as month,
                    SUM(approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisgroupid = '{groupid}'
                    AND encounterdatefrom >= DATE '{contract_start_str}'
                    AND encounterdatefrom <= DATE '{contract_end_str}'
                GROUP BY DATE_TRUNC('month', encounterdatefrom)
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber_bigint
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisgroupid = '{groupid}'
                    AND panumber IS NOT NULL
                    AND encounterdatefrom >= DATE '{contract_start_str}'
                    AND encounterdatefrom <= DATE '{contract_end_str}'
            ),
            unclaimed_pa_monthly AS (
                SELECT 
                    DATE_TRUNC('month', requestdate) as month,
                    SUM(granted) as unclaimed_pa_cost
                FROM "AI DRIVEN DATA"."PA DATA"
                WHERE groupname = '{groupname.replace("'", "''")}'
                    AND requestdate >= TIMESTAMP '{contract_start_str}'
                    AND requestdate <= TIMESTAMP '{contract_end_str}'
                    AND panumber IS NOT NULL
                    AND CAST(panumber AS BIGINT) IS NOT NULL
                    AND CAST(panumber AS BIGINT) NOT IN (SELECT panumber_bigint FROM claimed_pa_numbers)
                GROUP BY DATE_TRUNC('month', requestdate)
            )
            SELECT 
                COALESCE(c.month, pa.month) as month,
                COALESCE(c.claims_cost, 0) + COALESCE(pa.unclaimed_pa_cost, 0) as total_cost
            FROM claims_monthly c
            FULL OUTER JOIN unclaimed_pa_monthly pa ON c.month = pa.month
            ORDER BY month
            """
            monthly_cost_df = conn.execute(monthly_cost_query).fetchdf()
            monthly_costs = [
                {
                    'month': row['month'].strftime('%Y-%m') if pd.notna(row['month']) else None,
                    'total_cost': float(row['total_cost']) if pd.notna(row['total_cost']) else 0.0
                }
                for _, row in monthly_cost_df.iterrows()
            ] if not monthly_cost_df.empty else []
            
            # 8. Top 20 providers by cost and unique enrollee count (claims + unclaimed PA in contract; matches provider module)
            # Use BIGINT for panumber so claimed vs unclaimed match correctly
            providers_query = f"""
            WITH claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber_bigint
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisgroupid = '{groupid}'
                    AND panumber IS NOT NULL
                    AND encounterdatefrom >= DATE '{contract_start_str}'
                    AND encounterdatefrom <= DATE '{contract_end_str}'
            ),
            claims_with_provider AS (
                SELECT 
                    c.nhisproviderid as provider_id_raw,
                    c.approvedamount,
                    c.enrollee_id
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.nhisgroupid = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                    AND c.nhisproviderid IS NOT NULL
                    AND CAST(c.nhisproviderid AS VARCHAR) != ''
            ),
            pa_with_provider AS (
                SELECT 
                    pa.providerid as provider_id_raw,
                    pa.granted as approvedamount,
                    pa.IID as enrollee_id
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND pa.providerid IS NOT NULL
                    AND CAST(pa.providerid AS VARCHAR) != ''
                    AND CAST(pa.panumber AS BIGINT) IS NOT NULL
                    AND CAST(pa.panumber AS BIGINT) NOT IN (SELECT panumber_bigint FROM claimed_pa_numbers)
            ),
            all_provider_encounters AS (
                SELECT provider_id_raw, approvedamount, enrollee_id FROM claims_with_provider
                UNION ALL
                SELECT provider_id_raw, approvedamount, enrollee_id FROM pa_with_provider
            ),
            providers_clean AS (
                SELECT 
                    p.providerid,
                    p.providername
                FROM "AI DRIVEN DATA"."PROVIDERS" p
                WHERE p.providerid IS NOT NULL
                    AND CAST(p.providerid AS VARCHAR) != ''
            )
            SELECT 
                COALESCE(pr.providername, 'Unknown') as providername,
                SUM(ape.approvedamount) as total_cost,
                COUNT(DISTINCT ape.enrollee_id) as unique_enrollees
            FROM all_provider_encounters ape
            LEFT JOIN providers_clean pr ON TRY_CAST(TRIM(LEADING '0' FROM CAST(ape.provider_id_raw AS VARCHAR)) AS INTEGER) = TRY_CAST(TRIM(LEADING '0' FROM CAST(pr.providerid AS VARCHAR)) AS INTEGER)
            GROUP BY pr.providername
            ORDER BY total_cost DESC
            LIMIT 20
            """
            providers_df = conn.execute(providers_query).fetchdf()
            top_providers = [
                {
                    'providername': row['providername'],
                    'total_cost': float(row['total_cost']) if pd.notna(row['total_cost']) else 0.0,
                    'unique_enrollees': int(row['unique_enrollees']) if pd.notna(row['unique_enrollees']) else 0
                }
                for _, row in providers_df.iterrows()
            ] if not providers_df.empty else []
            
            # 9. Top 20 procedures by cost and by count (claims by encounterdatefrom in contract)
            procedures_query = f"""
            WITH claims_procedures AS (
                SELECT 
                    c.code as procedurecode,
                    c.approvedamount,
                    1 as count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.nhisgroupid = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                    AND c.code IS NOT NULL
            ),
            pa_procedures AS (
                SELECT 
                    pa.code as procedurecode,
                    pa.granted as approvedamount,
                    1 as count
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND pa.code IS NOT NULL
            ),
            all_procedures AS (
                SELECT procedurecode, approvedamount, count FROM claims_procedures
                UNION ALL
                SELECT procedurecode, approvedamount, count FROM pa_procedures
            )
            SELECT 
                procedurecode,
                SUM(approvedamount) as total_cost,
                SUM(count) as total_count
            FROM all_procedures
            GROUP BY procedurecode
            ORDER BY total_cost DESC
            LIMIT 20
            """
            procedures_df = conn.execute(procedures_query).fetchdf()
            top_procedures = [
                {
                    'procedurecode': row['procedurecode'],
                    'total_cost': float(row['total_cost']) if pd.notna(row['total_cost']) else 0.0,
                    'total_count': int(row['total_count']) if pd.notna(row['total_count']) else 0
                }
                for _, row in procedures_df.iterrows()
            ] if not procedures_df.empty else []
            
            # 10. Top 20 enrollees by cost and by visit (claims by encounterdatefrom, PA by requestdate in contract)
            enrollees_query = f"""
            WITH claims_by_enrollee AS (
                SELECT 
                    c.enrollee_id,
                    SUM(c.approvedamount) as claims_cost,
                    COUNT(DISTINCT c.panumber) as visit_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE c.nhisgroupid = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                GROUP BY c.enrollee_id
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS BIGINT) as panumber_bigint
                FROM "AI DRIVEN DATA"."CLAIMS DATA"
                WHERE nhisgroupid = '{groupid}'
                    AND panumber IS NOT NULL
                    AND encounterdatefrom >= DATE '{contract_start_str}'
                    AND encounterdatefrom <= DATE '{contract_end_str}'
            ),
            unclaimed_pa_by_enrollee AS (
                SELECT 
                    pa.IID as enrollee_id,
                    SUM(pa.granted) as unclaimed_pa_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND pa.panumber IS NOT NULL
                    AND CAST(pa.panumber AS BIGINT) IS NOT NULL
                    AND CAST(pa.panumber AS BIGINT) NOT IN (SELECT panumber_bigint FROM claimed_pa_numbers)
                GROUP BY pa.IID
            )
            SELECT 
                COALESCE(c.enrollee_id, pa.enrollee_id) as enrollee_id,
                COALESCE(c.claims_cost, 0) + COALESCE(pa.unclaimed_pa_cost, 0) as total_cost,
                COALESCE(c.visit_count, 0) as visit_count
            FROM claims_by_enrollee c
            FULL OUTER JOIN unclaimed_pa_by_enrollee pa ON c.enrollee_id = pa.enrollee_id
            ORDER BY total_cost DESC
            LIMIT 20
            """
            enrollees_df = conn.execute(enrollees_query).fetchdf()
            top_enrollees = [
                {
                    'enrollee_id': row['enrollee_id'],
                    'total_cost': float(row['total_cost']) if pd.notna(row['total_cost']) else 0.0,
                    'visit_count': int(row['visit_count']) if pd.notna(row['visit_count']) else 0
                }
                for _, row in enrollees_df.iterrows()
            ] if not enrollees_df.empty else []
            
            # 12. Benefit Limit Analysis for all enrollees in this client
            benefit_limit_query = f"""
            WITH client_enrollees AS (
                SELECT DISTINCT m.enrollee_id, m.memberid
                FROM "AI DRIVEN DATA"."MEMBERS" m
                WHERE m.groupid = {groupid}
                    AND m.iscurrent = 1
            ),
            enrollee_plans AS (
                SELECT 
                    ce.enrollee_id,
                    ce.memberid,
                    mp.planid
                FROM client_enrollees ce
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON ce.memberid = mp.memberid
                WHERE mp.iscurrent = 1
            ),
            plan_benefits AS (
                SELECT 
                    ep.enrollee_id,
                    pl.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc,
                    pl.maxlimit,
                    pl.countperannum
                FROM enrollee_plans ep
                INNER JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" pl ON ep.planid = pl.planid
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON pl.benefitcodeid = bc.benefitcodeid
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(c.panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.panumber IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
            ),
            enrollee_utilization AS (
                SELECT 
                    c.enrollee_id,
                    c.code as procedurecode,
                    SUM(c.approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.code IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                GROUP BY c.enrollee_id, c.code
                
                UNION ALL
                
                SELECT 
                    pa.IID as enrollee_id,
                    pa.code as procedurecode,
                    SUM(pa.granted) as claims_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND CAST(pa.panumber AS INT64) NOT IN (SELECT panumber_int FROM claimed_pa_numbers)
                    AND pa.code IS NOT NULL
                GROUP BY pa.IID, pa.code
            ),
            benefit_mapping AS (
                SELECT 
                    u.enrollee_id,
                    u.procedurecode,
                    u.claims_cost,
                    b.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc
                FROM enrollee_utilization u
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" b ON u.procedurecode = b.procedurecode
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON b.benefitcodeid = bc.benefitcodeid
                WHERE b.benefitcodeid IS NOT NULL
            ),
            benefit_utilization AS (
                SELECT 
                    bm.enrollee_id,
                    bm.benefitcodeid,
                    bm.benefitdesc,
                    SUM(bm.claims_cost) as used_amount,
                    COUNT(DISTINCT bm.procedurecode) as used_count
                FROM benefit_mapping bm
                GROUP BY bm.enrollee_id, bm.benefitcodeid, bm.benefitdesc
            ),
            benefit_comparison AS (
                SELECT 
                    pb.enrollee_id,
                    pb.benefitcodeid,
                    pb.benefitdesc,
                    COALESCE(bu.used_amount, 0) as used_amount,
                    COALESCE(bu.used_count, 0) as used_count,
                    pb.maxlimit,
                    pb.countperannum,
                    -- Monetary overage (for maxlimit)
                    CASE 
                        WHEN pb.maxlimit IS NOT NULL AND COALESCE(bu.used_amount, 0) > pb.maxlimit 
                        THEN COALESCE(bu.used_amount, 0) - pb.maxlimit
                        ELSE 0
                    END as monetary_overage,
                    -- Count overage (for countperannum) - convert to monetary equivalent if needed, or just track separately
                    CASE 
                        WHEN pb.countperannum IS NOT NULL AND COALESCE(bu.used_count, 0) > pb.countperannum
                        THEN COALESCE(bu.used_count, 0) - pb.countperannum
                        ELSE 0
                    END as count_overage,
                    CASE 
                        WHEN pb.maxlimit IS NOT NULL AND COALESCE(bu.used_amount, 0) = pb.maxlimit 
                        THEN 1
                        WHEN pb.countperannum IS NOT NULL AND COALESCE(bu.used_count, 0) = pb.countperannum
                        THEN 1
                        ELSE 0
                    END as hit_limit_exactly,
                    CASE 
                        WHEN pb.maxlimit IS NOT NULL AND COALESCE(bu.used_amount, 0) > pb.maxlimit 
                        THEN 1
                        WHEN pb.countperannum IS NOT NULL AND COALESCE(bu.used_count, 0) > pb.countperannum
                        THEN 1
                        ELSE 0
                    END as exceeded_limit
                FROM plan_benefits pb
                LEFT JOIN benefit_utilization bu ON pb.enrollee_id = bu.enrollee_id AND pb.benefitcodeid = bu.benefitcodeid
            )
            SELECT 
                COUNT(DISTINCT CASE WHEN exceeded_limit = 1 THEN enrollee_id END) as enrollees_exceeded_count,
                COUNT(DISTINCT CASE WHEN hit_limit_exactly = 1 THEN enrollee_id END) as enrollees_hit_limit_count,
                SUM(monetary_overage) as total_overage_amount,
                COUNT(DISTINCT CASE WHEN exceeded_limit = 1 THEN enrollee_id || '|' || benefitcodeid END) as total_violations
            FROM benefit_comparison
            """
            benefit_limit_df = conn.execute(benefit_limit_query).fetchdf()
            
            benefit_limit_stats = {
                'enrollees_exceeded_count': 0,
                'enrollees_hit_limit_count': 0,
                'total_overage_amount': 0.0,
                'total_violations': 0,
                'exceeded_violations': [],
                'hit_limit_violations': []
            }
            
            if not benefit_limit_df.empty:
                row = benefit_limit_df.iloc[0]
                benefit_limit_stats = {
                    'enrollees_exceeded_count': int(row['enrollees_exceeded_count']) if pd.notna(row['enrollees_exceeded_count']) else 0,
                    'enrollees_hit_limit_count': int(row['enrollees_hit_limit_count']) if pd.notna(row['enrollees_hit_limit_count']) else 0,
                    'total_overage_amount': float(row['total_overage_amount']) if pd.notna(row['total_overage_amount']) else 0.0,
                    'total_violations': int(row['total_violations']) if pd.notna(row['total_violations']) else 0,
                    'exceeded_violations': [],
                    'hit_limit_violations': []
                }
            
            # Get detailed exceeded violations
            exceeded_violations_query = f"""
            WITH client_enrollees AS (
                SELECT DISTINCT m.enrollee_id, m.memberid
                FROM "AI DRIVEN DATA"."MEMBERS" m
                WHERE m.groupid = {groupid}
                    AND m.iscurrent = 1
            ),
            enrollee_plans AS (
                SELECT 
                    ce.enrollee_id,
                    ce.memberid,
                    mp.planid
                FROM client_enrollees ce
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON ce.memberid = mp.memberid
                WHERE mp.iscurrent = 1
            ),
            plan_benefits AS (
                SELECT 
                    ep.enrollee_id,
                    pl.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc,
                    pl.maxlimit,
                    pl.countperannum
                FROM enrollee_plans ep
                INNER JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" pl ON ep.planid = pl.planid
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON pl.benefitcodeid = bc.benefitcodeid
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(c.panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.panumber IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
            ),
            enrollee_utilization AS (
                SELECT 
                    c.enrollee_id,
                    c.code as procedurecode,
                    SUM(c.approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.code IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                GROUP BY c.enrollee_id, c.code
                
                UNION ALL
                
                SELECT 
                    pa.IID as enrollee_id,
                    pa.code as procedurecode,
                    SUM(pa.granted) as claims_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND CAST(pa.panumber AS INT64) NOT IN (SELECT panumber_int FROM claimed_pa_numbers)
                    AND pa.code IS NOT NULL
                GROUP BY pa.IID, pa.code
            ),
            benefit_mapping AS (
                SELECT 
                    u.enrollee_id,
                    u.procedurecode,
                    u.claims_cost,
                    b.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc
                FROM enrollee_utilization u
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" b ON u.procedurecode = b.procedurecode
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON b.benefitcodeid = bc.benefitcodeid
                WHERE b.benefitcodeid IS NOT NULL
            ),
            benefit_utilization AS (
                SELECT 
                    bm.enrollee_id,
                    bm.benefitcodeid,
                    bm.benefitdesc,
                    SUM(bm.claims_cost) as used_amount,
                    COUNT(DISTINCT bm.procedurecode) as used_count
                FROM benefit_mapping bm
                GROUP BY bm.enrollee_id, bm.benefitcodeid, bm.benefitdesc
            ),
            benefit_comparison AS (
                SELECT 
                    pb.enrollee_id,
                    pb.benefitcodeid,
                    pb.benefitdesc,
                    COALESCE(bu.used_amount, 0) as used_amount,
                    COALESCE(bu.used_count, 0) as used_count,
                    pb.maxlimit,
                    pb.countperannum,
                    CASE 
                        WHEN pb.maxlimit IS NOT NULL AND COALESCE(bu.used_amount, 0) > pb.maxlimit 
                        THEN COALESCE(bu.used_amount, 0) - pb.maxlimit
                        ELSE 0
                    END as monetary_overage
                FROM plan_benefits pb
                LEFT JOIN benefit_utilization bu ON pb.enrollee_id = bu.enrollee_id AND pb.benefitcodeid = bu.benefitcodeid
                WHERE (pb.maxlimit IS NOT NULL AND COALESCE(bu.used_amount, 0) > pb.maxlimit)
            )
            SELECT 
                enrollee_id,
                benefitdesc as benefit_name,
                maxlimit as limit_amount,
                used_amount,
                monetary_overage as overused_amount
            FROM benefit_comparison
            ORDER BY monetary_overage DESC
            """
            exceeded_violations_df = conn.execute(exceeded_violations_query).fetchdf()
            if not exceeded_violations_df.empty:
                for _, row in exceeded_violations_df.iterrows():
                    benefit_limit_stats['exceeded_violations'].append({
                        'enrollee_id': str(row['enrollee_id']) if pd.notna(row['enrollee_id']) else '',
                        'benefit_name': str(row['benefit_name']) if pd.notna(row['benefit_name']) else 'Unknown',
                        'limit': float(row['limit_amount']) if pd.notna(row['limit_amount']) else 0.0,
                        'amount_used': float(row['used_amount']) if pd.notna(row['used_amount']) else 0.0,
                        'overused': float(row['overused_amount']) if pd.notna(row['overused_amount']) else 0.0
                    })
            
            # Get detailed hit limit violations
            hit_limit_violations_query = f"""
            WITH client_enrollees AS (
                SELECT DISTINCT m.enrollee_id, m.memberid
                FROM "AI DRIVEN DATA"."MEMBERS" m
                WHERE m.groupid = {groupid}
                    AND m.iscurrent = 1
            ),
            enrollee_plans AS (
                SELECT 
                    ce.enrollee_id,
                    ce.memberid,
                    mp.planid
                FROM client_enrollees ce
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON ce.memberid = mp.memberid
                WHERE mp.iscurrent = 1
            ),
            plan_benefits AS (
                SELECT 
                    ep.enrollee_id,
                    pl.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc,
                    pl.maxlimit,
                    pl.countperannum
                FROM enrollee_plans ep
                INNER JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" pl ON ep.planid = pl.planid
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON pl.benefitcodeid = bc.benefitcodeid
            ),
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(c.panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.panumber IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
            ),
            enrollee_utilization AS (
                SELECT 
                    c.enrollee_id,
                    c.code as procedurecode,
                    SUM(c.approvedamount) as claims_cost
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN client_enrollees ce ON c.enrollee_id = ce.enrollee_id
                WHERE c.code IS NOT NULL
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                GROUP BY c.enrollee_id, c.code
                
                UNION ALL
                
                SELECT 
                    pa.IID as enrollee_id,
                    pa.code as procedurecode,
                    SUM(pa.granted) as claims_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND CAST(pa.panumber AS INT64) NOT IN (SELECT panumber_int FROM claimed_pa_numbers)
                    AND pa.code IS NOT NULL
                GROUP BY pa.IID, pa.code
            ),
            benefit_mapping AS (
                SELECT 
                    u.enrollee_id,
                    u.procedurecode,
                    u.claims_cost,
                    b.benefitcodeid,
                    bc.benefitcodedesc as benefitdesc
                FROM enrollee_utilization u
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" b ON u.procedurecode = b.procedurecode
                LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON b.benefitcodeid = bc.benefitcodeid
                WHERE b.benefitcodeid IS NOT NULL
            ),
            benefit_utilization AS (
                SELECT 
                    bm.enrollee_id,
                    bm.benefitcodeid,
                    bm.benefitdesc,
                    SUM(bm.claims_cost) as used_amount,
                    COUNT(DISTINCT bm.procedurecode) as used_count
                FROM benefit_mapping bm
                GROUP BY bm.enrollee_id, bm.benefitcodeid, bm.benefitdesc
            ),
            benefit_comparison AS (
                SELECT 
                    pb.enrollee_id,
                    pb.benefitcodeid,
                    pb.benefitdesc,
                    COALESCE(bu.used_amount, 0) as used_amount,
                    COALESCE(bu.used_count, 0) as used_count,
                    pb.maxlimit,
                    pb.countperannum
                FROM plan_benefits pb
                LEFT JOIN benefit_utilization bu ON pb.enrollee_id = bu.enrollee_id AND pb.benefitcodeid = bu.benefitcodeid
            )
            SELECT 
                enrollee_id,
                benefitdesc as benefit_name,
                maxlimit as limit_amount,
                used_amount
            FROM benefit_comparison
            WHERE (maxlimit IS NOT NULL AND used_amount = maxlimit AND used_amount > 0)
                AND (maxlimit IS NOT NULL AND used_amount <= maxlimit)  -- Exclude exceeded (only exact matches)
            ORDER BY used_amount DESC
            """
            hit_limit_violations_df = conn.execute(hit_limit_violations_query).fetchdf()
            if not hit_limit_violations_df.empty:
                for _, row in hit_limit_violations_df.iterrows():
                    benefit_limit_stats['hit_limit_violations'].append({
                        'enrollee_id': str(row['enrollee_id']) if pd.notna(row['enrollee_id']) else '',
                        'benefit_name': str(row['benefit_name']) if pd.notna(row['benefit_name']) else 'Unknown',
                        'limit': float(row['limit_amount']) if pd.notna(row['limit_amount']) else 0.0,
                        'amount_used': float(row['used_amount']) if pd.notna(row['used_amount']) else 0.0,
                        'overused': 0.0
                    })
            
            # Initialize variables
            banding_analysis = []
            plan_analysis = []
            
            # Banding Analysis - Replicate EXACT logic from complete_calculation_engine.py analyze_provider_bands
            # Get all providers used by this group with their bands, then group by band to get:
            # - Count of unique providers (hospitals) per band
            # - Total cost per band
            # - Percentage of total cost
            # Uses CLAIMS DATA + unclaimed PA, groups by provider first, then by band
            banding_analysis_query = f"""
            WITH claims_with_providers AS (
                SELECT
                    c.nhisproviderid,
                    p.providername,
                    p.bands,
                    SUM(c.approvedamount) as total_cost,
                    COUNT(*) as claim_count,
                    COUNT(DISTINCT c.enrollee_id) as unique_members
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(c.nhisproviderid AS VARCHAR)) AS INTEGER) = 
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(p.providerid AS VARCHAR)) AS INTEGER)
                WHERE CAST(c.nhisgroupid AS VARCHAR) = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                    AND c.approvedamount > 0
                    AND c.nhisproviderid IS NOT NULL
                    AND CAST(c.nhisproviderid AS VARCHAR) != ''
                GROUP BY c.nhisproviderid, p.providername, p.bands
            ),
            unclaimed_pa_with_providers AS (
                SELECT
                    pa.providerid as nhisproviderid,
                    p.providername,
                    p.bands,
                    SUM(pa.granted) as total_cost,
                    COUNT(*) as pa_count,
                    COUNT(DISTINCT pa.IID) as unique_members
                FROM "AI DRIVEN DATA"."PA DATA" pa
                LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(pa.providerid AS VARCHAR)) AS INTEGER) = 
                    TRY_CAST(TRIM(LEADING '0' FROM CAST(p.providerid AS VARCHAR)) AS INTEGER)
                LEFT JOIN (
                    SELECT DISTINCT CAST(panumber AS BIGINT) as panumber
                    FROM "AI DRIVEN DATA"."CLAIMS DATA"
                    WHERE CAST(nhisgroupid AS VARCHAR) = '{groupid}'
                        AND panumber IS NOT NULL
                        AND encounterdatefrom >= DATE '{contract_start_str}'
                        AND encounterdatefrom <= DATE '{contract_end_str}'
                ) claimed ON CAST(pa.panumber AS BIGINT) = claimed.panumber
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND pa.granted > 0
                    AND claimed.panumber IS NULL
                    AND pa.providerid IS NOT NULL
                    AND CAST(pa.providerid AS VARCHAR) != ''
                GROUP BY pa.providerid, p.providername, p.bands
            ),
            all_providers_with_bands AS (
                SELECT 
                    nhisproviderid,
                    providername,
                    COALESCE(bands, 'UNKNOWN') as bands,
                    SUM(total_cost) as total_cost
                FROM (
                    SELECT nhisproviderid, providername, bands, total_cost FROM claims_with_providers
                    UNION ALL
                    SELECT nhisproviderid, providername, bands, total_cost FROM unclaimed_pa_with_providers
                )
                GROUP BY nhisproviderid, providername, COALESCE(bands, 'UNKNOWN')
            ),
            band_summary AS (
                SELECT
                    UPPER(TRIM(bands)) as band,
                    COUNT(DISTINCT nhisproviderid) as hospital_count,
                    SUM(total_cost) as total_cost
                FROM all_providers_with_bands
                GROUP BY UPPER(TRIM(bands))
            )
            SELECT
                band,
                hospital_count,
                total_cost
            FROM band_summary
            ORDER BY total_cost DESC
            """
            
            try:
                banding_analysis_df = conn.execute(banding_analysis_query).fetchdf()
                logger.info(f"Banding analysis query returned {len(banding_analysis_df)} rows for {groupname}")
                banding_analysis = []
                if not banding_analysis_df.empty:
                    # Calculate total cost for percentage calculation
                    total_all_bands = banding_analysis_df['total_cost'].sum()
                    for _, row in banding_analysis_df.iterrows():
                        band_name = str(row['band']) if pd.notna(row['band']) else 'UNKNOWN'
                        total_cost = float(row['total_cost']) if pd.notna(row['total_cost']) else 0
                        hospital_count = int(row['hospital_count']) if pd.notna(row['hospital_count']) else 0
                        pct_of_total = (total_cost / total_all_bands * 100) if total_all_bands > 0 else 0
                        
                        banding_analysis.append({
                            'band': band_name,
                            'hospital_count': hospital_count,
                            'total_cost': total_cost,
                            'pct_of_total': round(pct_of_total, 2)
                        })
                else:
                    logger.warning(f"No banding analysis data returned for {groupname}")
            except Exception as e:
                logger.error(f"Error executing banding analysis query: {e}")
                import traceback
                traceback.print_exc()
                banding_analysis = []
            
            # Plan Analysis - Replicate EXACT logic from complete_calculation_engine.py analyze_plan_distribution
            # CRITICAL: Must filter MEMBER_PLANS to iscurrent = 1 to get most recent plan for each enrollee
            plan_analysis_query = f"""
            WITH claims_with_plans AS (
                SELECT
                    c.enrollee_id,
                    c.approvedamount,
                    m.memberid,
                    mp.planid,
                    p.planname
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                -- Join to MEMBER (all enrollees including terminated)
                INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
                -- Join to MEMBER_PLANS with iscurrent=1 (most recent plan assignment) - CRITICAL FILTER
                INNER JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON m.memberid = mp.memberid AND mp.iscurrent = 1
                -- Join to PLANS for planname (direct comparison handles DOUBLE to BIGINT conversion)
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p ON mp.planid = p.planid
                WHERE CAST(m.groupid AS VARCHAR) = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                    AND c.approvedamount > 0
                    AND mp.planid IS NOT NULL
            ),
            pa_issued AS (
                SELECT
                    pa.panumber,
                    pa.IID as enrollee_id,
                    pa.plancode,
                    pa.granted,
                    -- Try direct plancode match first
                    p1.planname as direct_planname,
                    p1.planid as direct_planid,
                    -- Try enrollee lookup as fallback (with groupid filter and iscurrent=1)
                    m.memberid,
                    mp.planid as plan_planid,
                    p2.planname as plan_planname
                FROM "AI DRIVEN DATA"."PA DATA" pa
                -- Direct plancode match
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p1 ON CAST(pa.plancode AS VARCHAR) = CAST(p1.plancode AS VARCHAR)
                -- Enrollee-based match (for PAs without plancode or when plancode doesn't match)
                LEFT JOIN "AI DRIVEN DATA"."MEMBER" m ON pa.IID = m.legacycode AND CAST(m.groupid AS VARCHAR) = '{groupid}'
                -- CRITICAL: Filter MEMBER_PLANS to iscurrent=1 to get most recent plan
                LEFT JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON m.memberid = mp.memberid AND mp.iscurrent = 1
                LEFT JOIN "AI DRIVEN DATA"."PLANS" p2 ON mp.planid = p2.planid
                WHERE pa.groupname = '{groupname.replace("'", "''")}'
                    AND pa.requestdate >= TIMESTAMP '{contract_start_str}'
                    AND pa.requestdate <= TIMESTAMP '{contract_end_str}'
                    AND pa.granted > 0
            ),
            claimed_pa AS (
                SELECT DISTINCT CAST(c.panumber AS BIGINT) as panumber
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                WHERE CAST(c.nhisgroupid AS VARCHAR) = '{groupid}'
                    AND c.encounterdatefrom >= DATE '{contract_start_str}'
                    AND c.encounterdatefrom <= DATE '{contract_end_str}'
                    AND c.panumber IS NOT NULL
            ),
            unclaimed_pa_with_plans AS (
                SELECT
                    pa.granted as approvedamount,
                    -- Use direct match if available, otherwise use plan-based match
                    COALESCE(pa.direct_planname, pa.plan_planname) as planname
                FROM pa_issued pa
                LEFT JOIN claimed_pa cp ON CAST(pa.panumber AS BIGINT) = cp.panumber
                WHERE cp.panumber IS NULL
                    AND COALESCE(pa.direct_planid, pa.plan_planid) IS NOT NULL
            ),
            claims_by_plan AS (
                SELECT
                    COALESCE(planname, 'Unknown Plan') as planname,
                    SUM(approvedamount) as claims_cost
                FROM claims_with_plans
                GROUP BY COALESCE(planname, 'Unknown Plan')
            ),
            pa_by_plan AS (
                SELECT
                    COALESCE(planname, 'Unknown Plan') as planname,
                    SUM(approvedamount) as pa_cost
                FROM unclaimed_pa_with_plans
                GROUP BY COALESCE(planname, 'Unknown Plan')
            ),
            all_plans AS (
                SELECT planname, claims_cost, 0.0 as pa_cost FROM claims_by_plan
                UNION ALL
                SELECT planname, 0.0 as claims_cost, pa_cost FROM pa_by_plan
            )
            SELECT
                planname,
                SUM(claims_cost) as claims_cost,
                SUM(pa_cost) as pa_cost,
                SUM(claims_cost + pa_cost) as total_cost
            FROM all_plans
            WHERE planname IS NOT NULL AND planname != 'Unknown Plan'
            GROUP BY planname
            ORDER BY total_cost DESC
            """
            
            try:
                plan_analysis_df = conn.execute(plan_analysis_query).fetchdf()
                logger.info(f"Plan analysis query returned {len(plan_analysis_df)} rows for {groupname}")
                plan_analysis = []
                if not plan_analysis_df.empty:
                    for _, row in plan_analysis_df.iterrows():
                        plan_analysis.append({
                            'planname': str(row['planname']) if pd.notna(row['planname']) else 'Unknown Plan',
                            'total_cost': float(row['total_cost']) if pd.notna(row['total_cost']) else 0,
                            'claims_cost': float(row['claims_cost']) if pd.notna(row['claims_cost']) else 0,
                            'pa_cost': float(row['pa_cost']) if pd.notna(row['pa_cost']) else 0
                        })
                else:
                    logger.warning(f"No plan analysis data returned for {groupname}")
            except Exception as e:
                logger.error(f"Error executing plan analysis query: {e}")
                import traceback
                traceback.print_exc()
                plan_analysis = []
            
            # Total medical cost = claims (encounterdatefrom in contract) + unclaimed PA (requestdate in contract)
            total_medical_cost = total_claims_encounter + total_unclaimed_pa
            
            return {
                'success': True,
                'client': {
                    'name': groupname,
                    'active_lives': active_lives,
                    'plan_count': plan_count,
                    'plan_names': plan_names.split(', ') if plan_names != 'N/A' else [],
                    'mlr': mlr_value,
                    'total_debit_amount': debit_amount or 0.0,
                    'total_cash_received': total_cash_received,
                    'total_cash_before_contract': total_cash_before_contract,
                    'cash_before_contract_details': cash_before_details,
                    'expected_cash': expected_cash,
                    'pmpm': pmpm_value,
                    'premium_pmpm': premium_pmpm_value,
                    'total_medical_cost': total_medical_cost,
                    'total_pa_cost': total_pa_cost,
                    'total_claims_encounter': total_claims_encounter,
                    'total_claims_submitted': total_claims_submitted,
                    'total_unclaimed_pa': total_unclaimed_pa,
                    'gender_split': gender_split,
                    'data_completeness': completeness,
                    'contract_period': {
                        'start': contract_start_str,
                        'end': contract_end_str
                    },
                    'monthly_costs': monthly_costs,
                    'top_providers': top_providers,
                    'top_procedures': top_procedures,
                    'top_enrollees': top_enrollees,
                    'benefit_limit_stats': benefit_limit_stats,
                    'banding_analysis': banding_analysis,
                    'plan_analysis': plan_analysis
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting client profile: {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }


    @staticmethod
    def get_top_enrollees_by_cost_contract_period(
        limit: int = 20,
        conn=None
    ) -> Dict[str, Any]:
        """
        Get top enrollees by total cost (Claims + Unclaimed PA) filtered by their client's contract period
        
        Args:
            limit: Number of top enrollees to return (default 20)
            conn: Database connection (optional, will create if not provided)
            
        Returns:
            Dictionary with top enrollees and statistics
        """
        try:
            from core.database import get_db_connection
            
            if conn is None:
                conn = get_db_connection()
            
            # Query to get top enrollees by cost within their client's contract period
            query = f"""
            WITH contract_periods AS (
                SELECT 
                    gc.groupid,
                    gc.groupname,
                    gc.startdate as contract_start,
                    gc.enddate as contract_end
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
                WHERE gc.iscurrent = 1
            ),
            -- Get claims cost per enrollee within contract period
            claims_by_enrollee AS (
                SELECT
                    c.enrollee_id,
                    g.groupname,
                    SUM(c.approvedamount) as claims_cost,
                    COUNT(DISTINCT c.panumber) as visit_count
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN "AI DRIVEN DATA"."GROUPS" g 
                    ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                INNER JOIN contract_periods cp 
                    ON g.groupname = cp.groupname
                WHERE CAST(c.encounterdatefrom AS DATE) >= CAST(cp.contract_start AS DATE)
                    AND CAST(c.encounterdatefrom AS DATE) <= CAST(cp.contract_end AS DATE)
                    AND c.approvedamount > 0
                GROUP BY c.enrollee_id, g.groupname
            ),
            -- Get claimed PA numbers (claims with encounterdatefrom in contract)
            claimed_pa_numbers AS (
                SELECT DISTINCT CAST(panumber AS INT64) as panumber_int
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN "AI DRIVEN DATA"."GROUPS" g 
                    ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                INNER JOIN contract_periods cp 
                    ON g.groupname = cp.groupname
                WHERE c.panumber IS NOT NULL
                    AND CAST(c.encounterdatefrom AS DATE) >= CAST(cp.contract_start AS DATE)
                    AND CAST(c.encounterdatefrom AS DATE) <= CAST(cp.contract_end AS DATE)
            ),
            -- Get unclaimed PA cost per enrollee within contract period
            unclaimed_pa_by_enrollee AS (
                SELECT
                    pa.IID as enrollee_id,
                    pa.groupname,
                    SUM(pa.granted) as pa_cost
                FROM "AI DRIVEN DATA"."PA DATA" pa
                INNER JOIN contract_periods cp 
                    ON pa.groupname = cp.groupname
                LEFT JOIN claimed_pa_numbers claimed 
                    ON CAST(pa.panumber AS INT64) = claimed.panumber_int
                WHERE CAST(pa.requestdate AS DATE) >= CAST(cp.contract_start AS DATE)
                    AND CAST(pa.requestdate AS DATE) <= CAST(cp.contract_end AS DATE)
                    AND pa.granted > 0
                    AND claimed.panumber_int IS NULL
                GROUP BY pa.IID, pa.groupname
            ),
            -- Combine claims and PA costs (aggregate to handle duplicates from UNION)
            enrollee_costs_combined AS (
                SELECT
                    COALESCE(c.enrollee_id, p.enrollee_id) as enrollee_id,
                    COALESCE(c.groupname, p.groupname) as groupname,
                    COALESCE(c.claims_cost, 0) as claims_cost,
                    COALESCE(p.pa_cost, 0) as pa_cost,
                    COALESCE(c.visit_count, 0) as visit_count
                FROM claims_by_enrollee c
                LEFT JOIN unclaimed_pa_by_enrollee p
                    ON c.enrollee_id = p.enrollee_id
                
                UNION ALL
                
                SELECT
                    p.enrollee_id,
                    p.groupname,
                    0 as claims_cost,
                    p.pa_cost,
                    0 as visit_count
                FROM unclaimed_pa_by_enrollee p
                LEFT JOIN claims_by_enrollee c
                    ON p.enrollee_id = c.enrollee_id
                WHERE c.enrollee_id IS NULL
            ),
            -- Aggregate to sum up costs for enrollees that appear in both
            enrollee_costs AS (
                SELECT
                    enrollee_id,
                    MAX(groupname) as groupname,
                    SUM(claims_cost) as claims_cost,
                    SUM(pa_cost) as pa_cost,
                    SUM(claims_cost) + SUM(pa_cost) as total_cost,
                    MAX(visit_count) as visit_count
                FROM enrollee_costs_combined
                GROUP BY enrollee_id
            ),
            -- Join with member info
            enrollee_with_info AS (
                SELECT
                    ec.enrollee_id,
                    ec.groupname,
                    ec.claims_cost,
                    ec.pa_cost,
                    ec.total_cost,
                    ec.visit_count,
                    COALESCE(m.firstname, '') as firstname,
                    COALESCE(m.lastname, '') as surname,
                    m.idnumber as nhislegacynumber
                FROM enrollee_costs ec
                LEFT JOIN "AI DRIVEN DATA"."MEMBER" m
                    ON ec.enrollee_id = m.idnumber
            )
            SELECT
                enrollee_id,
                nhislegacynumber,
                firstname,
                surname,
                groupname,
                claims_cost,
                pa_cost,
                total_cost,
                visit_count
            FROM enrollee_with_info
            ORDER BY total_cost DESC
            LIMIT {limit}
            """
            
            logger.info(f"Executing query for top {limit} enrollees by cost (contract period)")
            result_df = conn.execute(query).fetchdf()
            logger.info(f"Query returned {len(result_df)} rows")
            
            if result_df.empty:
                logger.warning("No enrollees found in contract period query")
                # Try a simpler test query to see if there's any data
                test_query = """
                SELECT COUNT(*) as contract_count
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
                WHERE iscurrent = 1
                """
                test_df = conn.execute(test_query).fetchdf()
                logger.info(f"Active contracts found: {test_df.iloc[0]['contract_count'] if not test_df.empty else 0}")
                
                return {
                    'success': True,
                    'top_enrollees': [],
                    'statistics': {
                        'total_enrollees': 0,
                        'total_cost': 0,
                        'average_cost': 0,
                        'limit': limit
                    }
                }
            
            top_enrollees = result_df.to_dict('records')
            logger.info(f"Processed {len(top_enrollees)} enrollees")
            
            # Calculate statistics
            total_cost = result_df['total_cost'].sum()
            avg_cost = result_df['total_cost'].mean() if len(result_df) > 0 else 0
            
            return {
                'success': True,
                'top_enrollees': top_enrollees,
                'statistics': {
                    'total_enrollees': len(result_df),
                    'total_cost': float(total_cost),
                    'average_cost': float(avg_cost),
                    'limit': limit
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting top enrollees by cost (contract period): {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }
    
    @staticmethod
    def get_top_enrollees_by_visits_contract_period(
        limit: int = 20,
        conn=None
    ) -> Dict[str, Any]:
        """
        Get top enrollees by visit count filtered by their client's contract period
        
        Args:
            limit: Number of top enrollees to return (default 20)
            conn: Database connection (optional, will create if not provided)
            
        Returns:
            Dictionary with top enrollees by visits
        """
        try:
            from core.database import get_db_connection
            
            if conn is None:
                conn = get_db_connection()
            
            # Query to get top enrollees by visits within their client's contract period
            query = f"""
            WITH contract_periods AS (
                SELECT 
                    gc.groupid,
                    gc.groupname,
                    gc.startdate as contract_start,
                    gc.enddate as contract_end
                FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
                WHERE gc.iscurrent = 1
            ),
            -- Get visit counts and costs per enrollee within contract period
            enrollee_visits AS (
                SELECT
                    c.enrollee_id,
                    g.groupname,
                    COUNT(DISTINCT c.panumber) as visit_count,
                    SUM(c.approvedamount) as claims_cost,
                    COUNT(*) as claim_count,
                    CASE 
                        WHEN COUNT(DISTINCT c.panumber) > 0 
                        THEN SUM(c.approvedamount) / COUNT(DISTINCT c.panumber)
                        ELSE 0
                    END as avg_cost_per_visit
                FROM "AI DRIVEN DATA"."CLAIMS DATA" c
                INNER JOIN "AI DRIVEN DATA"."GROUPS" g 
                    ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
                INNER JOIN contract_periods cp 
                    ON g.groupname = cp.groupname
                WHERE CAST(c.encounterdatefrom AS DATE) >= CAST(cp.contract_start AS DATE)
                    AND CAST(c.encounterdatefrom AS DATE) <= CAST(cp.contract_end AS DATE)
                    AND c.approvedamount > 0
                GROUP BY c.enrollee_id, g.groupname
            ),
            -- Join with member info
            enrollee_with_info AS (
                SELECT
                    ev.enrollee_id,
                    ev.groupname,
                    ev.visit_count,
                    ev.claims_cost,
                    ev.claim_count,
                    ev.avg_cost_per_visit,
                    COALESCE(m.firstname, '') as firstname,
                    COALESCE(m.lastname, '') as surname,
                    m.idnumber as nhislegacynumber
                FROM enrollee_visits ev
                LEFT JOIN "AI DRIVEN DATA"."MEMBER" m
                    ON ev.enrollee_id = m.idnumber
            )
            SELECT
                enrollee_id,
                nhislegacynumber,
                firstname,
                surname,
                groupname,
                visit_count,
                claims_cost as total_cost,
                claim_count,
                avg_cost_per_visit
            FROM enrollee_with_info
            ORDER BY visit_count DESC
            LIMIT {limit}
            """
            
            logger.info(f"Executing query for top {limit} enrollees by visits (contract period)")
            result_df = conn.execute(query).fetchdf()
            logger.info(f"Query returned {len(result_df)} rows")
            
            if result_df.empty:
                logger.warning("No enrollees found in contract period visits query")
                return {
                    'success': True,
                    'top_enrollees': [],
                    'statistics': {
                        'total_enrollees': 0,
                        'total_visits': 0,
                        'average_visits': 0,
                        'limit': limit
                    }
                }
            
            top_enrollees = result_df.to_dict('records')
            logger.info(f"Processed {len(top_enrollees)} enrollees")
            
            # Calculate statistics
            total_visits = result_df['visit_count'].sum()
            avg_visits = result_df['visit_count'].mean() if len(result_df) > 0 else 0
            
            return {
                'success': True,
                'top_enrollees': top_enrollees,
                'statistics': {
                    'total_enrollees': len(result_df),
                    'total_visits': int(total_visits),
                    'average_visits': float(avg_visits),
                    'limit': limit
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting top enrollees by visits (contract period): {e}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'error': str(e)
            }


# Global instance
enrollee_service = EnrolleeAnalyticsService()

__all__ = ["enrollee_service", "EnrolleeAnalyticsService"]
