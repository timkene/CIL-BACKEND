# KLAIRE - Complete Knowledge Base for Claude Desktop
# CLEARLINE INTERNATIONAL LIMITED - Data Model & Query Guide

**Company:** CLEARLINE INTERNATIONAL LIMITED  
**AI Analyst:** KLAIRE  
**Database:** DuckDB (`ai_driven_data.duckdb`)  
**Schema:** All tables are in `"AI DRIVEN DATA"` schema  
**Last Updated:** 2025-11-08

---

## ⚠️ CRITICAL: Database Schema & Querying Rules

1. **All tables are in the `"AI DRIVEN DATA"` schema**
   - Always use: `"AI DRIVEN DATA"."TABLE_NAME"`
   - Example: `SELECT * FROM "AI DRIVEN DATA"."CLAIMS DATA" LIMIT 10;`

2. **To list all tables:**
   ```sql
   SELECT table_name 
   FROM information_schema.tables 
   WHERE table_schema = 'AI DRIVEN DATA'
   ORDER BY table_name;
   ```

3. **Key columns for matching:**
   - `nhisgroupid` = group ID for claims (use this to match groups in CLAIMS DATA)
   - `nhisproviderid` = provider ID for claims (use this to get provider name)
   - `diagnosiscode` = diagnosis code (use this to get diagnosis description)
   - `panumber` = PA authorization code (link PA to Claims)
   - For MLR calculations: Use `encounterdatefrom` for claims (within client current contract); use `requestdate` for PA (within client current contract)

---

## Business Model

Clearline International Limited is a health insurance company (HMO) that:
- Partners with hospitals (providers)
- Takes premiums from customers (groups/clients)
- Provides health insurance services to enrollees
- Sells plans (individual or family) to clients

---

## Key Concepts

### Clients/Groups
- **Groups** = **Clients** = **groupname** (all refer to the same thing)
- Each client has a unique `groupid`
- Use `GROUPS` table to get client name from `groupid`
- Some tables already have `groupname` (like PA DATA), making it easier
- **For claims matching:** Use `nhisgroupid` in CLAIMS DATA to join with GROUPS

### Plans
- Plans can be **Individual** or **Family** (found in `GROUP_PLAN` table)
- Each plan has:
  - Cost per unit
  - Number of units sold (individual/family/both)
- Clients can have multiple plans (up to 10 plans per client)
- Plans are sold for **1 year periods**, then must be renewed
- `GROUP_CONTRACT` contains contract periods for each client
- Always use contract dates unless user specifies a different period

### Enrollees/Members
- **Enrollees** = **Members** = **Customers**
- IDs can appear as: `legacycode`, `enrollee_id`, `IID`, `memberid` (case insensitive)
- If you see both `memberid` and `legacycode` in same table, use them to map and get member info
- `MEMBERS` table contains all **active** enrollees with DOB, phone, email, gender, etc.
- `MEMBER` / raw `member` table (from MediCloud) contains **all members ever enrolled**, including terminated ones  
  - Use this when you see a claim or PA for an enrollee who is no longer active and you need historical info  
  - From here you can get original registration date, DOB, gender, contact details, etc.
- `MEMBER_COVERAGE` (raw coverage periods) contains all historical coverage records for members  
  - Use this to see **when coverage started and ended** for a member, including terminated coverage  
  - This complements `MEMBERS` / `ALL_ACTIVE_MEMBER`, which only shows current, non‑terminated coverage
- `MEMBER_PLAN` contains members and their plans - use `iscurrent = 1` for current plan
- One member can have multiple plans - always use `iscurrent = 1` for current data

---

## Providers & Tariffs

### Providers (Hospitals)
- `PROVIDERS` table contains all hospitals we partner with
- Each provider has unique `providerid`
- Use `providerid` or `nhisproviderid` to get provider name from `PROVIDERS`
- In CLAIMS DATA, use `nhisproviderid` to join with PROVIDERS

### Tariffs
- Tariffs are price lists containing procedures and agreed prices
- Each provider is mapped to a tariff (found in `PROVIDERS_TARIFF`)
- `TARIFF` table contains all tariffs with procedures and prices
- Use `tariffid` to get `tariffname` from `TARIFF` table
- Providers use their mapped tariff prices for transactions

---

## Pre-Authorization (PA) Process

### PA DATA Table
- When enrollee goes to hospital, hospital sends PA request
- Request contains: diagnosis and procedures to be performed
- Clearline gives authorization code called `panumber`
- `panumber` is unique to enrollee + date (in `requestdate` column)
- `granted` column contains the authorized amount
- `procedurecode` or `code` = unique alphanumeric code for each procedure
- Use `procedurecode` to get procedure name from `PROCEDURE DATA` table

### Diagnosis
- `TBPADIAGNOSIS` table contains all `panumber` and their diagnosis
- Use `TBPADIAGNOSIS` to get diagnosis for a `panumber`
- `DIAGNOSIS` table (from MediCloud) contains diagnosis codes and descriptions
- Use diagnosis code to get diagnosis description from `DIAGNOSIS` table

---

## Claims Process

### CLAIMS DATA Table
- After PA is given, hospital submits claim for vetting and payment
- **Two important dates:**
  - `encounterdatefrom` = date the encounter took place (usually same as PA `requestdate`, but not always)
  - `datesubmitted` = date the claim was submitted
- **For MLR calculations: Use `encounterdatefrom` for claims (filter by client current contract); use `requestdate` for PA**
- **Important:** Claims can exist WITHOUT `panumber` - this is normal!
  - Some authorization is done at hospital end to reduce delay
  - Rows without `panumber` are allowed and expected
- `approvedamount` = amount approved for payment
- `deniedamount` = amount denied from that transaction
- `chargeamount` = amount submitted by hospital
- `code` = procedure code used
- `diagnosiscode` = diagnosis code (use this to get diagnosis description from DIAGNOSIS table)
- **Key columns:**
  - `nhisgroupid` = group ID (use this to match with GROUPS.groupid)
  - `nhisproviderid` = provider ID (use this to match with PROVIDERS.providerid)
  - `panumber` = links to PA DATA (can be NULL)

---

## Benefits & Limits

### Benefits
- Benefits are classes/buckets of procedures
- Relationship: `BENEFITCODE_PROCEDURES` maps `procedurecode` → `benefitcodeid`
- `BENEFITCODES` contains benefit descriptions
- To get benefit for a procedure:
  1. Get `benefitcodeid` from `BENEFITCODE_PROCEDURES` using `procedurecode`
  2. Get benefit description from `BENEFITCODES` using `benefitcodeid`

### Plan Benefit Limits

#### PLANBENEFITCODE_LIMIT Table Structure
- **Purpose**: Defines limits for each plan × benefit combination
- **Key Columns**:
  - `planid`: Links to PLANS table
  - `benefitcodeid`: Links to BENEFITCODES table
  - `maxlimit`: Monetary limit (e.g., ₦200,000) - NULL if no monetary limit
  - `countperannum`: Count limit per year (e.g., 15 times) - NULL if no count limit
  - `countperweek`: Count limit per week - NULL if no weekly limit
  - `countperquarter`: Count limit per quarter - NULL if no quarterly limit
  - `countpertwoyears`: Count limit per two years - NULL if no two-year limit
  - `countperlifetime`: Count limit per lifetime - NULL if no lifetime limit
  - `daysallowed`: Days allowed for certain benefits (e.g., inpatient days)
  - `agelimit`: Age limit for benefit eligibility
  - `genderallowed`: Gender restriction for benefit
  - `timelimit`: Time-based limit
- **Important**: A benefit can have BOTH monetary and count limits, or just one, or neither (unlimited)

#### Limit Types
- **Monetary Limits** (`maxlimit`): Maximum amount that can be spent on a benefit per period
  - Example: ₦200,000 per year for "Outpatient Consultation"
  - Check: Sum all `approvedamount` (claims) + `granted` (unclaimed PA) for procedures in that benefit
- **Count Limits** (`countperannum`, `countperweek`, etc.): Maximum number of times a benefit can be used per period
  - Example: 15 times per year for "Laboratory Tests"
  - Check: Count all procedures in that benefit used by the member
- **Unlimited Benefits**: Benefits with NO limit defined in PLANBENEFITCODE_LIMIT
  - These represent exposure risks - members can use them without restriction
  - Important to identify for contract renewal negotiations

#### How to Check if Enrollee Exceeded Limits
1. **Get member's plan**: Use `MEMBER_PLAN` table with `iscurrent = 1` to get `planid` for the member
2. **Get member's utilization**: Combine claims (`CLAIMS DATA`) and unclaimed PA (`PA DATA`) for the contract period
3. **Map procedures to benefits**: Use `BENEFITCODE_PROCEDURES` to get `benefitcodeid` for each procedure
4. **Aggregate by benefit**: 
   - For monetary limits: SUM(`approvedamount` + `granted`) per benefit
   - For count limits: COUNT(*) per benefit
5. **Compare against limits**: Join with `PLANBENEFITCODE_LIMIT` using `planid` + `benefitcodeid`
6. **Flag overages**: 
   - Monetary overage = `total_spent - maxlimit` (if `total_spent > maxlimit`)
   - Count overage = `times_used - countperannum` (if `times_used > countperannum`)

#### Benefit Limit Analysis Use Cases

**1. Over-Limit Members (Fraud/Abuse Detection)**
- **Purpose**: Identify members who have exceeded their benefit limits
- **Risk Levels**:
  - 🔴 CRITICAL: Overage > 100% of limit (e.g., used ₦400,000 when limit is ₦200,000)
  - 🟡 MODERATE: Overage < 100% of limit
- **What to Report**:
  - Member name and enrollee_id
  - Benefit name that was exceeded
  - Limit type (MONETARY or COUNT)
  - Usage vs. limit (e.g., "Used ₦250,000 / Limit ₦200,000")
  - Overage amount (e.g., "₦50,000 OVER")
  - Risk level

**2. Unlimited Benefits (Exposure Risk)**
- **Purpose**: Identify benefits that are being used but have NO limits defined
- **Risk Levels**:
  - 🔴 HIGH EXPOSURE: >₦5M spent with no limit
  - 🟡 MODERATE EXPOSURE: >₦1M spent with no limit
  - ✅ LOW EXPOSURE: <₦1M spent
- **What to Report**:
  - Benefit name
  - Total times used
  - Total cost
  - Status: "UNLIMITED"
  - Risk level
  - Recommendation: "Recommend adding limits"

**3. Benefit Cost Breakdown**
- **Purpose**: Understand which benefits are driving costs for a company
- **What to Report**:
  - Benefit name
  - Number of unique members using the benefit
  - Total times used
  - Total cost (sum of claims + unclaimed PA)
  - Average cost per use
  - Percentage of total medical cost
- **Order**: By total cost DESC (top cost drivers first)

**4. High-Cost Members per Benefit**
- **Purpose**: Identify member concentration risk (few members driving high costs)
- **What to Report**:
  - Enrollee ID and member name
  - Total times used
  - Total cost per member
  - Percentage of company's total cost
  - Risk level (CRITICAL >₦10M, HIGH >₦5M, MODERATE otherwise)

#### Key Relationships for Limit Analysis
- `MEMBER.memberid` → `MEMBER_PLAN.memberid` (get member's plan)
- `MEMBER_PLAN.planid` → `PLANBENEFITCODE_LIMIT.planid` (get limits for plan)
- `CLAIMS DATA.code` / `PA DATA.code` → `BENEFITCODE_PROCEDURES.procedurecode` (map procedure to benefit)
- `BENEFITCODE_PROCEDURES.benefitcodeid` → `PLANBENEFITCODE_LIMIT.benefitcodeid` (get limit for benefit)
- `BENEFITCODE_PROCEDURES.benefitcodeid` → `BENEFITCODES.benefitcodeid` (get benefit name)

#### Important Notes for Limit Analysis
1. **Use contract period**: Always filter utilization by contract dates from `GROUP_CONTRACT`
2. **Include unclaimed PA**: For current contracts, include both claims AND unclaimed PA in utilization
3. **Member's current plan**: Always use `MEMBER_PLAN` with `iscurrent = 1` to get the correct plan
4. **Multiple plans**: A member can have multiple plans - check limits for each plan separately
5. **Case sensitivity**: Use `LOWER(TRIM())` when matching procedure codes between tables
6. **NULL handling**: A benefit with `maxlimit IS NULL` AND `countperannum IS NULL` is UNLIMITED
7. **Both limits**: A benefit can have both monetary AND count limits - check both separately

### Plans Mapping
- `PLANS` table maps `plancode` ↔ `planid`
- Use this when you see `plancode` in some tables and `planid` in others

---

## Coverage & Contracts

### GROUP_CONTRACT
- Contains contract periods for each client
- Contracts are always **1 year periods**
- Always use contract dates when doing client analysis unless user specifies otherwise
- Use `iscurrent = 1` for current contract, `iscurrent = 0` for previous contracts

### GROUP_COVERAGE
- Clients can pay in installments
- Coverage extends as payments are made
- Coverage periods change with payments
- **Important:** Coverage should NEVER be outside contract period
- When coverage hits 1 year, it restarts if client renews
- Use `GROUP_COVERAGE` to find clients that will be terminated this month by checking `enddate`

---

## Table Relationships Summary

### Key Relationships
- `PA DATA.panumber` → `CLAIMS DATA.panumber` (PA to claims linkage)
- `PA DATA.providerid` → `PROVIDERS.providerid` (PA to provider)
- `CLAIMS DATA.nhisgroupid` → `GROUPS.groupid` (Claims to groups) ⚠️ **Use nhisgroupid for claims**
- `CLAIMS DATA.nhisproviderid` → `PROVIDERS.providerid` (Claims to providers) ⚠️ **Use nhisproviderid for claims**
- `CLAIMS DATA.enrollee_id` → `MEMBERS.enrollee_id` (Claims to members)
- `MEMBERS.groupid` → `GROUPS.groupid` (Members to groups)
- `CLAIMS DATA.panumber` → `TBPADIAGNOSIS.panumber` (Claims to diagnoses)
- `PA DATA.code` → `PROCEDURE DATA.procedurecode` (PA to procedure details)
- `PROVIDERS_TARIFF.providerid` → `PROVIDERS.providerid` (Provider to tariff mapping)
- `PROVIDERS_TARIFF.tariffid` → `TARIFF.tariffid` (Tariff details)
- `BENEFITCODE_PROCEDURES.procedurecode` → `PROCEDURE DATA.procedurecode`
- `BENEFITCODE_PROCEDURES.benefitcodeid` → `BENEFITCODES.benefitcodeid`
- `PLANBENEFITCODE_LIMIT.planid` → `PLANS.planid`
- `PLANBENEFITCODE_LIMIT.benefitcodeid` → `BENEFITCODES.benefitcodeid`
- `MEMBER_PLAN.memberid` → `MEMBERS.memberid`
- `MEMBER_PLAN.planid` → `PLANS.planid`

---

## Important Rules

1. **Always use contract dates** from `GROUP_CONTRACT` for client analysis unless user specifies otherwise
2. **Use `iscurrent = 1`** for current data where applicable (MEMBER_PLAN, GROUP_COVERAGE, etc.)
3. **Claims without panumber are normal** - don't filter them out
4. **For MLR calculations: Use `encounterdatefrom` for claims (within client current contract) and `requestdate` for PA (within client current contract); MLR = (claims + unclaimed PA) / total debit cost**
5. **Enrollee IDs vary** - use `legacycode`, `enrollee_id`, `IID`, or `memberid` (case insensitive)
6. **One member can have multiple plans** - always use `iscurrent = 1`
7. **Coverage never exceeds contract period** - important for termination checks
8. **Use `nhisgroupid` for matching groups in CLAIMS DATA** (not `groupid`)
9. **Use `nhisproviderid` for matching providers in CLAIMS DATA** (not `providerid`)
10. **Always use schema prefix:** `"AI DRIVEN DATA"."TABLE_NAME"`

---

## Medical Loss Ratio (MLR)

### Definition
- **MLR** = Medical Loss Ratio
- **Calculation**: Percentage of money received by clients that was used for medical expenses
- **Formula**: MLR = (Total Medical Spending / Total Premium Received) × 100%

### Total Medical Spending Calculation
- **Total Medical Spending** = Claims + Unclaimed PA (both within client current contract)
  - **Claims**: Approved claims (`approvedamount` from CLAIMS DATA) where **`encounterdatefrom`** falls within the client's current contract period
    - **Date filtering:** Use `encounterdatefrom` (not `datesubmitted`) so MLR reflects when care occurred, not when the claim was submitted
    - **Group matching:** Use `nhisgroupid` to match with GROUPS
  - **Unclaimed PA**: PA with `granted` amounts that have not yet been claimed, where **`requestdate`** falls within the client's current contract period
    - PA requests that don't have a corresponding claim (check if `panumber` exists in CLAIMS DATA for claims with encounterdatefrom in contract)
    - **Only count for current/active contracts** (where `enddate >= today`)
- **MLR** = (Total Medical Spending / Total Debit Cost) × 100%

### MLR Calculation Period
- Can be calculated for:
  - **Current contract period**: Use GROUP_CONTRACT dates
  - **Stated period**: Use user-specified dates
- Compare against CLIENT_CASH_RECEIVED for the same period to get accurate MLR

### BV-MLR vs CASH-MLR
- **BV-MLR**: MLR calculated using Debit Note amount as premium
- **CASH-MLR**: MLR calculated using Cash Received amount as premium
- Both use the same medical spending (Claims + Unclaimed PA)

---

## Financial Tables

### Derived Tables

#### CLIENT_CASH_RECEIVED
- **Purpose**: Tracks cash received from each client with date
- **Use Case**: 
  - Compare cash received vs. claims paid + unclaimed PA for current contract or stated period
  - Calculate MLR: (Claims + Unclaimed PA) / Cash Received
- **Structure**: Contains `groupname`, `Date`, `Amount` (or `assets_amount`)
- **Matching Logic**: Matches transactions on code + refno + date

#### DEBIT_NOTE
- **Purpose**: Contains debit notes (premium invoices) for each client
- **Use Case**: Calculate BV-MLR using debit note amounts as premium
- **Structure**: Contains `CompanyName` (groupname), `From` (date), `Amount`
- **Filtering**: Exclude rows with "tpa" in Description

#### SALARY_AND_PALLIATIVE
- **Purpose**: Contains all salary paid month by month to Clearline staff
- **Structure**: Monthly salary transactions
- **Matching Logic**: Matches on refno + date (no company code needed - this is internal)

#### EXPENSE_AND_COMMISSION
- **Purpose**: Contains all Clearline operational expenses including commissions
- **Structure**: Cost of each type of expense (operational costs, commissions, etc.)
- **Matching Logic**: Matches on gldesc + date

#### DEBIT_NOTE_ACCRUED
- **Purpose**: Contains all debit cost spread into months (allocated monthly amounts)
- **Structure**: Shows amount allocated per month to be spent on each company from start date to end date
- **Use Case**: 
  - Get monthly MLR by comparing monthly allocated amounts vs. monthly medical spending (claims + unclaimed PA)
  - Track monthly allocation vs. actual spending per client
- **Matching Logic**: Matches on code + date

### FIN_GL Raw Tables
- **FIN_GL_2023_RAW**: GL data from Excel (sheet 1)
- **FIN_GL_2024_RAW**: GL data from Excel (sheet 2)
- **FIN_GL_2025_RAW**: GL data from EACCOUNT database

---

## Column Naming Notes

- `legacycode`, `enrollee_id`, `IID` = enrollee ID (case insensitive)
- `memberid` = member ID
- `panumber` = PA authorization code
- `groupid` = client/group unique ID
- `nhisgroupid` = group ID in claims (use this for matching groups in CLAIMS DATA)
- `providerid` = hospital/provider unique ID
- `nhisproviderid` = provider ID in claims (use this for matching providers in CLAIMS DATA)
- `tariffid` = tariff unique ID
- `procedurecode` or `code` = procedure unique code
- `benefitcodeid` = benefit unique ID
- `planid` = plan unique ID
- `plancode` = plan code (can map to planid via PLANS table)
- `diagnosiscode` = diagnosis code (use this to get diagnosis description)

---

## Data Updates

- All tables update via `auto_update_database.py`
- Derived tables rebuild automatically after source tables update
- GROUP_CONTRACT must be loaded for accurate contract date queries
- Always verify table exists before querying

---

## Common Query Patterns

### 1. Medical History Analysis (Enrollee Medical Records)

**User Query Pattern:**
- "show me all medical record for [ENROLLEE_ID] for the last [PERIOD]"
- "show me all medical history for [ENROLLEE_ID] for the past [PERIOD]"

**What to Provide:**
1. **All Prior Authorizations (PAs)** with:
   - PA number, request date, status, provider name
   - All procedures in each PA (procedure code, description, requested amount, granted amount)
   - All diagnoses for each PA (diagnosis code, description)
   
2. **All Claims** with:
   - Claim number, submission date, encounter date
   - Procedures claimed (procedure code, charge amount, approved amount, denied amount)
   - Diagnoses associated with claims
   
3. **Medical Trends & Insights:**
   - **Frequent conditions**: Count occurrences of each diagnosis (e.g., "Malaria treated 5 times")
   - **Frequent procedures**: Count occurrences of each procedure type
   - **Antibiotic usage**: Flag frequent antibiotic prescriptions (amoxicillin, azithromycin, ceftriaxone, ciprofloxacin, metronidazole, etc.)
   - **Antimalaria usage**: Flag frequent antimalaria drug usage (artemether, lumefantrine, artesunate, etc.)
   - **Pain management**: Track pain medication usage (paracetamol, diclofenac, ibuprofen, tramadol, etc.)
   - **Chronic conditions**: Identify chronic disease management (hypertension, diabetes, asthma, COPD, heart conditions)
   - **Surgical procedures**: Identify surgeries vs. medical treatments
   - **Dental procedures**: Track dental visits and procedures
   - **Provider patterns**: Which hospitals are used most frequently
   - **Cost trends**: Total costs over time, average cost per visit
   - **Gaps in care**: Missing follow-ups, incomplete treatments
   - **Red flags**: Co-infections, severe conditions, unusual patterns

**Key Tables:**
- `PA DATA` - Prior authorizations (use `IID`, `enrollee_id`, or `memberid` to find member)
- `TBPADIAGNOSIS` - Diagnoses for each PA (link via `panumber`)
- `DIAGNOSIS` - Diagnosis descriptions (link via diagnosis code)
- `PROCEDURE DATA` - Procedure descriptions (link via procedure code)
- `CLAIMS DATA` - Claims submitted (link via `panumber` or `enrollee_id`)
- `PROVIDERS` - Hospital/provider names (link via `providerid` or `nhisproviderid`)

**Date Filtering:**
- Use `requestdate` from PA DATA for PA filtering
- Use `datesubmitted` and `encounterdatefrom` from CLAIMS DATA for claims filtering
- Support periods: "last 3 months", "last 6 months", "last 1 year", "last 2 years", or specific date ranges

### 2. MLR Comparison Between Contract Periods

**User Query Pattern:**
- "compare [COMPANY_NAME] mlr between last contract and this one"
- "compare [COMPANY_NAME] mlr between last contract and current contract"
- "why is [COMPANY_NAME] mlr good/bad this year compared to last year"

**What to Provide:**

1. **MLR Comparison:**
   - Last contract MLR vs. Current contract MLR
   - Premium received (cash received) for both periods
   - Medical spending (claims + unclaimed PA) for both periods
   - MLR percentage change and direction (improving or worsening)

2. **Hospital/Provider Analysis:**
   - **New hospitals**: Hospitals used in current contract but not in last contract
   - **Dropped hospitals**: Hospitals used in last contract but not in current contract
   - **Hospital cost changes**: Compare costs per hospital between periods
   - **Top cost drivers**: Which hospitals are driving the MLR change (good or bad)
   - **Hospital utilization**: Number of visits/cases per hospital in each period
   - **Average cost per visit**: Compare hospital pricing between periods

3. **Diagnosis Analysis:**
   - **New diagnoses**: Diagnoses appearing in current contract but not in last contract
   - **Diagnosis pattern changes**: Shifts in diagnosis frequency (e.g., more malaria, more surgeries)
   - **Top cost diagnoses**: Which diagnoses are costing most in each period
   - **Severity changes**: More severe conditions in current vs. last contract

4. **Procedure Analysis:**
   - **Surgical procedures**: More/fewer surgeries in current contract
   - **Dental procedures**: More/fewer dental visits in current contract
   - **Specialty procedures**: Changes in specialty care (cardiology, orthopedics, etc.)
   - **Procedure cost changes**: Price changes for common procedures

5. **Utilization Analysis:**
   - **Total cases**: Number of PAs/claims in each period
   - **Average cost per case**: Compare between periods
   - **Member utilization**: More/fewer members using services
   - **Frequency patterns**: More frequent visits per member

6. **Root Cause Summary:**
   - Summarize the main drivers of MLR change
   - Identify if it's hospital-related, diagnosis-related, procedure-related, or utilization-related
   - Highlight significant findings (e.g., "New expensive hospital added", "More surgeries this year", "Malaria cases doubled")

**Key Tables:**
- `GROUP_CONTRACT` - Contract periods (use `iscurrent = 1` for current, `iscurrent = 0` for last contract)
- `CLIENT_CASH_RECEIVED` - Premium received (filter by contract dates)
- `CLAIMS DATA` - Claims (filter by `datesubmitted` within contract dates, use `nhisgroupid` for group matching)
- `PA DATA` - Prior authorizations (filter by `requestdate` within contract dates)
- `PROVIDERS` - Hospital/provider information
- `TBPADIAGNOSIS` - Diagnoses for analysis
- `DIAGNOSIS` - Diagnosis descriptions
- `PROCEDURE DATA` - Procedure descriptions

**Contract Period Logic:**
- Get current contract: `SELECT * FROM "AI DRIVEN DATA"."GROUP_CONTRACT" WHERE groupname = '[COMPANY_NAME]' AND iscurrent = 1`
- Get last contract: `SELECT * FROM "AI DRIVEN DATA"."GROUP_CONTRACT" WHERE groupname = '[COMPANY_NAME]' AND iscurrent = 0 ORDER BY enddate DESC LIMIT 1`
- Use `startdate` and `enddate` from contracts to filter all financial and medical data

**MLR Calculation:**
- MLR = (Total Medical Spending / Total Premium Received) × 100%
- Total Medical Spending = Claims (approvedamount) + Unclaimed PA (granted amount where panumber not in CLAIMS DATA)
- Total Premium Received = Sum of CLIENT_CASH_RECEIVED.Amount (or assets_amount) for the contract period
- **For previous contracts:** Use only claims (unclaimed PA won't be claimed anymore)
- **For current contracts:** Use claims + unclaimed PA

**Important Notes:**
- Always compare like-for-like periods (full contract periods, not partial)
- Consider seasonality if comparing different months
- Look for outliers (single large claims, new expensive providers)
- Consider member count changes between contracts

---

## SQL Query Templates

### Template 1: Medical History Analysis for Enrollee

**Purpose:** Get complete medical history including procedures, diagnoses, and medical trends for a specific enrollee over a specified period.

**Parameters:**
- `enrollee_id`: Enrollee ID (can be IID, enrollee_id, memberid, or legacycode)
- `period_start`: Start date (e.g., '2025-08-01' or calculated from "last 6 months")
- `period_end`: End date (e.g., '2025-11-06' or CURRENT_DATE)

**Query Structure:**

```sql
-- Step 1: Get all PAs for the enrollee in the period
WITH enrollee_pas AS (
    SELECT DISTINCT 
        pa.panumber,
        pa.requestdate,
        pa.pastatus,
        pa.providerid,
        p.providername,
        pa.groupname
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE (pa.IID = '{enrollee_id}' 
           OR pa.enrollee_id = '{enrollee_id}'
           OR pa.memberid = '{enrollee_id}')
      AND pa.requestdate >= '{period_start}'
      AND pa.requestdate <= '{period_end}'
),

-- Step 2: Get all diagnoses for these PAs
enrollee_diagnoses AS (
    SELECT 
        td.panumber,
        td.code as diagnosiscode,
        td."desc" as diagnosisdesc,
        d.diagnosisdesc as full_description,
        pa.requestdate
    FROM "AI DRIVEN DATA"."TBPADIAGNOSIS" td
    INNER JOIN enrollee_pas pa ON CAST(td.panumber AS VARCHAR) = CAST(pa.panumber AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON td.code = d.diagnosiscode
),

-- Step 3: Get all procedures for these PAs
enrollee_procedures AS (
    SELECT 
        pa.panumber,
        pa.requestdate,
        pa.code as procedurecode,
        pd.proceduredesc,
        pa.requested,
        pa.granted,
        pa.benefitcode,
        pa.pastatus,
        p.providername
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN enrollee_pas ep ON pa.panumber = ep.panumber
    LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON pa.code = pd.procedurecode
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
),

-- Step 4: Get all claims for this enrollee
enrollee_claims AS (
    SELECT 
        c.panumber,
        c.datesubmitted,
        c.encounterdatefrom,
        c.code as procedurecode,
        c.chargeamount,
        c.approvedamount,
        c.deniedamount,
        c.diagnosiscode,
        p.providername
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON CAST(c.nhisproviderid AS VARCHAR) = CAST(p.providerid AS VARCHAR)
    WHERE (c.enrollee_id = '{enrollee_id}' 
           OR CAST(c.panumber AS VARCHAR) IN (SELECT panumber FROM enrollee_pas))
      AND (c.datesubmitted >= '{period_start}' OR c.encounterdatefrom >= '{period_start}')
      AND (c.datesubmitted <= '{period_end}' OR c.encounterdatefrom <= '{period_end}')
)

-- Main Results
SELECT 
    'PA' as record_type,
    ep.panumber,
    ep.requestdate as date,
    ep.providername,
    ep.pastatus,
    NULL as diagnosiscode,
    NULL as diagnosisdesc,
    proc.procedurecode,
    proc.proceduredesc,
    proc.requested,
    proc.granted,
    NULL as chargeamount,
    NULL as approvedamount,
    NULL as deniedamount
FROM enrollee_pas ep
LEFT JOIN enrollee_procedures proc ON ep.panumber = proc.panumber
UNION ALL
SELECT 
    'CLAIM' as record_type,
    ec.panumber,
    ec.datesubmitted as date,
    ec.providername,
    NULL as pastatus,
    ec.diagnosiscode,
    d.diagnosisdesc,
    ec.procedurecode,
    pd.proceduredesc,
    NULL as requested,
    NULL as granted,
    ec.chargeamount,
    ec.approvedamount,
    ec.deniedamount
FROM enrollee_claims ec
LEFT JOIN "AI DRIVEN DATA"."DIAGNOSIS" d ON ec.diagnosiscode = d.diagnosiscode
LEFT JOIN "AI DRIVEN DATA"."PROCEDURE DATA" pd ON ec.procedurecode = pd.procedurecode
ORDER BY date DESC;
```

**Medical Trend Analysis Queries:**

```sql
-- Diagnosis Frequency
SELECT 
    ed.diagnosiscode,
    ed.diagnosisdesc,
    COUNT(*) as frequency,
    MIN(ed.requestdate) as first_occurrence,
    MAX(ed.requestdate) as last_occurrence
FROM enrollee_diagnoses ed
GROUP BY ed.diagnosiscode, ed.diagnosisdesc
ORDER BY frequency DESC;

-- Procedure Frequency
SELECT 
    proc.procedurecode,
    proc.proceduredesc,
    COUNT(*) as frequency,
    SUM(proc.granted) as total_cost,
    AVG(proc.granted) as avg_cost
FROM enrollee_procedures proc
GROUP BY proc.procedurecode, proc.proceduredesc
ORDER BY frequency DESC;

-- Provider Usage Pattern
SELECT 
    ep.providername,
    COUNT(DISTINCT ep.panumber) as visit_count,
    SUM(proc.granted) as total_cost
FROM enrollee_pas ep
LEFT JOIN enrollee_procedures proc ON ep.panumber = proc.panumber
GROUP BY ep.providername
ORDER BY visit_count DESC;
```

---

### Template 2: MLR Comparison Between Contract Periods

**Purpose:** Compare a company's MLR between last contract and current contract, identifying drivers of change.

**Parameters:**
- `company_name`: Company/group name (e.g., 'CDK INDUSTRIES')
- Current contract: `iscurrent = 1` in GROUP_CONTRACT
- Last contract: `iscurrent = 0` in GROUP_CONTRACT, ordered by enddate DESC LIMIT 1

**Query Structure:**

```sql
-- Step 1: Get contract periods
WITH contracts AS (
    SELECT 
        groupname,
        groupid,
        startdate,
        enddate,
        iscurrent,
        ROW_NUMBER() OVER (PARTITION BY groupname, iscurrent ORDER BY enddate DESC) as rn
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT"
    WHERE groupname = '{company_name}'
      AND iscurrent IN (0, 1)
),
current_contract AS (
    SELECT * FROM contracts WHERE iscurrent = 1 AND rn = 1
),
last_contract AS (
    SELECT * FROM contracts WHERE iscurrent = 0 AND rn = 1
),

-- Step 2: Get premium received (cash received)
current_premium AS (
    SELECT SUM(ABS(Amount)) as total_premium
    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
    WHERE groupname = '{company_name}'
      AND Date >= (SELECT startdate FROM current_contract)
      AND Date <= (SELECT enddate FROM current_contract)
),
last_premium AS (
    SELECT SUM(ABS(Amount)) as total_premium
    FROM "AI DRIVEN DATA"."CLIENT_CASH_RECEIVED"
    WHERE groupname = '{company_name}'
      AND Date >= (SELECT startdate FROM last_contract)
      AND Date <= (SELECT enddate FROM last_contract)
),

-- Step 3: Get medical spending (claims + unclaimed PA)
-- For current contract: use claims + unclaimed PA
-- For last contract: use only claims (unclaimed PA won't be claimed anymore)
current_claims AS (
    SELECT SUM(approvedamount) as total_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."GROUPS" g ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
    WHERE g.groupname = '{company_name}'
      AND c.datesubmitted >= (SELECT startdate FROM current_contract)
      AND c.datesubmitted <= (SELECT enddate FROM current_contract)
      AND c.nhisgroupid IS NOT NULL
),
current_unclaimed_pa AS (
    SELECT SUM(pa.granted) as total_unclaimed
    FROM "AI DRIVEN DATA"."PA DATA" pa
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
      AND CAST(pa.panumber AS BIGINT) NOT IN (
          SELECT DISTINCT CAST(panumber AS BIGINT) 
          FROM "AI DRIVEN DATA"."CLAIMS DATA"
          WHERE panumber IS NOT NULL
      )
),
last_claims AS (
    SELECT SUM(approvedamount) as total_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."GROUPS" g ON CAST(c.nhisgroupid AS VARCHAR) = CAST(g.groupid AS VARCHAR)
    WHERE g.groupname = '{company_name}'
      AND c.datesubmitted >= (SELECT startdate FROM last_contract)
      AND c.datesubmitted <= (SELECT enddate FROM last_contract)
      AND c.nhisgroupid IS NOT NULL
)

-- MLR Comparison
SELECT 
    'Current Contract' as period,
    (SELECT startdate FROM current_contract) as start_date,
    (SELECT enddate FROM current_contract) as end_date,
    (SELECT total_premium FROM current_premium) as premium_received,
    (SELECT total_claims FROM current_claims) + COALESCE((SELECT total_unclaimed FROM current_unclaimed_pa), 0) as medical_spending,
    ((SELECT total_claims FROM current_claims) + COALESCE((SELECT total_unclaimed FROM current_unclaimed_pa), 0)) / 
    NULLIF((SELECT total_premium FROM current_premium), 0) * 100 as mlr_percentage
UNION ALL
SELECT 
    'Last Contract' as period,
    (SELECT startdate FROM last_contract) as start_date,
    (SELECT enddate FROM last_contract) as end_date,
    (SELECT total_premium FROM last_premium) as premium_received,
    (SELECT total_claims FROM last_claims) as medical_spending,
    (SELECT total_claims FROM last_claims) / 
    NULLIF((SELECT total_premium FROM last_premium), 0) * 100 as mlr_percentage;
```

**Hospital Comparison:**

```sql
-- Hospitals used in each period
WITH current_hospitals AS (
    SELECT DISTINCT 
        p.providerid,
        p.providername,
        COUNT(DISTINCT pa.panumber) as case_count,
        SUM(pa.granted) as total_cost,
        AVG(pa.granted) as avg_cost_per_case
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM current_contract)
      AND pa.requestdate <= (SELECT enddate FROM current_contract)
    GROUP BY p.providerid, p.providername
),
last_hospitals AS (
    SELECT DISTINCT 
        p.providerid,
        p.providername,
        COUNT(DISTINCT pa.panumber) as case_count,
        SUM(pa.granted) as total_cost,
        AVG(pa.granted) as avg_cost_per_case
    FROM "AI DRIVEN DATA"."PA DATA" pa
    LEFT JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
    WHERE pa.groupname = '{company_name}'
      AND pa.requestdate >= (SELECT startdate FROM last_contract)
      AND pa.requestdate <= (SELECT enddate FROM last_contract)
    GROUP BY p.providerid, p.providername
)

-- New hospitals (in current but not in last)
SELECT 
    'NEW HOSPITAL' as change_type,
    ch.providername,
    ch.case_count,
    ch.total_cost,
    ch.avg_cost_per_case,
    NULL as last_period_cost,
    NULL as cost_change
FROM current_hospitals ch
LEFT JOIN last_hospitals lh ON ch.providerid = lh.providerid
WHERE lh.providerid IS NULL

UNION ALL

-- Dropped hospitals (in last but not in current)
SELECT 
    'DROPPED HOSPITAL' as change_type,
    lh.providername,
    NULL as case_count,
    NULL as total_cost,
    NULL as avg_cost_per_case,
    lh.total_cost as last_period_cost,
    NULL as cost_change
FROM last_hospitals lh
LEFT JOIN current_hospitals ch ON lh.providerid = ch.providerid
WHERE ch.providerid IS NULL

UNION ALL

-- Hospitals with cost changes
SELECT 
    'COST CHANGE' as change_type,
    ch.providername,
    ch.case_count,
    ch.total_cost,
    ch.avg_cost_per_case,
    lh.total_cost as last_period_cost,
    ch.total_cost - lh.total_cost as cost_change
FROM current_hospitals ch
INNER JOIN last_hospitals lh ON ch.providerid = lh.providerid
WHERE ABS(ch.total_cost - lh.total_cost) > 100000  -- Significant change threshold
ORDER BY cost_change DESC;
```

---

### Template 3: Complete Company Analysis - MLR + Benefit Limit Analysis

**Purpose:** Comprehensive analysis for contract renewal negotiations including MLR, benefit cost breakdown, over-limit members, unlimited benefits, and high-cost members.

**Parameters:**
- `company_name`: Company/group name (e.g., 'ARIK AIR LIMITED')
- Current contract: `iscurrent = 1` in GROUP_CONTRACT

**This analysis provides 5 sections:**

1. **MLR Analysis** - Profitability overview
2. **Benefit Cost Breakdown** - What's driving costs
3. **Over-Limit Members** - Fraud/abuse risks
4. **Unlimited Benefits** - Exposure risks
5. **Top High-Cost Members** - Member concentration risk

**Query Structure:**

```sql
-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 1: MLR ANALYSIS (Financial Overview)
-- ═══════════════════════════════════════════════════════════════════════════

WITH company_contract AS (
    SELECT 
        gc.groupname,
        gc.startdate,
        gc.enddate,
        g.groupid
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
    LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON gc.groupname = g.groupname
    WHERE gc.groupname = '{company_name}'
        AND gc.iscurrent = 1
),

premium_data AS (
    -- Get debit notes (billed amount)
    SELECT 
        cc.groupname,
        SUM(dn.Amount) as total_debit
    FROM "AI DRIVEN DATA"."DEBIT_NOTE" dn
    INNER JOIN company_contract cc ON dn.CompanyName = cc.groupname
    WHERE dn."From" >= cc.startdate
        AND dn."From" <= cc.enddate
        AND NOT LOWER(dn.Description) LIKE '%tpa%'
    GROUP BY cc.groupname
),

claims_cost AS (
    -- Get total claims cost
    SELECT 
        cc.groupname,
        SUM(c.approvedamount) as total_claims
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    WHERE c.datesubmitted >= cc.startdate
        AND c.datesubmitted <= cc.enddate
        AND c.approvedamount > 0
    GROUP BY cc.groupname
),

unclaimed_pa AS (
    -- Get unclaimed PA (future liability)
    SELECT 
        cc.groupname,
        SUM(pa.granted) as unclaimed_pa_amount
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN company_contract cc ON pa.groupname = cc.groupname
    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
    WHERE pa.requestdate >= cc.startdate
        AND pa.requestdate <= cc.enddate
        AND pa.granted > 0
        AND c.panumber IS NULL
    GROUP BY cc.groupname
),

mlr_summary AS (
    SELECT 
        cc.groupname,
        cc.startdate,
        cc.enddate,
        pd.total_debit,
        COALESCE(cls.total_claims, 0) as claims_amount,
        COALESCE(upa.unclaimed_pa_amount, 0) as unclaimed_pa,
        COALESCE(cls.total_claims, 0) + COALESCE(upa.unclaimed_pa_amount, 0) as total_medical_cost,
        pd.total_debit * 0.10 as commission,
        -- MLR calculation
        (COALESCE(cls.total_claims, 0) + COALESCE(upa.unclaimed_pa_amount, 0) + pd.total_debit * 0.10) / pd.total_debit * 100 as mlr_pct,
        -- Months remaining
        EXTRACT(YEAR FROM cc.enddate) * 12 + EXTRACT(MONTH FROM cc.enddate) - 
        (EXTRACT(YEAR FROM CURRENT_DATE) * 12 + EXTRACT(MONTH FROM CURRENT_DATE)) as months_remaining
    FROM company_contract cc
    LEFT JOIN premium_data pd ON cc.groupname = pd.groupname
    LEFT JOIN claims_cost cls ON cc.groupname = cls.groupname
    LEFT JOIN unclaimed_pa upa ON cc.groupname = upa.groupname
)
SELECT 
    '═══ SECTION 1: MLR ANALYSIS ═══' as section,
    groupname as company,
    CONCAT(TO_CHAR(startdate, 'YYYY-MM-DD'), ' to ', TO_CHAR(enddate, 'YYYY-MM-DD')) as contract_period,
    CONCAT('₦', TO_CHAR(total_debit, '999,999,999')) as premium_billed,
    CONCAT('₦', TO_CHAR(claims_amount, '999,999,999')) as claims_paid,
    CONCAT('₦', TO_CHAR(unclaimed_pa, '999,999,999')) as unclaimed_pa,
    CONCAT('₦', TO_CHAR(total_medical_cost, '999,999,999')) as total_medical_cost,
    CONCAT('₦', TO_CHAR(commission, '999,999,999')) as commission_10pct,
    CONCAT(ROUND(mlr_pct, 1), '%') as mlr_percentage,
    months_remaining as months_to_renewal,
    CASE 
        WHEN mlr_pct > 90 THEN '🔴 LOSS-MAKING (Increase 20%+)'
        WHEN mlr_pct > 80 THEN '🟡 HIGH RISK (Increase 10-15%)'
        WHEN mlr_pct > 75 THEN '⚠️  BREAK-EVEN (Monitor closely)'
        WHEN mlr_pct > 65 THEN '✅ PROFITABLE (Maintain premium)'
        ELSE '💰 HIGHLY PROFITABLE (Consider decrease?)'
    END as status
FROM mlr_summary;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 2: BENEFIT COST BREAKDOWN
-- ═══════════════════════════════════════════════════════════════════════════

WITH company_contract AS (
    SELECT 
        gc.groupname,
        gc.startdate,
        gc.enddate,
        g.groupid
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
    LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON gc.groupname = g.groupname
    WHERE gc.groupname = '{company_name}'
        AND gc.iscurrent = 1
),

all_utilization AS (
    -- Claims
    SELECT 
        c.enrollee_id,
        c.code as procedurecode,
        c.approvedamount as amount
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    WHERE c.datesubmitted >= cc.startdate
        AND c.datesubmitted <= cc.enddate
        AND c.approvedamount > 0
    
    UNION ALL
    
    -- Unclaimed PA
    SELECT 
        pa.IID as enrollee_id,
        pa.code as procedurecode,
        pa.granted as amount
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN company_contract cc ON pa.groupname = cc.groupname
    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
    WHERE pa.requestdate >= cc.startdate
        AND pa.requestdate <= cc.enddate
        AND pa.granted > 0
        AND c.panumber IS NULL
),

benefit_costs AS (
    SELECT 
        COALESCE(bc.benefitcodedesc, 'Unmapped Procedures') as benefit_name,
        bp.benefitcodeid,
        COUNT(DISTINCT u.enrollee_id) as unique_members,
        COUNT(*) as total_uses,
        ROUND(SUM(u.amount), 2) as total_cost,
        ROUND(AVG(u.amount), 2) as avg_cost_per_use
    FROM all_utilization u
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bp ON 
        LOWER(TRIM(u.procedurecode)) = LOWER(TRIM(bp.procedurecode))
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON bp.benefitcodeid = bc.benefitcodeid
    GROUP BY bp.benefitcodeid, bc.benefitcodedesc
)
SELECT 
    '═══ SECTION 2: BENEFIT COST BREAKDOWN ═══' as section,
    benefit_name,
    unique_members as members_using,
    total_uses as times_used,
    CONCAT('₦', TO_CHAR(total_cost, '999,999,999')) as total_cost,
    CONCAT('₦', TO_CHAR(avg_cost_per_use, '999,999')) as avg_per_use,
    CONCAT(ROUND(total_cost / SUM(total_cost) OVER () * 100, 1), '%') as pct_of_total_cost
FROM benefit_costs
ORDER BY total_cost DESC
LIMIT 15;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 3: OVER-LIMIT MEMBERS (Fraud/Abuse Risks)
-- ═══════════════════════════════════════════════════════════════════════════

WITH company_contract AS (
    SELECT 
        gc.groupname,
        gc.startdate,
        gc.enddate,
        g.groupid
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
    LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON gc.groupname = g.groupname
    WHERE gc.groupname = '{company_name}'
        AND gc.iscurrent = 1
),

member_plans AS (
    SELECT DISTINCT
        m.legacycode as enrollee_id,
        m.firstname,
        m.lastname,
        mp.planid
    FROM "AI DRIVEN DATA"."MEMBER" m
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON m.memberid = mp.memberid AND mp.iscurrent = 1
),

all_utilization AS (
    SELECT 
        c.enrollee_id,
        c.code as procedurecode,
        c.approvedamount as amount
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    WHERE c.datesubmitted >= cc.startdate
        AND c.datesubmitted <= cc.enddate
        AND c.approvedamount > 0
    
    UNION ALL
    
    SELECT 
        pa.IID as enrollee_id,
        pa.code as procedurecode,
        pa.granted as amount
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN company_contract cc ON pa.groupname = cc.groupname
    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
    WHERE pa.requestdate >= cc.startdate
        AND pa.requestdate <= cc.enddate
        AND pa.granted > 0
        AND c.panumber IS NULL
),

member_benefit_usage AS (
    SELECT 
        u.enrollee_id,
        mp.firstname,
        mp.lastname,
        mp.planid,
        bp.benefitcodeid,
        bc.benefitcodedesc as benefit_name,
        COUNT(*) as times_used,
        ROUND(SUM(u.amount), 2) as total_spent
    FROM all_utilization u
    INNER JOIN member_plans mp ON u.enrollee_id = mp.enrollee_id
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bp ON 
        LOWER(TRIM(u.procedurecode)) = LOWER(TRIM(bp.procedurecode))
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON bp.benefitcodeid = bc.benefitcodeid
    WHERE bp.benefitcodeid IS NOT NULL
    GROUP BY u.enrollee_id, mp.firstname, mp.lastname, mp.planid, bp.benefitcodeid, bc.benefitcodedesc
),

over_limit_members AS (
    SELECT 
        mbu.enrollee_id,
        CONCAT(mbu.firstname, ' ', mbu.lastname) as member_name,
        mbu.benefit_name,
        mbu.times_used,
        mbu.total_spent,
        lim.maxlimit as monetary_limit,
        lim.countperannum as count_limit,
        -- Calculate overage
        CASE 
            WHEN lim.maxlimit IS NOT NULL THEN mbu.total_spent - lim.maxlimit
            ELSE NULL
        END as monetary_overage,
        CASE 
            WHEN lim.countperannum IS NOT NULL THEN mbu.times_used - lim.countperannum
            ELSE NULL
        END as count_overage,
        -- Flag type
        CASE 
            WHEN lim.maxlimit IS NOT NULL AND mbu.total_spent > lim.maxlimit THEN 'MONETARY'
            WHEN lim.countperannum IS NOT NULL AND mbu.times_used > lim.countperannum THEN 'COUNT'
            ELSE NULL
        END as limit_type_exceeded
    FROM member_benefit_usage mbu
    LEFT JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" lim ON 
        mbu.planid = lim.planid AND mbu.benefitcodeid = lim.benefitcodeid
    WHERE (lim.maxlimit IS NOT NULL AND mbu.total_spent > lim.maxlimit)
       OR (lim.countperannum IS NOT NULL AND mbu.times_used > lim.countperannum)
)
SELECT 
    '═══ SECTION 3: OVER-LIMIT MEMBERS ═══' as section,
    enrollee_id,
    member_name,
    benefit_name,
    limit_type_exceeded as limit_type,
    CASE 
        WHEN limit_type_exceeded = 'MONETARY' 
        THEN CONCAT('Used ₦', TO_CHAR(total_spent, '999,999'), ' / Limit ₦', TO_CHAR(monetary_limit, '999,999'))
        ELSE CONCAT('Used ', times_used, ' times / Limit ', count_limit, ' times')
    END as usage_vs_limit,
    CASE 
        WHEN limit_type_exceeded = 'MONETARY' 
        THEN CONCAT('₦', TO_CHAR(monetary_overage, '999,999'), ' OVER')
        ELSE CONCAT(count_overage, ' uses OVER')
    END as overage,
    CASE 
        WHEN limit_type_exceeded = 'MONETARY' AND monetary_overage > monetary_limit 
        THEN '🔴 CRITICAL (>100% over)'
        WHEN limit_type_exceeded = 'COUNT' AND count_overage > count_limit 
        THEN '🔴 CRITICAL (>100% over)'
        ELSE '🟡 MODERATE'
    END as risk_level
FROM over_limit_members
ORDER BY 
    CASE WHEN limit_type_exceeded = 'MONETARY' THEN monetary_overage ELSE count_overage * 10000 END DESC
LIMIT 20;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 4: UNLIMITED BENEFITS (Exposure Risks)
-- ═══════════════════════════════════════════════════════════════════════════

WITH company_contract AS (
    SELECT 
        gc.groupname,
        gc.startdate,
        gc.enddate,
        g.groupid
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
    LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON gc.groupname = g.groupname
    WHERE gc.groupname = '{company_name}'
        AND gc.iscurrent = 1
),

company_plans AS (
    -- Get all unique plans for this company
    SELECT DISTINCT mp.planid
    FROM "AI DRIVEN DATA"."MEMBER" m
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    LEFT JOIN "AI DRIVEN DATA"."MEMBER_PLANS" mp ON m.memberid = mp.memberid AND mp.iscurrent = 1
    WHERE mp.planid IS NOT NULL
),

all_utilization AS (
    SELECT 
        c.code as procedurecode,
        c.approvedamount as amount
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    WHERE c.datesubmitted >= cc.startdate
        AND c.datesubmitted <= cc.enddate
        AND c.approvedamount > 0
    
    UNION ALL
    
    SELECT 
        pa.code as procedurecode,
        pa.granted as amount
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN company_contract cc ON pa.groupname = cc.groupname
    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
    WHERE pa.requestdate >= cc.startdate
        AND pa.requestdate <= cc.enddate
        AND pa.granted > 0
        AND c.panumber IS NULL
),

benefit_usage AS (
    SELECT 
        bp.benefitcodeid,
        bc.benefitcodedesc as benefit_name,
        COUNT(*) as total_uses,
        ROUND(SUM(u.amount), 2) as total_cost
    FROM all_utilization u
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODE_PROCEDURES" bp ON 
        LOWER(TRIM(u.procedurecode)) = LOWER(TRIM(bp.procedurecode))
    LEFT JOIN "AI DRIVEN DATA"."BENEFITCODES" bc ON bp.benefitcodeid = bc.benefitcodeid
    WHERE bp.benefitcodeid IS NOT NULL
    GROUP BY bp.benefitcodeid, bc.benefitcodedesc
),

unlimited_benefits AS (
    SELECT 
        bu.benefit_name,
        bu.total_uses,
        bu.total_cost
    FROM benefit_usage bu
    WHERE NOT EXISTS (
        -- Check if this benefit has ANY limit defined in ANY plan for this company
        SELECT 1
        FROM company_plans cp
        INNER JOIN "AI DRIVEN DATA"."PLANBENEFITCODE_LIMIT" lim ON 
            cp.planid = lim.planid AND bu.benefitcodeid = lim.benefitcodeid
        WHERE lim.maxlimit IS NOT NULL OR lim.countperannum IS NOT NULL
    )
)
SELECT 
    '═══ SECTION 4: UNLIMITED BENEFITS (EXPOSURE RISK) ═══' as section,
    benefit_name,
    total_uses as times_used,
    CONCAT('₦', TO_CHAR(total_cost, '999,999,999')) as total_cost,
    '⚪ UNLIMITED' as status,
    CASE 
        WHEN total_cost > 5000000 THEN '🔴 HIGH EXPOSURE (>₦5M spent with no limit)'
        WHEN total_cost > 1000000 THEN '🟡 MODERATE EXPOSURE (>₦1M spent with no limit)'
        ELSE '✅ LOW EXPOSURE (<₦1M spent)'
    END as risk_level,
    'Recommend adding limits' as recommendation
FROM unlimited_benefits
ORDER BY total_cost DESC;

-- ═══════════════════════════════════════════════════════════════════════════
-- SECTION 5: TOP HIGH-COST MEMBERS (Member Concentration Risk)
-- ═══════════════════════════════════════════════════════════════════════════

WITH company_contract AS (
    SELECT 
        gc.groupname,
        gc.startdate,
        gc.enddate,
        g.groupid
    FROM "AI DRIVEN DATA"."GROUP_CONTRACT" gc
    LEFT JOIN "AI DRIVEN DATA"."GROUPS" g ON gc.groupname = g.groupname
    WHERE gc.groupname = '{company_name}'
        AND gc.iscurrent = 1
),

all_utilization AS (
    SELECT 
        c.enrollee_id,
        c.approvedamount as amount
    FROM "AI DRIVEN DATA"."CLAIMS DATA" c
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON c.enrollee_id = m.legacycode
    INNER JOIN company_contract cc ON CAST(m.groupid AS VARCHAR) = CAST(cc.groupid AS VARCHAR)
    WHERE c.datesubmitted >= cc.startdate
        AND c.datesubmitted <= cc.enddate
        AND c.approvedamount > 0
    
    UNION ALL
    
    SELECT 
        pa.IID as enrollee_id,
        pa.granted as amount
    FROM "AI DRIVEN DATA"."PA DATA" pa
    INNER JOIN company_contract cc ON pa.groupname = cc.groupname
    LEFT JOIN "AI DRIVEN DATA"."CLAIMS DATA" c ON CAST(pa.panumber AS BIGINT) = CAST(c.panumber AS BIGINT)
    WHERE pa.requestdate >= cc.startdate
        AND pa.requestdate <= cc.enddate
        AND pa.granted > 0
        AND c.panumber IS NULL
),

member_totals AS (
    SELECT 
        u.enrollee_id,
        m.firstname,
        m.lastname,
        COUNT(*) as total_uses,
        ROUND(SUM(u.amount), 2) as total_cost
    FROM all_utilization u
    INNER JOIN "AI DRIVEN DATA"."MEMBER" m ON u.enrollee_id = m.legacycode
    GROUP BY u.enrollee_id, m.firstname, m.lastname
)
SELECT 
    '═══ SECTION 5: TOP 15 HIGH-COST MEMBERS ═══' as section,
    enrollee_id,
    CONCAT(firstname, ' ', lastname) as member_name,
    total_uses as times_used,
    CONCAT('₦', TO_CHAR(total_cost, '999,999,999')) as total_cost,
    CONCAT(ROUND(total_cost / SUM(total_cost) OVER () * 100, 1), '%') as pct_of_company_cost,
    CASE 
        WHEN total_cost > 10000000 THEN '🔴 CRITICAL (>₦10M)'
        WHEN total_cost > 5000000 THEN '🟡 HIGH (>₦5M)'
        ELSE '✅ MODERATE'
    END as risk_level
FROM member_totals
ORDER BY total_cost DESC
LIMIT 15;
```

**Usage Notes:**
- Replace `{company_name}` with the actual company name
- Each section can be run independently or together
- Results are ordered by risk/cost (highest first)
- Use this analysis for data-driven renewal negotiations

---

## Usage Notes

1. **Replace placeholders**: Replace `{enrollee_id}`, `{company_name}`, `{period_start}`, `{period_end}` with actual values
2. **Date calculations**: For "last 6 months", use `CURRENT_DATE - INTERVAL '6 months'` as period_start
3. **Error handling**: Always check if contracts exist before running MLR comparison queries
4. **Performance**: These queries can be large - consider adding LIMIT clauses for initial exploration
5. **Null handling**: Use COALESCE or NULLIF to handle division by zero in MLR calculations
6. **Schema prefix**: Always use `"AI DRIVEN DATA"."TABLE_NAME"` format
7. **Type casting**: When joining `panumber` between PA and Claims, cast to BIGINT: `CAST(panumber AS BIGINT)`
8. **Group matching in claims**: Use `nhisgroupid` to join CLAIMS DATA with GROUPS

---

## Available Tables (40+ tables)

Key tables include:
- `CLAIMS DATA` - All claims submitted
- `PA DATA` - Prior authorizations
- `GROUPS` - Client/group information
- `MEMBERS` - Enrollee/member information
- `PROVIDERS` - Hospital/provider information
- `GROUP_CONTRACT` - Contract periods
- `DEBIT_NOTE` - Debit notes (premium invoices)
- `CLIENT_CASH_RECEIVED` - Cash received from clients
- `DIAGNOSIS` - Diagnosis codes and descriptions
- `PROCEDURE DATA` - Procedure codes and descriptions
- `TBPADIAGNOSIS` - PA diagnoses
- And many more...

**To see all tables:**
```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'AI DRIVEN DATA'
ORDER BY table_name;
```

---

**End of Knowledge Base**

