# DEBIT NOTE ACCRUED Table Script

## Overview
This script creates the `DEBIT_NOTE_ACCRUED` table from FIN_GL raw data using the correct pairing logic.

## Files
- `create_debit_note_accrued.py` - Standalone script to create the table
- `auto_update_database.py` - Updated to include DEBIT_NOTE_ACCRUED in auto-updates

## Usage

### Standalone Usage
```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT
source venv/bin/activate
python3 create_debit_note_accrued.py
```

### Auto-Update Integration
```bash
cd /Users/kenechukwuchukwuka/Downloads/DLT
source venv/bin/activate
python3 auto_update_database.py
```

## Table Logic

### Requirements
1. **Transaction Pairing**: Each transaction is posted twice (one debited, one credited)
2. **Account Types**: INCOME (always debited/negative) + CURRENT LIABILITIES (always credited/positive)
3. **Matching Criteria**: Same GLDate and code
4. **Amount Validation**: Sum must equal zero (e.g., -2000 + 2000 = 0)

### Output Columns
- `Date`: Transaction date
- `groupname`: Company/group name from E_ACCOUNT_GROUP
- `Amount`: Positive amount (from CURRENT LIABILITIES side)
- `Year`: Extracted year
- `Month`: Extracted month

### Data Sources
- `FIN_GL_2023_RAW`
- `FIN_GL_2024_RAW` 
- `FIN_GL_2025_RAW`
- `E_ACCOUNT_GROUP` (for groupname mapping)

## Validation
The script includes validation to ensure:
- Table exists with correct structure
- ARIK AIR has July & September 2025 data (test case)
- All expected columns are present

## Expected Results
- **Total Rows**: ~90,000 transactions
- **Total Amount**: ~₦17 billion
- **ARIK AIR July 2025**: ₦27,631,230
- **ARIK AIR September 2025**: ₦27,631,230

## Troubleshooting
If the script fails:
1. Ensure all FIN_GL raw tables exist
2. Check E_ACCOUNT_GROUP table is loaded
3. Verify data types match (code field as VARCHAR)
4. Run validation to identify specific issues

## Last Updated
2025-01-25 - Fixed pairing logic to handle both transaction orders

