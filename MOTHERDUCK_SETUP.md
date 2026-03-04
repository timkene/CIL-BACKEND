# MotherDuck Cloud Database Setup & Usage

## Overview

Your Clearline contract analyzer now supports **both local and cloud databases**:
- **Local DuckDB**: Runs on your machine (default)
- **MotherDuck**: Cloud-based DuckDB for faster remote access

## Initial Setup (One-Time)

### 1. Upload Data to MotherDuck

Run the initial upload script to copy all tables from local to cloud:

```bash
python /Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py initial
```

This will:
- Create the `ai_driven_data` database in MotherDuck
- Upload all tables from `AI DRIVEN DATA` schema
- Show progress for each table uploaded

**Expected output:**
```
🚀 Starting initial upload to MotherDuck...
📦 Creating database: ai_driven_data
📊 Found X tables to upload
⏳ Uploading: AI DRIVEN DATA.CLAIMS DATA
✅ Uploaded XXX,XXX rows
...
🎉 Initial upload complete!
```

## Daily Sync (Keep Cloud Updated)

### Option 1: Daily Full Sync (Recommended)

Updates all configured tables with latest data:

```bash
python /Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py daily
```

### Option 2: Smart Sync (Faster)

Only updates tables that changed:

```bash
python /Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py smart
```

## Using MotherDuck in the App

### In the Streamlit UI:

1. Launch your contract analyzer app
2. In the sidebar, you'll see **"🗄️ Database Source"**
3. Check the box **"☁️ Use MotherDuck (Cloud)"**
4. The app will now query from the cloud database

**Visual indicators:**
- ✅ **Checked**: 🌐 Using MotherDuck cloud database
- ❌ **Unchecked**: 💻 Using local database

### Benefits of Using MotherDuck:

1. **Faster access** from remote locations
2. **Shared access** across multiple machines
3. **No need** to copy database file
4. **Automatic backups** in the cloud

## Configuration Files

### motherduck.py

Contains:
- `MOTHERDUCK_TOKEN`: Your authentication token (already configured)
- `MOTHERDUCK_DB`: Database name (`ai_driven_data`)
- `LOCAL_DB`: Path to local database file

**Tables synced by default:**
- CLAIMS DATA
- DEBIT_NOTE
- CLIENT_CASH_RECEIVED
- PA DATA
- GROUP_CONTRACT
- GROUPS
- ENROLLEES
- PROVIDERS

To add more tables, edit the `tables_to_sync` list in `motherduck.py` line 80.

## Automated Daily Sync (Optional)

Set up a cron job to automatically sync every day at 2 AM:

```bash
crontab -e
```

Add this line:
```
0 2 * * * /Users/kenechukwuchukwuka/Downloads/DLT/venv/bin/python /Users/kenechukwuchukwuka/Downloads/DLT/motherduck.py daily
```

## Troubleshooting

### "No database named 'ai_driven_data' found"
**Solution**: Run the initial upload:
```bash
python motherduck.py initial
```

### Token expired
**Solution**: Get a new token from MotherDuck console and update `MOTHERDUCK_TOKEN` in `motherduck.py`

### Tables out of sync
**Solution**: Run daily sync:
```bash
python motherduck.py daily
```

### Connection issues
**Solution**:
1. Check internet connection
2. Verify token is still valid
3. Try using local database instead (uncheck the MotherDuck toggle)

## Code Changes Summary

### complete_calculation_engine.py

The `CalculationEngine` class now accepts:
```python
engine = CalculationEngine(
    use_motherduck=True,  # Use cloud database
    motherduck_token=None # Auto-loads from motherduck.py
)
```

### contract_analyzer_complete_hybrid.py

Added sidebar toggle to switch between local and cloud databases. The toggle is located in the **Database Source** section before company selection.

## Security Notes

⚠️ **IMPORTANT**: The MotherDuck token in `motherduck.py` provides read/write access to your cloud database.

**Best practices:**
- Keep `motherduck.py` private
- Do not commit to public repositories
- Rotate tokens periodically
- Use environment variables in production

## Cost

MotherDuck pricing is based on:
- Storage: Very low cost for typical HMO data
- Queries: Pay per query (usually < ₦1 per analysis)
- Free tier available for testing

Your typical analysis cost remains ~₦60, with minimal additional cost for MotherDuck queries.
