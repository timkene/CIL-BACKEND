# Automated MotherDuck Database Updates

This setup allows you to automatically update your MotherDuck database twice daily (6am and 6pm) while keeping manual control over local DuckDB updates.

## How It Works

- **Manual Run**: Updates BOTH local DuckDB and MotherDuck
  ```bash
  python auto_update_database.py
  ```

- **Cron Job (Automatic)**: Updates ONLY MotherDuck at 6am and 6pm daily
  ```bash
  python auto_update_database.py --motherduck-only
  ```

This means:
- If you don't manually run the script, MotherDuck will be more recent than local DuckDB
- Manual runs keep both databases in sync
- Automated runs keep MotherDuck up-to-date without touching local DuckDB

## Setup Instructions

### 1. Install Cron Jobs

Run the setup script:

```bash
./setup_cron_motherduck.sh
```

This will:
- Add two cron jobs (6am and 6pm)
- Create a `logs/` directory for log files
- Set up automatic MotherDuck updates

### 2. Verify Installation

Check your cron jobs:

```bash
crontab -l
```

You should see entries like:
```
0 6 * * * cd /path/to/DLT && /path/to/venv/bin/python auto_update_database.py --motherduck-only >> logs/motherduck_update_6am.log 2>&1
0 18 * * * cd /path/to/DLT && /path/to/venv/bin/python auto_update_database.py --motherduck-only >> logs/motherduck_update_6pm.log 2>&1
```

### 3. Monitor Logs

Check the log files to see if updates are running:

```bash
# Morning update log
tail -f logs/motherduck_update_6am.log

# Evening update log
tail -f logs/motherduck_update_6pm.log
```

## Command Options

### Manual Updates (Both Databases)
```bash
# Update both local and MotherDuck
python auto_update_database.py

# Update both with verbose logging
python auto_update_database.py --verbose
```

### MotherDuck Only (For Cron)
```bash
# Update only MotherDuck (used by cron)
python auto_update_database.py --motherduck-only

# Update only MotherDuck with verbose logging
python auto_update_database.py --motherduck-only --verbose
```

### Local Only
```bash
# Update only local DuckDB
python auto_update_database.py --no-motherduck
```

## Removing Cron Jobs

To remove the automated updates:

```bash
crontab -e
```

Then delete the lines containing `auto_update_database.py --motherduck-only` and save.

Or use:

```bash
crontab -l | grep -v "auto_update_database.py --motherduck-only" | crontab -
```

## Important Notes

1. **Derived Tables**: In `--motherduck-only` mode, derived tables (like `DEBIT_NOTE_ACCRUED`, `CLIENT_CASH_RECEIVED`, etc.) are synced from local DuckDB if it exists. If local DuckDB is outdated, these tables in MotherDuck will also be outdated until you run a manual update.

2. **FIN_GL Tables**: The 2025 FIN_GL table requires local DuckDB to be updated first (it reads from EACCOUNT). In `--motherduck-only` mode, it will try to sync from local if available.

3. **Log Files**: Logs are automatically created in the `logs/` directory. Check them regularly to ensure updates are running successfully.

4. **Virtual Environment**: The cron job uses the virtual environment at `venv/bin/python`. Make sure this path is correct in your setup.

## Troubleshooting

### Cron job not running?
- Check if cron service is running: `sudo service cron status` (Linux) or check System Preferences > Users & Groups > Login Items (macOS)
- Verify the Python path in cron is correct
- Check log files for errors

### Permission errors?
- Ensure the script has execute permissions: `chmod +x setup_cron_motherduck.sh`
- Check file permissions on `auto_update_database.py`

### Database connection errors?
- Verify MotherDuck token is still valid
- Check network connectivity
- Review log files for specific error messages

