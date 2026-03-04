#!/bin/bash
# Setup cron job to update MotherDuck database automatically at 6am and 6pm daily

# Get the absolute path to the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/auto_update_database.py"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Check if virtual environment exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Virtual environment not found at $VENV_PYTHON"
    echo "   Please ensure you have activated and set up the virtual environment"
    exit 1
fi

# Check if the Python script exists
if [ ! -f "$PYTHON_SCRIPT" ]; then
    echo "❌ Python script not found at $PYTHON_SCRIPT"
    exit 1
fi

# Create log directory if it doesn't exist
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# Cron job entries (9am and 3pm daily)
CRON_ENTRY_9AM="0 9 * * * cd $SCRIPT_DIR && $VENV_PYTHON $PYTHON_SCRIPT --motherduck-only >> $LOG_DIR/motherduck_update_9am.log 2>&1"
CRON_ENTRY_3PM="0 15 * * * cd $SCRIPT_DIR && $VENV_PYTHON $PYTHON_SCRIPT --motherduck-only >> $LOG_DIR/motherduck_update_15pm.log 2>&1"

# Check if cron jobs already exist
CRON_TEMP=$(mktemp)
crontab -l 2>/dev/null > "$CRON_TEMP" || true

if grep -q "auto_update_database.py --motherduck-only" "$CRON_TEMP" 2>/dev/null; then
    echo "⚠️  Cron jobs for MotherDuck updates already exist!"
    echo ""
    echo "Current cron jobs:"
    grep "auto_update_database.py --motherduck-only" "$CRON_TEMP" || true
    echo ""
    read -p "Do you want to remove existing entries and add new ones? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Remove existing entries
        grep -v "auto_update_database.py --motherduck-only" "$CRON_TEMP" > "${CRON_TEMP}.new"
        mv "${CRON_TEMP}.new" "$CRON_TEMP"
        echo "✅ Removed existing cron entries"
    else
        echo "❌ Cancelled. No changes made."
        rm "$CRON_TEMP"
        exit 0
    fi
fi

# Add new cron entries
echo "$CRON_ENTRY_6AM" >> "$CRON_TEMP"
echo "$CRON_ENTRY_6PM" >> "$CRON_TEMP"

# Install the new crontab
crontab "$CRON_TEMP"
rm "$CRON_TEMP"

echo "✅ Cron jobs installed successfully!"
echo ""
echo "Scheduled updates:"
echo "  - 6:00 AM daily: Update MotherDuck database"
echo "  - 6:00 PM daily: Update MotherDuck database"
echo ""
echo "Log files will be saved to:"
echo "  - $LOG_DIR/motherduck_update_6am.log"
echo "  - $LOG_DIR/motherduck_update_6pm.log"
echo ""
echo "To view your cron jobs, run: crontab -l"
echo "To remove these cron jobs, run: crontab -e (then delete the lines)"

