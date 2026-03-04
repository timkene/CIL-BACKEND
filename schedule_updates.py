#!/usr/bin/env python3
"""
Database Update Scheduler
Schedules automatic updates of the AI DRIVEN DATA database
"""

import schedule
import time
import logging
import os
import sys
from datetime import datetime
from auto_update_database import DatabaseUpdater

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def run_database_update():
    """Run the database update process"""
    logger.info("🕐 Scheduled database update starting...")
    
    try:
        updater = DatabaseUpdater()
        success = updater.update_all_tables()
        
        if success:
            logger.info("✅ Scheduled update completed successfully!")
        else:
            logger.error("❌ Scheduled update failed!")
            
    except Exception as e:
        logger.error(f"❌ Error during scheduled update: {e}")

def setup_schedule():
    """Set up the update schedule"""
    logger.info("📅 Setting up database update schedule...")
    
    # Clear any existing schedules
    schedule.clear()

    # Business days only: Monday to Friday at 09:00, 12:00, and 15:00
    for day in [schedule.every().monday, schedule.every().tuesday, schedule.every().wednesday,
                schedule.every().thursday, schedule.every().friday]:
        day.at("09:00").do(run_database_update)
        day.at("12:00").do(run_database_update)
        day.at("15:00").do(run_database_update)

    logger.info("✅ Schedule configured (Mon–Fri):")
    logger.info("   - 09:00")
    logger.info("   - 12:00")
    logger.info("   - 15:00")

def main():
    """Main scheduler function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Schedule AI DRIVEN DATA database updates')
    parser.add_argument('--test', action='store_true', help='Run a test update immediately')
    parser.add_argument('--once', action='store_true', help='Run update once and exit')
    parser.add_argument('--daemon', action='store_true', help='Run as daemon (continuous)')
    
    args = parser.parse_args()
    
    if args.test:
        logger.info("🧪 Running test update...")
        run_database_update()
        return
    
    if args.once:
        logger.info("🔄 Running single update...")
        run_database_update()
        return
    
    if args.daemon:
        setup_schedule()
        logger.info("🚀 Starting scheduler daemon...")
        logger.info("Press Ctrl+C to stop")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            logger.info("🛑 Scheduler stopped by user")
    else:
        # Default: show schedule info
        setup_schedule()
        logger.info("\n📋 Available commands:")
        logger.info("   python schedule_updates.py --test    # Run test update")
        logger.info("   python schedule_updates.py --once    # Run update once")
        logger.info("   python schedule_updates.py --daemon  # Run as daemon")

if __name__ == "__main__":
    main()
