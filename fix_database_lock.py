#!/usr/bin/env python3
"""
Database Lock Fix Script
This script helps resolve DuckDB locking issues by:
1. Checking for processes using the database
2. Killing any conflicting processes
3. Testing database connectivity
"""

import os
import subprocess
import sys
import duckdb
from pathlib import Path

def check_database_lock():
    """Check if database is locked and by what process"""
    db_file = "ai_driven_data.duckdb"
    
    if not os.path.exists(db_file):
        print("❌ Database file not found!")
        return False
    
    print(f"🔍 Checking database lock for: {db_file}")
    
    # Check for processes using the file
    try:
        result = subprocess.run(['lsof', db_file], capture_output=True, text=True)
        if result.returncode == 0 and result.stdout:
            print("⚠️  Database is being used by:")
            print(result.stdout)
            return False
        else:
            print("✅ No processes found using the database")
            return True
    except FileNotFoundError:
        print("⚠️  lsof command not found, trying alternative method...")
        return True

def test_database_connection():
    """Test if database can be opened"""
    try:
        conn = duckdb.connect('ai_driven_data.duckdb', read_only=True)
        result = conn.execute("SELECT 1").fetchone()
        conn.close()
        print("✅ Database connection test successful!")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def kill_streamlit_processes():
    """Kill any running Streamlit processes"""
    try:
        print("🔄 Killing any running Streamlit processes...")
        subprocess.run(['pkill', '-f', 'streamlit run ai_health_analyst.py'], 
                      capture_output=True)
        print("✅ Streamlit processes killed")
        return True
    except Exception as e:
        print(f"⚠️  Could not kill processes: {e}")
        return False

def main():
    """Main function to fix database lock issues"""
    print("🚀 Database Lock Fix Script")
    print("=" * 40)
    
    # Check if database is locked
    if not check_database_lock():
        print("\n🔧 Attempting to fix database lock...")
        
        # Kill Streamlit processes
        kill_streamlit_processes()
        
        # Wait a moment
        import time
        time.sleep(2)
        
        # Check again
        if not check_database_lock():
            print("❌ Database is still locked. Manual intervention required.")
            print("💡 Try:")
            print("   1. Close all terminal windows")
            print("   2. Restart your computer")
            print("   3. Check for other applications using the database")
            return False
    
    # Test database connection
    if test_database_connection():
        print("\n🎉 Database is ready to use!")
        print("💡 You can now run: streamlit run ai_health_analyst.py")
        return True
    else:
        print("\n❌ Database connection still failing")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
