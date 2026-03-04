#!/usr/bin/env python3
"""
Setup script for AI Health Insurance Analyst
Configures the environment and installs dependencies
"""

import os
import sys
import subprocess
import toml
from pathlib import Path

def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("❌ Python 3.8 or higher is required")
        sys.exit(1)
    print(f"✅ Python {sys.version.split()[0]} detected")

def install_requirements():
    """Install required packages"""
    print("📦 Installing required packages...")
    
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", "requirements_ai.txt"
        ])
        print("✅ Requirements installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        sys.exit(1)

def create_secrets_template():
    """Create a secrets.toml template"""
    secrets_path = Path("secrets.toml")
    
    if not secrets_path.exists():
        print("📝 Creating secrets.toml template...")
        
        template = """# AI Health Insurance Analyst Configuration
# Copy this file and fill in your actual values

[openai]
api_key = "your-openai-api-key-here"

[medicloud]
server = "your-medicloud-server"
database = "your-medicloud-database"
username = "your-username"
password = "your-password"

[eaccount]
server = "your-eaccount-server"
database = "your-eaccount-database"
username = "your-username"
password = "your-password"

[google_sheets]
# For Google Sheets integration (if needed)
credentials_file = "path/to/credentials.json"
"""
        
        with open(secrets_path, "w") as f:
            f.write(template)
        
        print("✅ Created secrets.toml template")
        print("⚠️  Please edit secrets.toml with your actual credentials")
    else:
        print("✅ secrets.toml already exists")

def create_env_template():
    """Create a .env template"""
    env_path = Path(".env")
    
    if not env_path.exists():
        print("📝 Creating .env template...")
        
        template = """# AI Health Insurance Analyst Environment Variables
# Copy this file and fill in your actual values

OPENAI_API_KEY=your-openai-api-key-here
DATABASE_PATH=ai_driven_data.duckdb
LOG_LEVEL=INFO
"""
        
        with open(env_path, "w") as f:
            f.write(template)
        
        print("✅ Created .env template")
        print("⚠️  Please edit .env with your actual values")
    else:
        print("✅ .env already exists")

def create_startup_scripts():
    """Create startup scripts for different platforms"""
    
    # Windows batch file
    windows_script = """@echo off
echo Starting AI Health Insurance Analyst...
call venv\\Scripts\\activate
streamlit run ai_health_analyst.py
pause
"""
    
    with open("start_ai_analyst.bat", "w") as f:
        f.write(windows_script)
    
    # Unix shell script
    unix_script = """#!/bin/bash
echo "Starting AI Health Insurance Analyst..."
source venv/bin/activate
streamlit run ai_health_analyst.py
"""
    
    with open("start_ai_analyst.sh", "w") as f:
        f.write(unix_script)
    
    # Make Unix script executable
    os.chmod("start_ai_analyst.sh", 0o755)
    
    print("✅ Created startup scripts:")
    print("   - start_ai_analyst.bat (Windows)")
    print("   - start_ai_analyst.sh (Unix/Linux/Mac)")

def create_readme():
    """Create a comprehensive README"""
    readme_content = """# AI Health Insurance Analyst

An intelligent data analysis system powered by GPT-4 for health insurance data insights.

## 🚀 Quick Start

### 1. Setup
```bash
python setup_ai_analyst.py
```

### 2. Configure Credentials
Edit `secrets.toml` with your database credentials and OpenAI API key.

### 3. Start the Analyst
```bash
# Windows
start_ai_analyst.bat

# Unix/Linux/Mac
./start_ai_analyst.sh

# Or directly
streamlit run ai_health_analyst.py
```

## 📊 Features

- **Natural Language Queries**: Ask complex questions in plain English
- **AI-Powered Analysis**: GPT-4 generates SQL queries and provides insights
- **Health Insurance Expertise**: Specialized knowledge of PA, claims, and benefits
- **Interactive Visualizations**: Automatic charts and graphs
- **Real-time Data**: Connect to live database systems
- **Auto-Updates**: Scheduled data refresh capabilities

## 🗄️ Database Tables

- **PA DATA**: Pre-authorization requests and procedures
- **CLAIMS DATA**: Submitted claims and payments
- **PROVIDERS**: Healthcare providers and hospitals
- **GROUPS**: Client companies and organizations
- **MEMBERS**: Active enrollees and members
- **BENEFITCODES**: Benefit categories and descriptions
- **BENEFITCODE_PROCEDURES**: Procedure-to-benefit mappings
- **GROUP_PLANS**: Individual and family plans per group

## 🤖 Example Questions

- "What are the top 10 groups by total spending in the last 6 months?"
- "Which providers have the highest average claim amounts?"
- "What's the trend in PA approval rates over time?"
- "What are the most common procedures by benefit category?"
- "Which groups are approaching their plan limits?"

## 🔄 Auto-Update System

### Manual Update
```bash
python auto_update_database.py
```

### Scheduled Updates
```bash
# Run once
python schedule_updates.py --once

# Run as daemon (continuous)
python schedule_updates.py --daemon

# Test update
python schedule_updates.py --test
```

## 📁 File Structure

```
├── ai_health_analyst.py      # Main Streamlit application
├── auto_update_database.py   # Database update script
├── schedule_updates.py       # Scheduler for auto-updates
├── setup_ai_analyst.py       # Setup script
├── setup_ai_database.py      # Database initialization
├── dlt_sources.py           # Data source connections
├── requirements_ai.txt       # Python dependencies
├── secrets.toml             # Configuration file
├── .env                     # Environment variables
└── ai_driven_data.duckdb    # DuckDB database file
```

## 🔧 Configuration

### OpenAI API Key
Set your OpenAI API key in either:
- `secrets.toml`: `[openai] api_key = "your-key"`
- `.env`: `OPENAI_API_KEY=your-key`
- Environment variable: `export OPENAI_API_KEY=your-key`

### Database Credentials
Configure in `secrets.toml`:
```toml
[medicloud]
server = "your-server"
database = "your-database"
username = "your-username"
password = "your-password"
```

## 🚨 Troubleshooting

### Common Issues

1. **OpenAI API Key Not Found**
   - Ensure API key is set in `secrets.toml` or `.env`
   - Check that the key is valid and has credits

2. **Database Connection Failed**
   - Verify credentials in `secrets.toml`
   - Ensure VPN is connected (if required)
   - Check database server availability

3. **Import Errors**
   - Run `pip install -r requirements_ai.txt`
   - Ensure you're using the correct Python environment

4. **Permission Errors**
   - On Unix systems: `chmod +x start_ai_analyst.sh`
   - Ensure write permissions for database file

### Logs
- Application logs: `streamlit.log`
- Update logs: `database_update.log`
- Scheduler logs: `scheduler.log`

## 📞 Support

For issues or questions:
1. Check the logs for error messages
2. Verify all configuration files are properly set
3. Ensure all dependencies are installed
4. Test database connectivity separately

## 🔄 Updates

To update the system:
1. Pull latest code changes
2. Run `python setup_ai_analyst.py` to update dependencies
3. Restart the application

---

**AI Health Insurance Analyst** - Powered by Streamlit, DuckDB & GPT-4
"""
    
    with open("README_AI_ANALYST.md", "w") as f:
        f.write(readme_content)
    
    print("✅ Created README_AI_ANALYST.md")

def main():
    """Main setup function"""
    print("🏥 AI Health Insurance Analyst Setup")
    print("=" * 50)
    
    # Check Python version
    check_python_version()
    
    # Install requirements
    install_requirements()
    
    # Create configuration files
    create_secrets_template()
    create_env_template()
    
    # Create startup scripts
    create_startup_scripts()
    
    # Create documentation
    create_readme()
    
    print("\n🎉 Setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Edit secrets.toml with your database credentials")
    print("2. Set your OpenAI API key in secrets.toml or .env")
    print("3. Run: streamlit run ai_health_analyst.py")
    print("\n📖 See README_AI_ANALYST.md for detailed instructions")

if __name__ == "__main__":
    main()
