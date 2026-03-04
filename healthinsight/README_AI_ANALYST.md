# AI Health Insurance Analyst

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
