import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from typing import List, Dict, Any
import json

# Page configuration
st.set_page_config(
    page_title="AI Health Insurance Analyst (Basic)",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .ai-response {
        background: linear-gradient(135deg, #f0f2f6 0%, #e8f4fd 100%);
        padding: 1.5rem;
        border-radius: 15px;
        border-left: 5px solid #1f77b4;
        margin: 1rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .query-box {
        background: #f8f9fa;
        padding: 1rem;
        border-radius: 10px;
        border: 2px solid #e9ecef;
        margin: 1rem 0;
    }
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 25px;
        padding: 0.5rem 2rem;
        font-weight: bold;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 8px rgba(0,0,0,0.15);
    }
</style>
""", unsafe_allow_html=True)

def get_database_connection():
    """Get connection to the AI DRIVEN DATA database"""
    try:
        conn = duckdb.connect('ai_driven_data.duckdb')
        return conn
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

@st.cache_data
def get_database_schema():
    """Get comprehensive database schema information"""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        schema_info = {}
        
        # Get all tables in AI DRIVEN DATA schema
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'AI DRIVEN DATA'
            AND table_name != 'table_documentation'
        """).fetchall()
        
        for table in tables:
            table_name = table[0]
            
            # Get column information
            columns = conn.execute(f'DESCRIBE "AI DRIVEN DATA"."{table_name}"').fetchall()
            
            # Get row count
            row_count = conn.execute(f'SELECT COUNT(*) FROM "AI DRIVEN DATA"."{table_name}"').fetchone()[0]
            
            # Get sample data
            sample_data = conn.execute(f'SELECT * FROM "AI DRIVEN DATA"."{table_name}" LIMIT 3').fetchall()
            
            schema_info[table_name] = {
                'columns': [{'name': col[0], 'type': col[1]} for col in columns],
                'row_count': row_count,
                'sample_data': sample_data
            }
        
        conn.close()
        return schema_info
    except Exception as e:
        st.error(f"Error getting database schema: {e}")
        return None

def generate_basic_sql_query(user_question: str, schema_info: Dict) -> str:
    """Generate basic SQL query based on common patterns"""
    
    question_lower = user_question.lower()
    
    # Common query patterns
    if "how many" in question_lower and "pa" in question_lower:
        return "SELECT COUNT(*) as total_pa_requests FROM \"AI DRIVEN DATA\".\"PA DATA\""
    
    elif "how many" in question_lower and "claim" in question_lower:
        return "SELECT COUNT(*) as total_claims FROM \"AI DRIVEN DATA\".\"CLAIMS DATA\""
    
    elif "how many" in question_lower and "group" in question_lower:
        return "SELECT COUNT(*) as total_groups FROM \"AI DRIVEN DATA\".\"GROUPS\""
    
    elif "how many" in question_lower and "provider" in question_lower:
        return "SELECT COUNT(*) as total_providers FROM \"AI DRIVEN DATA\".\"PROVIDERS\""
    
    elif "how many" in question_lower and "member" in question_lower:
        return "SELECT COUNT(*) as total_members FROM \"AI DRIVEN DATA\".\"MEMBERS\""
    
    elif "top" in question_lower and "group" in question_lower and "spending" in question_lower:
        return """
        SELECT 
            groupname,
            SUM(granted) as total_spending
        FROM "AI DRIVEN DATA"."PA DATA" 
        GROUP BY groupname 
        ORDER BY total_spending DESC 
        LIMIT 10
        """
    
    elif "top" in question_lower and "provider" in question_lower:
        return """
        SELECT 
            p.providername,
            COUNT(*) as total_requests,
            AVG(pa.granted) as avg_amount
        FROM "AI DRIVEN DATA"."PA DATA" pa
        JOIN "AI DRIVEN DATA"."PROVIDERS" p ON pa.providerid = p.providerid
        GROUP BY p.providername
        ORDER BY total_requests DESC
        LIMIT 10
        """
    
    elif "approval" in question_lower and "rate" in question_lower:
        return """
        SELECT 
            pastatus,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
        FROM "AI DRIVEN DATA"."PA DATA"
        GROUP BY pastatus
        ORDER BY count DESC
        """
    
    elif "common" in question_lower and "procedure" in question_lower:
        return """
        SELECT 
            code,
            COUNT(*) as frequency
        FROM "AI DRIVEN DATA"."PA DATA"
        GROUP BY code
        ORDER BY frequency DESC
        LIMIT 10
        """
    
    elif "spending" in question_lower and "trend" in question_lower:
        return """
        SELECT 
            DATE_TRUNC('month', requestdate) as month,
            COUNT(*) as requests,
            SUM(granted) as total_spending
        FROM "AI DRIVEN DATA"."PA DATA"
        WHERE requestdate >= CURRENT_DATE - INTERVAL '12 months'
        GROUP BY month
        ORDER BY month
        """
    
    else:
        # Default query - show basic stats
        return """
        SELECT 
            'PA Requests' as metric,
            COUNT(*) as count
        FROM "AI DRIVEN DATA"."PA DATA"
        UNION ALL
        SELECT 
            'Claims' as metric,
            COUNT(*) as count
        FROM "AI DRIVEN DATA"."CLAIMS DATA"
        UNION ALL
        SELECT 
            'Groups' as metric,
            COUNT(*) as count
        FROM "AI DRIVEN DATA"."GROUPS"
        UNION ALL
        SELECT 
            'Providers' as metric,
            COUNT(*) as count
        FROM "AI DRIVEN DATA"."PROVIDERS"
        UNION ALL
        SELECT 
            'Members' as metric,
            COUNT(*) as count
        FROM "AI DRIVEN DATA"."MEMBERS"
        """

def execute_query_and_analyze(query: str, user_question: str) -> Dict[str, Any]:
    """Execute SQL query and provide analysis"""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        # Execute the query
        result_df = pd.read_sql(query, conn)
        conn.close()
        
        if result_df.empty:
            return {
                'data': result_df,
                'analysis': "No data found matching your criteria.",
                'visualization': None
            }
        
        # Generate basic analysis
        analysis = f"""
## 📊 Analysis Results

**Question:** {user_question}

**Query Results Summary:**
- **Rows returned:** {len(result_df):,}
- **Columns:** {', '.join(result_df.columns)}

**Key Insights:**
"""
        
        # Add specific insights based on the data
        if len(result_df) == 1 and len(result_df.columns) == 2:
            # Single metric result
            metric_name = result_df.columns[0]
            metric_value = result_df.iloc[0, 0]
            analysis += f"- **{metric_name}:** {metric_value:,}\n"
        
        elif 'total_spending' in result_df.columns:
            # Spending analysis
            total_spending = result_df['total_spending'].sum()
            analysis += f"- **Total Spending:** ₦{total_spending:,.2f}\n"
            analysis += f"- **Top Spender:** {result_df.iloc[0, 0] if len(result_df) > 0 else 'N/A'}\n"
        
        elif 'pastatus' in result_df.columns:
            # Status analysis
            approved = result_df[result_df['pastatus'] == 'APPROVED']['count'].sum() if 'APPROVED' in result_df['pastatus'].values else 0
            total = result_df['count'].sum()
            approval_rate = (approved / total * 100) if total > 0 else 0
            analysis += f"- **Approval Rate:** {approval_rate:.1f}%\n"
            analysis += f"- **Total Requests:** {total:,}\n"
        
        else:
            # General analysis
            analysis += f"- **Data points:** {len(result_df):,}\n"
            if len(result_df) > 0:
                analysis += f"- **First result:** {result_df.iloc[0].to_dict()}\n"
        
        analysis += "\n**Recommendations:**\n"
        analysis += "- Review the data for trends and patterns\n"
        analysis += "- Consider drilling down into specific categories\n"
        analysis += "- Monitor key metrics over time\n"
        
        # Determine if visualization would be helpful
        visualization = None
        if len(result_df) > 1 and len(result_df.columns) >= 2:
            try:
                # Try to create a simple visualization
                if len(result_df.columns) == 2:
                    # Bar chart for two columns
                    fig = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[1], 
                               title=f"Analysis: {user_question[:50]}...")
                    visualization = fig
                elif len(result_df.columns) >= 3:
                    # Scatter plot for multiple columns
                    fig = px.scatter(result_df, x=result_df.columns[0], y=result_df.columns[1], 
                                   color=result_df.columns[2] if len(result_df.columns) > 2 else None,
                                   title=f"Analysis: {user_question[:50]}...")
                    visualization = fig
            except:
                visualization = None
        
        return {
            'data': result_df,
            'analysis': analysis,
            'visualization': visualization
        }
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return None

def display_quick_insights():
    """Display quick insights and metrics"""
    conn = get_database_connection()
    if not conn:
        return
    
    try:
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_pa = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."PA DATA"').fetchone()[0]
            st.metric("📋 Total PA Requests", f"{total_pa:,}")
        
        with col2:
            total_claims = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."CLAIMS DATA"').fetchone()[0]
            st.metric("💰 Total Claims", f"{total_claims:,}")
        
        with col3:
            total_groups = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."GROUPS"').fetchone()[0]
            st.metric("🏢 Active Groups", f"{total_groups:,}")
        
        with col4:
            total_providers = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."PROVIDERS"').fetchone()[0]
            st.metric("🏥 Network Providers", f"{total_providers:,}")
        
        conn.close()
    except Exception as e:
        st.error(f"Error loading quick insights: {e}")

def main():
    """Main application function"""
    
    # Header
    st.markdown('<h1 class="main-header">🏥 AI Health Insurance Analyst (Basic)</h1>', unsafe_allow_html=True)
    st.markdown("### Powered by Pattern Matching | Your Health Insurance Data Analyst")
    
    # Quick insights
    display_quick_insights()
    
    # Main query interface
    st.markdown("### 💬 Ask Your Health Insurance Question")
    
    # Example questions
    with st.expander("💡 Example Questions", expanded=False):
        st.markdown("""
        **Basic Queries:**
        - "How many total PA requests do we have?"
        - "How many claims are in the system?"
        - "How many groups are active?"
        - "How many providers are in the network?"
        - "How many members do we have?"
        
        **Analysis Queries:**
        - "What are the top groups by spending?"
        - "What are the top providers by activity?"
        - "What's the PA approval rate?"
        - "What are the most common procedures?"
        - "Show me spending trends over time"
        """)
    
    # Query input
    user_question = st.text_area(
        "Enter your question:",
        placeholder="e.g., How many total PA requests do we have?",
        height=100
    )
    
    col1, col2 = st.columns([1, 4])
    
    with col1:
        analyze_button = st.button("🔍 Analyze", type="primary")
    
    with col2:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    # Process query
    if analyze_button and user_question:
        with st.spinner("🤖 Analyzing your question..."):
            # Get database schema
            schema_info = get_database_schema()
            if not schema_info:
                st.error("Failed to load database schema")
                return
            
            # Generate SQL query
            sql_query = generate_basic_sql_query(user_question, schema_info)
            
            if sql_query:
                # Display the generated SQL
                st.markdown("### 📝 Generated SQL Query")
                st.code(sql_query, language="sql")
                
                # Execute query and analyze
                result = execute_query_and_analyze(sql_query, user_question)
                
                if result:
                    # Display analysis
                    st.markdown("### 🧠 Analysis")
                    st.markdown(f'<div class="ai-response">{result["analysis"]}</div>', unsafe_allow_html=True)
                    
                    # Display visualization if available
                    if result['visualization']:
                        st.markdown("### 📊 Visualization")
                        st.plotly_chart(result['visualization'], use_container_width=True)
                    
                    # Display data
                    st.markdown("### 📋 Query Results")
                    st.dataframe(result['data'], use_container_width=True)
                    
                    # Download option
                    csv = result['data'].to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results as CSV",
                        data=csv,
                        file_name=f"health_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**Database Info:**")
    schema_info = get_database_schema()
    if schema_info:
        total_tables = len(schema_info)
        total_rows = sum(info['row_count'] for info in schema_info.values())
        st.sidebar.metric("Tables", total_tables)
        st.sidebar.metric("Total Rows", f"{total_rows:,}")
    
    st.sidebar.markdown("**AI Health Insurance Analyst (Basic)**")
    st.sidebar.markdown("Built with Streamlit & DuckDB")
    st.sidebar.markdown("Pattern-based query generation")

if __name__ == "__main__":
    main()
