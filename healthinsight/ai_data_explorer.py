import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="AI DRIVEN DATA Explorer",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    .schema-info {
        background: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
        margin: 1rem 0;
    }
    .table-header {
        background: #1f77b4;
        color: white;
        padding: 0.5rem;
        border-radius: 5px 5px 0 0;
        font-weight: bold;
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
def get_table_info():
    """Get information about tables in the database"""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        # Get table list using information_schema
        tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'AI DRIVEN DATA'
        """).fetchall()
        
        table_info = {}
        
        for table in tables:
            table_name = table[0]
            if table_name != 'table_documentation':  # Skip documentation table
                # Get row count
                row_count = conn.execute(f'SELECT COUNT(*) FROM "AI DRIVEN DATA"."{table_name}"').fetchone()[0]
                
                # Get column info
                columns = conn.execute(f'DESCRIBE "AI DRIVEN DATA"."{table_name}"').fetchall()
                
                table_info[table_name] = {
                    'row_count': row_count,
                    'columns': [{'name': col[0], 'type': col[1]} for col in columns]
                }
        
        conn.close()
        return table_info
    except Exception as e:
        st.error(f"Error getting table info: {e}")
        return None

@st.cache_data
def get_pa_data_sample(limit=100):
    """Get sample data from PA DATA table"""
    conn = get_database_connection()
    if not conn:
        return None
    
    try:
        query = f'SELECT * FROM "AI DRIVEN DATA"."PA DATA" LIMIT {limit}'
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error loading PA data: {e}")
        return None

def display_database_overview():
    """Display database overview and statistics"""
    st.markdown('<h1 class="main-header">🤖 AI DRIVEN DATA Explorer</h1>', unsafe_allow_html=True)
    
    # Database info
    table_info = get_table_info()
    if not table_info:
        st.error("Unable to load database information")
        return
    
    # Schema information
    st.markdown('<div class="schema-info">', unsafe_allow_html=True)
    st.markdown("### 📊 Database Schema: AI DRIVEN DATA")
    st.markdown("This database contains healthcare and insurance data for AI-driven analysis and insights.")
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Table overview
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("📋 Total Tables", len(table_info))
    
    with col2:
        total_rows = sum(info['row_count'] for info in table_info.values())
        st.metric("📈 Total Rows", f"{total_rows:,}")
    
    with col3:
        st.metric("🗄️ Database Size", "AI DRIVEN DATA")
    
    # Table details
    st.markdown("### 📋 Available Tables")
    
    for table_name, info in table_info.items():
        with st.expander(f"📊 {table_name} ({info['row_count']:,} rows)"):
            st.markdown(f"**Columns ({len(info['columns'])}):**")
            
            # Display columns in a nice format
            col_cols = st.columns(3)
            for i, col in enumerate(info['columns']):
                with col_cols[i % 3]:
                    st.markdown(f"• **{col['name']}** ({col['type']})")

def display_pa_data_analysis():
    """Display PA DATA analysis and visualizations"""
    st.markdown("### 📊 PA DATA Analysis")
    
    # Load data
    df = get_pa_data_sample(1000)  # Load more data for analysis
    if df is None:
        st.error("Unable to load PA data")
        return
    
    # Convert date column
    if 'requestdate' in df.columns:
        df['requestdate'] = pd.to_datetime(df['requestdate'])
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total PA Requests", len(df))
    
    with col2:
        approved_count = len(df[df['pastatus'] == 'APPROVED']) if 'pastatus' in df.columns else 0
        st.metric("Approved Requests", approved_count)
    
    with col3:
        total_granted = df['granted'].sum() if 'granted' in df.columns else 0
        st.metric("Total Amount Granted", f"₦{total_granted:,.2f}")
    
    with col4:
        avg_granted = df['granted'].mean() if 'granted' in df.columns else 0
        st.metric("Average Grant Amount", f"₦{avg_granted:,.2f}")
    
    # Visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Status distribution
        if 'pastatus' in df.columns:
            status_counts = df['pastatus'].value_counts()
            fig_status = px.pie(
                values=status_counts.values,
                names=status_counts.index,
                title="PA Request Status Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        # Division distribution
        if 'divisionname' in df.columns:
            division_counts = df['divisionname'].value_counts()
            fig_division = px.bar(
                x=division_counts.index,
                y=division_counts.values,
                title="Requests by Division",
                color=division_counts.values,
                color_continuous_scale="Blues"
            )
            fig_division.update_layout(showlegend=False)
            st.plotly_chart(fig_division, use_container_width=True)
    
    # Time series analysis
    if 'requestdate' in df.columns:
        st.markdown("#### 📈 Request Trends Over Time")
        
        # Group by date and count requests
        daily_requests = df.groupby(df['requestdate'].dt.date).size().reset_index()
        daily_requests.columns = ['date', 'count']
        
        fig_trend = px.line(
            daily_requests,
            x='date',
            y='count',
            title="Daily PA Request Volume",
            color_discrete_sequence=['#1f77b4']
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    
    # Amount analysis
    if 'granted' in df.columns:
        st.markdown("#### 💰 Grant Amount Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Amount distribution
            fig_hist = px.histogram(
                df,
                x='granted',
                title="Distribution of Grant Amounts",
                nbins=20,
                color_discrete_sequence=['#2ca02c']
            )
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Top groups by amount
            if 'groupname' in df.columns:
                group_amounts = df.groupby('groupname')['granted'].sum().sort_values(ascending=False).head(10)
                fig_groups = px.bar(
                    x=group_amounts.values,
                    y=group_amounts.index,
                    orientation='h',
                    title="Top 10 Groups by Total Grant Amount",
                    color=group_amounts.values,
                    color_continuous_scale="Greens"
                )
                st.plotly_chart(fig_groups, use_container_width=True)

def display_data_table():
    """Display the actual data table"""
    st.markdown("### 📋 PA DATA Table")
    
    # Load data
    df = get_pa_data_sample(100)
    if df is None:
        st.error("Unable to load PA data")
        return
    
    # Column descriptions
    st.markdown("""
    **Column Descriptions (Complete Real Data - 15 columns):**
    - **panumber**: Unique authorization code (can appear multiple times for multiple procedures)
    - **groupname**: Client/company name (unique per client)
    - **divisionname**: Geographic division (not unique)
    - **plancode**: Insurance plan code (unique per plan)
    - **IID**: Unique identifier for each enrollee
    - **providerid**: Unique identifier for healthcare provider
    - **requestdate**: Date when authorization was granted
    - **pastatus**: Status (AUTHORIZED, PENDING, REJECTED)
    - **code**: Procedure code (unique per procedure)
    - **userid**: Agent/authorizer who granted the authorization
    - **totaltariff**: Total tariff amount for the procedure
    - **benefitcode**: Benefit code associated with the procedure
    - **dependantnumber**: Dependent code (0=principal, 1=spouse, 2-5=children)
    - **requested**: Amount requested for the procedure
    - **granted**: Amount actually authorized/granted for the procedure
    """)
    
    # Display table
    st.dataframe(df, use_container_width=True)
    
    # Download option
    csv = df.to_csv(index=False)
    st.download_button(
        label="📥 Download PA DATA as CSV",
        data=csv,
        file_name=f"pa_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

def main():
    """Main function"""
    # Sidebar navigation
    st.sidebar.title("🤖 Navigation")
    page = st.sidebar.selectbox(
        "Choose a page:",
        ["Database Overview", "PA DATA Analysis", "Data Table"]
    )
    
    # Display selected page
    if page == "Database Overview":
        display_database_overview()
    elif page == "PA DATA Analysis":
        display_pa_data_analysis()
    elif page == "Data Table":
        display_data_table()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**AI DRIVEN DATA Explorer**")
    st.sidebar.markdown("Built with Streamlit & DuckDB")
    st.sidebar.markdown("Schema: AI DRIVEN DATA")

if __name__ == "__main__":
    main()
