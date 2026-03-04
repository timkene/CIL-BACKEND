import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="Test App", page_icon="🧪", layout="wide")

st.title("🧪 Test Streamlit App")

st.write("This is a simple test to see if Streamlit is working...")

# Test database connection
try:
    conn = duckdb.connect('ai_driven_data.duckdb', read_only=True)
    st.success("✅ Database connection successful")
    
    # Test a simple query
    result = conn.execute('SELECT COUNT(*) FROM "AI DRIVEN DATA"."PA DATA"').fetchone()
    st.write(f"PA DATA rows: {result[0]:,}")
    
    conn.close()
except Exception as e:
    st.error(f"❌ Database error: {e}")

st.write("✅ Test completed successfully!")
