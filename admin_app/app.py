# Import python packages
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Snowflake Cost Dashboard - Home",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .savings-opportunity {
        background-color: #fff3cd;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #ffc107;
    }

</style>
""", unsafe_allow_html=True)

st.title("🏠 Snowflake Cost Optimization Dashboard")
st.markdown("**Welcome to your Account Admin Dashboard** - Navigate through different analysis sections to track costs, identify savings opportunities, and optimize resource usage across your entire Snowflake account")

# Display Streamlit version
try:
    st.sidebar.info(f"Streamlit version {st.__version__}")
except:
    st.sidebar.info("Streamlit version: Unable to detect")

# Get active session
session = get_active_session()

# Sidebar configuration
st.sidebar.header("📊 Analysis Configuration")

# Date range selection
col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input(
        "Start Date",
        value=date.today() - timedelta(days=7),
        key="start_date"
    )
with col2:
    end_date = st.date_input(
        "End Date", 
        value=date.today() - timedelta(days = -1),
        key="end_date"
    )

# Dashboard overview and navigation
st.markdown("---")

if start_date and end_date and start_date <= end_date:
    # Quick overview section
    st.header("📊 Account Overview")
    
    # Get total costs for current period
    total_cost_query = f"""
        SELECT 
            SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) as current_compute,
            SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as current_cloud_services
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
        WHERE qa.START_TIME >= '{start_date}' AND qa.START_TIME <= '{end_date}'
        """
    
    # Get AI costs separately (may not be available in all accounts)
    ai_cost_query = f"""
    SELECT 
        COALESCE(SUM(cf.TOKEN_CREDITS), 0) as current_ai
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
    WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
    """
    
    cost_summary = session.sql(total_cost_query).to_pandas()
    
    # Try to get AI costs, handle gracefully if not available
    ai_current = 0
    try:
        ai_cost_result = session.sql(ai_cost_query).to_pandas()
        if not ai_cost_result.empty:
            ai_current = ai_cost_result['CURRENT_AI'].iloc[0] or 0
    except Exception as e:
        st.sidebar.warning("⚠️ AI cost tracking not available in this account")
        ai_current = 0
    
    if not cost_summary.empty:
        current_total = (
            cost_summary['CURRENT_COMPUTE'].iloc[0] + 
            cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0] + 
            ai_current
        )
        
        # Display key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Credits",
                f"{current_total:,.1f}"
            )
        
        with col2:
            st.metric(
                "Compute Credits",
                f"{cost_summary['CURRENT_COMPUTE'].iloc[0]:,.1f}"
            )
        
        with col3:
            st.metric(
                "Cloud Services Credits", 
                f"{cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0]:,.1f}"
            )
        
        with col4:
            st.metric(
                "AI Credits",
                f"{ai_current:,.1f}"
            )

    # Navigation section
    st.markdown("---")
    st.header("📋 Analysis Sections")
    st.markdown("Navigate to different analysis pages using the sidebar or click the links below:")
    
    # Create navigation cards
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 💰 Cost Overview
        - Daily cost trends and activity
        - Cost breakdown by category
        - Total account spend analysis
        
        ### 🏭 Warehouse Optimization 
        - Warehouse utilization analysis
        - Cost per query efficiency
        - Right-sizing recommendations
        
        ### 👥 User Cost Analysis
        - Top spending users
        - Query efficiency by user
        - User activity patterns
        
        ### 🗄️ Storage & Data Costs
        - Data scanning analysis
        - Storage cost tracking
        - Large query identification
        """)
    
    with col2:
        st.markdown("""
        ### 🤖 AI Cost Management
        - AI function usage tracking
        - Token and credit consumption
        - Model cost analysis
        
        ### ⚡ Query Efficiency
        - Query failure analysis
        - Performance optimization
        - Memory spillage detection
        
        ### 📈 Cost Forecasting
        - Future cost projections
        - Trend analysis
        - Budget planning scenarios
        
        ### 🎯 Savings Opportunities
        - Consolidated optimization recommendations
        - Implementation timeline
        - Potential savings calculation
        """)

    st.markdown("---")
    st.subheader("💡 Quick Tips")
    st.info("""
    **Getting Started:**
    - Use the sidebar to navigate between different analysis sections
    - Adjust the date range to analyze different time periods  
    - Each section provides specific insights and recommendations
    - Check the Savings Opportunities page for consolidated recommendations
    """)

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This dashboard requires ACCOUNTADMIN privileges and access to SNOWFLAKE.ACCOUNT_USAGE views. Navigate to different sections using the sidebar menu.")
