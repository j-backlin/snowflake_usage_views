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
            service_type,
            SUM(credits_used) AS TOTAL_CREDITS,
            SUM(credits_used_compute) AS CURRENT_COMPUTE,
            SUM(credits_used_cloud_services) AS CURRENT_CLOUD_SERVICES
        FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_DAILY_HISTORY
        WHERE usage_date >= '{start_date}' AND usage_date <= '{end_date}'
        GROUP BY service_type
        ORDER BY TOTAL_CREDITS DESC
        """
    
    cost_summary = session.sql(total_cost_query).to_pandas()
    
    # Get AI costs from the same dataframe by filtering for AI_SERVICES
    ai_services_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'AI_SERVICES']
    ai_current = ai_services_data['TOTAL_CREDITS'].sum() if not ai_services_data.empty else 0
    
    # Get Warehouse costs from the same dataframe
    warehouse_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'WAREHOUSE_METERING']
    warehouse_current = warehouse_data['TOTAL_CREDITS'].sum() if not warehouse_data.empty else 0
    
    # Get Data Quality Monitoring costs from the same dataframe
    dq_monitoring_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'DATA_QUALITY_MONITORING']
    dq_current = dq_monitoring_data['TOTAL_CREDITS'].sum() if not dq_monitoring_data.empty else 0
    
    # Get Serverless Task costs
    serverless_task_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'SERVERLESS_TASK']
    serverless_task_current = serverless_task_data['TOTAL_CREDITS'].sum() if not serverless_task_data.empty else 0
    
    # Get Copy Files costs
    copy_files_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'COPY_FILES']
    copy_files_current = copy_files_data['TOTAL_CREDITS'].sum() if not copy_files_data.empty else 0
    
    # Get Auto Clustering costs
    auto_clustering_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'AUTO_CLUSTERING']
    auto_clustering_current = auto_clustering_data['TOTAL_CREDITS'].sum() if not auto_clustering_data.empty else 0
    
    # Get Telemetry Data Ingest costs
    telemetry_data_ingest_data = cost_summary[cost_summary['SERVICE_TYPE'] == 'TELEMETRY_DATA_INGEST']
    telemetry_data_ingest_current = telemetry_data_ingest_data['TOTAL_CREDITS'].sum() if not telemetry_data_ingest_data.empty else 0
    
    if not cost_summary.empty:
        current_total = cost_summary['TOTAL_CREDITS'].sum()
        
        # Display key metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Total Credits",
                f"{current_total:,.1f}"
            )
        
        with col2:
            st.metric(
                "Warehouse Credits",
                f"{warehouse_current:,.1f}"
            )
        
        with col3:
            st.metric(
                "Data Quality Monitoring Credits", 
                f"{dq_current:,.1f}"
            )
        
        with col4:
            st.metric(
                "AI Credits",
                f"{ai_current:,.1f}"
            )
        
        # Second row of metrics for additional service types
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            st.metric(
                "Serverless Task Credits",
                f"{serverless_task_current:,.1f}"
            )
        
        with col6:
            st.metric(
                "Copy Files Credits",
                f"{copy_files_current:,.1f}"
            )
        
        with col7:
            st.metric(
                "Auto Clustering Credits",
                f"{auto_clustering_current:,.1f}"
            )
        
        with col8:
            st.metric(
                "Telemetry Data Ingest Credits",
                f"{telemetry_data_ingest_current:,.1f}"
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
