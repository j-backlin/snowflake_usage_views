# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Snowflake User Dashboard - Home",
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
    .info-card {
        background-color: #e1f5fe;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #0288d1;
    }
</style>
""", unsafe_allow_html=True)

st.title("🏠 Snowflake User Cost & Performance Dashboard")
st.markdown("**Welcome to your Personal Dashboard** - Track your individual Snowflake usage, costs, and performance metrics across different analysis sections")

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
        value=date.today() - timedelta(days=0),
        key="end_date"
    )

# Dashboard overview and navigation
st.markdown("---")

if start_date and end_date and start_date <= end_date:
    # Quick overview section
    st.header("📊 Your Usage Overview")
    
    # Get basic usage stats for current period
    try:
        overview_query = f"""
        SELECT 
            COUNT(DISTINCT query_id) as total_queries,
            ROUND(SUM(COALESCE(CREDITS_ATTRIBUTED_COMPUTE, 0)), 3) as compute_credits,
            ROUND(SUM(COALESCE(CREDITS_USED_CLOUD_SERVICES, 0)), 3) as cs_credits,
            ROUND(AVG(total_elapsed_time)/1000, 2) as avg_execution_seconds
        FROM snowflake_copy_cost_views.account_usage.query_history q
        LEFT JOIN snowflake_copy_cost_views.account_usage.query_attribution_history qa ON q.query_id = qa.query_id
        WHERE q.start_time >= '{start_date}' AND q.start_time <= '{end_date}'
        """
        
        overview_df = session.sql(overview_query).to_pandas()
        
        if not overview_df.empty:
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Queries",
                    f"{int(overview_df['TOTAL_QUERIES'].iloc[0]):,}"
                )
            
            with col2:
                st.metric(
                    "Compute Credits",
                    f"{overview_df['COMPUTE_CREDITS'].iloc[0]:,.3f}"
                )
            
            with col3:
                st.metric(
                    "Cloud Services Credits", 
                    f"{overview_df['CS_CREDITS'].iloc[0]:,.3f}"
                )
            
            with col4:
                st.metric(
                    "Avg Query Time (sec)",
                    f"{overview_df['AVG_EXECUTION_SECONDS'].iloc[0]:,.2f}"
                )
    except Exception as e:
        st.warning("⚠️ Unable to load overview metrics. Please check your database access.")

    # Navigation section
    st.markdown("---")
    st.header("📋 Analysis Sections")
    st.markdown("Navigate to different analysis pages using the sidebar or explore the sections below:")
    
    # Create navigation cards
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### 🏭 Warehouse Usage
        - Daily warehouse credit usage
        - Warehouse utilization trends
        - Usage patterns by warehouse
        
        ### 🤖 AI Usage Analysis
        - AI function usage tracking
        - Token consumption analysis  
        - Cortex Analyst usage
        
        ### 💾 Spillage Analysis
        - Queries that spilled to disk
        - Local vs remote spillage
        - Performance impact analysis
        
        ### 🔍 Query Details
        - Detailed query execution data
        - Performance metrics
        - Error tracking
        """)
    
    with col2:
        st.markdown("""
        ### 🤖 AI Query Details
        - AI-specific query analysis
        - Model and token usage
        - Function performance tracking
        
        ### 💰 Expensive Queries
        - Highest cost queries
        - Resource consumption analysis
        - Cost optimization opportunities
        
        ### ☁️ Cloud Services Breakdown
        - Cloud services usage by type
        - Query compilation analysis
        - Service cost breakdown
        
        ### ⚡ Resource Utilization
        - Warehouse efficiency metrics
        - Query success rates
        - Data scanning patterns
        """)

    st.markdown("---")
    st.subheader("💡 Quick Tips")
    st.info("""
    **Getting Started:**
    - Use the sidebar to navigate between different analysis sections
    - Adjust the date range to analyze different time periods  
    - Each section provides specific insights about your usage patterns
    - Monitor your resource consumption and identify optimization opportunities
    """)

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This dashboard shows your personal Snowflake usage and performance metrics. Navigate to different sections using the sidebar menu.")
