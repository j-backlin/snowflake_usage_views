# Import python packages
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Warehouse Optimization - Snowflake Dashboard",
    page_icon="🏭",
    layout="wide"
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

st.title("🏭 Warehouse Optimization")
st.markdown("Analyze warehouse utilization, efficiency, and identify optimization opportunities")

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
        value=date.today() - timedelta(days=-1),
        key="end_date"
    )

if start_date and end_date and start_date <= end_date:
    # Warehouse utilization and cost analysis
    warehouse_analysis_query = f"""
    WITH warehouse_stats AS (
        SELECT 
            q.WAREHOUSE_NAME,
            DATE_TRUNC('day', q.START_TIME)::DATE as usage_date,
            SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as daily_credits,
            COUNT(DISTINCT q.QUERY_ID) as query_count,
            AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
            SUM(CASE WHEN qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) as successful_queries,
            COUNT(DISTINCT q.USER_NAME) as unique_users
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND q.WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1, 2
    ),
    warehouse_summary AS (
        SELECT 
            WAREHOUSE_NAME,
            SUM(daily_credits) as total_credits,
            AVG(daily_credits) as avg_daily_credits,
            SUM(query_count) as total_queries,
            AVG(avg_execution_seconds) as avg_execution_time,
            AVG(unique_users) as avg_daily_users,
            COUNT(DISTINCT usage_date) as active_days,
            MAX(daily_credits) as peak_daily_cost,
            MIN(daily_credits) as min_daily_cost
        FROM warehouse_stats
        GROUP BY 1
    )
    SELECT 
        WAREHOUSE_NAME,
        ROUND(total_credits, 2) as "Total Credits",
        ROUND(avg_daily_credits, 2) as "Avg Daily Credits",
        total_queries as "Total Queries", 
        ROUND(avg_execution_time, 2) as "Avg Execution (sec)",
        ROUND(avg_daily_users, 1) as "Avg Daily Users",
        active_days as "Active Days",
        ROUND(peak_daily_cost, 2) as "Peak Daily Cost",
        ROUND(total_credits / NULLIF(total_queries, 0), 4) as "Credits per Query"
    FROM warehouse_summary
    ORDER BY total_credits DESC
    """
    
    warehouse_data = session.sql(warehouse_analysis_query).to_pandas()
    
    if not warehouse_data.empty:
        st.subheader("Warehouse Cost Summary")
        st.dataframe(warehouse_data, use_container_width=True)
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            # Top 10 warehouses by cost
            top_warehouses = warehouse_data.head(10)
            fig_wh_cost = px.bar(
                top_warehouses,
                x='WAREHOUSE_NAME',
                y='Total Credits',
                title="Top 10 Warehouses by Total Credits",
                color='Total Credits',
                color_continuous_scale='Reds'
            )
            fig_wh_cost.update_xaxes(tickangle=45)
            st.plotly_chart(fig_wh_cost, use_container_width=True)
        
        with col2:
            # Efficiency scatter plot
            fig_efficiency = px.scatter(
                warehouse_data,
                x='Total Queries',
                y='Credits per Query',
                size='Total Credits',
                hover_data=['WAREHOUSE_NAME', 'Avg Daily Users'],
                title="Warehouse Efficiency: Credits per Query vs Query Volume",
                color='Avg Execution (sec)',
                color_continuous_scale='RdYlBu_r'
            )
            st.plotly_chart(fig_efficiency, use_container_width=True)
        
        # Optimization recommendations
        st.subheader("💡 Optimization Recommendations")
        
        # High cost per query warehouses
        high_cost_per_query = warehouse_data[warehouse_data['Credits per Query'] > warehouse_data['Credits per Query'].quantile(0.75)]
        if not high_cost_per_query.empty:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>⚠️ High Cost per Query Warehouses</h4>
                <p>These warehouses have high credits per query ratio - consider query optimization or right-sizing:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for _, wh in high_cost_per_query.iterrows():
                st.warning(f"🏭 **{wh['WAREHOUSE_NAME']}**: {wh['Credits per Query']:.4f} credits/query - Consider optimizing queries or warehouse size")
        
        # Low utilization warehouses  
        low_utilization = warehouse_data[warehouse_data['Avg Daily Users'] < 2]
        if not low_utilization.empty:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>📉 Low Utilization Warehouses</h4>
                <p>These warehouses have low user activity - consider consolidation:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for _, wh in low_utilization.iterrows():
                potential_savings = wh['Total Credits'] * 0.3  # Assume 30% savings from consolidation
                st.info(f"🏭 **{wh['WAREHOUSE_NAME']}**: {wh['Avg Daily Users']:.1f} avg users/day - Potential savings: {potential_savings:.1f} credits")

else:
    st.error("Please select a valid date range to begin analysis.")


