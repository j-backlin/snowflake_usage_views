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
    page_title="User Cost Analysis - Snowflake Dashboard",
    page_icon="👥",
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

st.title("👥 User Cost Analysis")
st.markdown("Analyze user activity, costs, and identify optimization opportunities")

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
    # User cost analysis
    user_cost_query = f"""
    WITH user_costs AS (
        SELECT 
            q.USER_NAME,
            SUM(q.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(q.CREDITS_USED_QUERY_ACCELERATION, 0)) as compute_credits,
            SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as cloud_services_credits,
            COUNT(DISTINCT q.QUERY_ID) as total_queries,
            COUNT(DISTINCT q.WAREHOUSE_NAME) as warehouses_used,
            AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
            SUM(CASE WHEN qh.EXECUTION_STATUS = 'FAILED' THEN 1 ELSE 0 END) as failed_queries,
            COUNT(DISTINCT DATE_TRUNC('day', q.START_TIME)) as active_days
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        GROUP BY 1
    ),
    user_ai_costs AS (
        SELECT 
            qh.USER_NAME,
            SUM(cf.TOKEN_CREDITS) as ai_credits,
            COUNT(*) as ai_queries
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
        WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
        GROUP BY 1
    )
    SELECT 
        uc.USER_NAME,
        ROUND(uc.compute_credits + uc.cloud_services_credits + COALESCE(ac.ai_credits, 0), 2) as "Total Credits",
        ROUND(uc.compute_credits, 2) as "Compute Credits",
        ROUND(uc.cloud_services_credits, 2) as "Cloud Services Credits", 
        ROUND(COALESCE(ac.ai_credits, 0), 2) as "AI Credits",
        uc.total_queries as "Total Queries",
        COALESCE(ac.ai_queries, 0) as "AI Queries",
        uc.warehouses_used as "Warehouses Used",
        ROUND(uc.avg_execution_seconds, 2) as "Avg Execution (sec)",
        uc.failed_queries as "Failed Queries",
        uc.active_days as "Active Days",
        ROUND((uc.compute_credits + uc.cloud_services_credits + COALESCE(ac.ai_credits, 0)) / NULLIF(uc.total_queries + COALESCE(ac.ai_queries, 0), 0), 4) as "Credits per Query"
    FROM user_costs uc
    LEFT JOIN user_ai_costs ac ON uc.USER_NAME = ac.USER_NAME
    ORDER BY "Total Credits" DESC
    """
    
    user_data = session.sql(user_cost_query).to_pandas()
    
    if not user_data.empty:
        # Top users summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Users", len(user_data))
        with col2:
            st.metric("Top User Cost", f"{user_data['Total Credits'].max():,.1f} credits")
        with col3:
            avg_cost = user_data['Total Credits'].mean()
            st.metric("Average User Cost", f"{avg_cost:.1f} credits")
        
        # Top spenders
        st.subheader("💸 Top 20 Cost Contributors")
        top_users = user_data.head(20)
        
        fig_users = px.bar(
            top_users,
            x='USER_NAME',
            y='Total Credits',
            color='Credits per Query',
            title="Top 20 Users by Total Credits",
            color_continuous_scale='Reds',
            hover_data=['Total Queries', 'AI Credits', 'Failed Queries']
        )
        fig_users.update_xaxes(tickangle=45)
        st.plotly_chart(fig_users, use_container_width=True)
        
        # User details table
        st.subheader("📊 User Cost Details")
        st.dataframe(user_data, use_container_width=True)
        
        # Cost distribution analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost distribution histogram
            fig_dist = px.histogram(
                user_data,
                x='Total Credits',
                nbins=20,
                title="User Cost Distribution",
                labels={'count': 'Number of Users'}
            )
            st.plotly_chart(fig_dist, use_container_width=True)
        
        with col2:
            # Efficiency vs Activity scatter
            fig_scatter = px.scatter(
                user_data,
                x='Total Queries',
                y='Credits per Query',
                size='Total Credits',
                color='Failed Queries',
                title="User Efficiency: Credits per Query vs Activity",
                hover_data=['USER_NAME', 'AI Credits']
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        # User optimization recommendations
        st.subheader("💡 User Optimization Opportunities")
        
        # High cost per query users
        high_cost_users = user_data[user_data['Credits per Query'] > user_data['Credits per Query'].quantile(0.9)]
        if not high_cost_users.empty:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>⚠️ Users with High Cost per Query</h4>
                <p>These users may benefit from query optimization training:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for _, user in high_cost_users.head(5).iterrows():
                st.warning(f"👤 **{user['USER_NAME']}**: {user['Credits per Query']:.4f} credits/query - {user['Total Queries']} queries, {user['Failed Queries']} failures")
        
        # High failure rate users
        high_failure_users = user_data[user_data['Failed Queries'] > 10]
        if not high_failure_users.empty:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>🚨 Users with High Query Failure Rates</h4>
                <p>These users have many failed queries, wasting resources:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for _, user in high_failure_users.iterrows():
                failure_rate = (user['Failed Queries'] / user['Total Queries']) * 100
                st.error(f"👤 **{user['USER_NAME']}**: {user['Failed Queries']} failures ({failure_rate:.1f}% failure rate)")

else:
    st.error("Please select a valid date range to begin analysis.")


