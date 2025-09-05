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
    page_title="Savings Opportunities - Snowflake Dashboard",
    page_icon="🎯",
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

st.title("🎯 Consolidated Savings Opportunities")
st.markdown("Identify and prioritize cost optimization opportunities across your Snowflake account")

st.markdown("""
<div class="metric-card">
    <h3>💡 Cost Optimization Summary</h3>
    <p>This section consolidates all identified savings opportunities across your Snowflake account.</p>
</div>
""", unsafe_allow_html=True)

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
    # Aggregate all savings opportunities
    savings_opportunities = []
    
    # Warehouse optimization savings
    warehouse_savings_query = f"""
    WITH warehouse_inefficiency AS (
        SELECT 
            q.WAREHOUSE_NAME,
            SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as total_credits,
            COUNT(DISTINCT q.QUERY_ID) as total_queries,
            AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
            COUNT(DISTINCT q.USER_NAME) as unique_users,
            COUNT(DISTINCT DATE_TRUNC('day', q.START_TIME)) as active_days
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND q.WAREHOUSE_NAME IS NOT NULL
        GROUP BY 1
    )
    SELECT 
        WAREHOUSE_NAME,
        total_credits,
        CASE 
            WHEN unique_users < 2 THEN total_credits * 0.3
            WHEN avg_execution_seconds > 120 THEN total_credits * 0.2  
            WHEN (total_credits / NULLIF(total_queries, 0)) > 0.01 THEN total_credits * 0.15
            ELSE 0
        END as potential_savings,
        CASE 
            WHEN unique_users < 2 THEN 'Low utilization - consider consolidation'
            WHEN avg_execution_seconds > 120 THEN 'Long execution times - consider optimization'
            WHEN (total_credits / NULLIF(total_queries, 0)) > 0.01 THEN 'High cost per query - review sizing'
            ELSE 'No issues identified'
        END as recommendation
    FROM warehouse_inefficiency
    WHERE total_credits > 1
    ORDER BY potential_savings DESC
    """
    
    wh_savings = session.sql(warehouse_savings_query).to_pandas()
    total_wh_savings = wh_savings['POTENTIAL_SAVINGS'].sum() if not wh_savings.empty else 0
    
    # Query efficiency savings
    query_savings_query = f"""
    SELECT 
        SUM(CASE WHEN qh.EXECUTION_STATUS = 'FAILED' THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.5 ELSE 0 END) as failed_query_waste,
        SUM(CASE WHEN (qh.BYTES_SPILLED_TO_LOCAL_STORAGE + qh.BYTES_SPILLED_TO_REMOTE_STORAGE) > 0 
            THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.2 ELSE 0 END) as spillage_waste,
        SUM(CASE WHEN qh.EXECUTION_TIME > 300000 THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.1 ELSE 0 END) as long_query_waste
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
    WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
    """
    
    query_savings = session.sql(query_savings_query).to_pandas()
    
    if not query_savings.empty:
        failed_savings = query_savings['FAILED_QUERY_WASTE'].iloc[0] or 0
        spillage_savings = query_savings['SPILLAGE_WASTE'].iloc[0] or 0
        long_query_savings = query_savings['LONG_QUERY_WASTE'].iloc[0] or 0
    else:
        failed_savings = spillage_savings = long_query_savings = 0
    
    # AI optimization savings
    ai_savings_query = f"""
    WITH expensive_ai AS (
        SELECT 
            SUM(cf.TOKEN_CREDITS) as total_ai_credits,
            AVG(cf.TOKEN_CREDITS / NULLIF(cf.TOKENS, 0)) as avg_cost_per_token
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
        WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
    )
    SELECT 
        total_ai_credits,
        total_ai_credits * 0.1 as potential_ai_savings  -- Assume 10% savings from optimization
    FROM expensive_ai
    """
    
    ai_savings_data = session.sql(ai_savings_query).to_pandas()
    ai_savings_amount = ai_savings_data['POTENTIAL_AI_SAVINGS'].iloc[0] if not ai_savings_data.empty else 0
    
    # Total savings calculation
    total_potential_savings = (
        total_wh_savings + 
        failed_savings + 
        spillage_savings + 
        long_query_savings + 
        ai_savings_amount
    )
    
    # Display savings summary
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Potential Savings", f"{total_potential_savings:.1f} credits")
    
    with col2:
        st.metric("Warehouse Optimization", f"{total_wh_savings:.1f} credits")
    
    with col3:
        query_total_savings = failed_savings + spillage_savings + long_query_savings
        st.metric("Query Optimization", f"{query_total_savings:.1f} credits")
    
    with col4:
        st.metric("AI Optimization", f"{ai_savings_amount:.1f} credits")
    
    # Savings breakdown chart
    if total_potential_savings > 0:
        savings_breakdown = pd.DataFrame({
            'Category': ['Warehouse Optimization', 'Query Failures', 'Memory Spillage', 'Long Queries', 'AI Optimization'],
            'Potential Savings': [total_wh_savings, failed_savings, spillage_savings, long_query_savings, ai_savings_amount]
        })
        
        fig_savings = px.pie(
            savings_breakdown,
            values='Potential Savings',
            names='Category',
            title="Potential Savings by Category",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        st.plotly_chart(fig_savings, use_container_width=True)
        
        # Detailed recommendations
        st.subheader("📋 Prioritized Action Plan")
        
        if total_wh_savings > 0:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>🏭 Warehouse Optimization (Highest Impact)</h4>
                <ul>
                    <li>Review underutilized warehouses for consolidation opportunities</li>
                    <li>Right-size warehouses based on actual workload patterns</li>
                    <li>Implement auto-suspend policies for idle warehouses</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        if query_total_savings > 0:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>⚡ Query Optimization</h4>
                <ul>
                    <li>Provide query performance training to high-cost users</li>
                    <li>Implement query monitoring and alerting</li>
                    <li>Review and optimize frequently failing queries</li>
                    <li>Address memory spillage through query tuning or warehouse sizing</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        if ai_savings_amount > 0:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>🤖 AI Cost Management</h4>
                <ul>
                    <li>Review AI function usage patterns for optimization opportunities</li>
                    <li>Consider model selection based on cost-effectiveness</li>
                    <li>Implement AI usage governance and monitoring</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        # Implementation timeline
        st.subheader("🗓️ Implementation Timeline")
        
        timeline_data = pd.DataFrame({
            'Priority': ['High', 'High', 'Medium', 'Medium', 'Low'],
            'Action': [
                'Consolidate underutilized warehouses',
                'Implement auto-suspend policies', 
                'Query optimization training',
                'AI usage governance',
                'Advanced query monitoring'
            ],
            'Estimated Savings': [
                total_wh_savings * 0.6,
                total_wh_savings * 0.4,
                query_total_savings * 0.7,
                ai_savings_amount,
                query_total_savings * 0.3
            ],
            'Timeline': ['1-2 weeks', '1 week', '2-4 weeks', '2-3 weeks', '4-6 weeks']
        })
        
        st.dataframe(timeline_data, use_container_width=True)
    
    else:
        st.success("🎉 Great! No major cost optimization opportunities identified. Your Snowflake account appears to be well-optimized!")
        
        st.markdown("""
        <div class="metric-card">
            <h4>✅ Best Practices Checklist</h4>
            <ul>
                <li>Regular monitoring of warehouse utilization</li>
                <li>Ongoing query performance optimization</li>
                <li>AI usage cost tracking</li>
                <li>User education on efficient query practices</li>
                <li>Periodic cost trend analysis</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

else:
    st.error("Please select a valid date range to begin analysis.")


