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
    page_title="Cost Overview - Snowflake Dashboard",
    page_icon="💰",
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

st.title("💰 Account Cost Overview")
st.markdown("Track total costs and daily trends across your Snowflake account")

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
    # Get total costs for current period
    total_cost_query = f"""
    SELECT 
        SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) as current_compute,
        SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as current_cloud_services
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
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

    # Daily cost trend
    daily_cost_query = f"""
    SELECT 
        DATE_TRUNC('day', qa.START_TIME)::DATE as usage_date,
        SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) as compute_credits,
        SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as cloud_services_credits,
        COUNT(DISTINCT qa.USER_NAME) as active_users,
        COUNT(DISTINCT qa.WAREHOUSE_NAME) as active_warehouses
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
    INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
    WHERE qa.START_TIME >= '{start_date}' AND qa.START_TIME <= '{end_date}'
    GROUP BY 1
    ORDER BY 1
    """
    
    daily_costs = session.sql(daily_cost_query).to_pandas()
    
    if not daily_costs.empty:
        # Add total costs column
        daily_costs['total_credits'] = daily_costs['COMPUTE_CREDITS'] + daily_costs['CLOUD_SERVICES_CREDITS']
        
        # Create dual-axis chart
        fig = make_subplots(
            rows=1, cols=1,
            specs=[[{"secondary_y": True}]],
            subplot_titles=["Daily Cost Trends"]
        )
        
        # Add cost bars
        fig.add_trace(
            go.Bar(
                x=daily_costs['USAGE_DATE'],
                y=daily_costs['COMPUTE_CREDITS'],
                name='Compute Credits',
                marker_color='#1f77b4'
            ),
            secondary_y=False
        )
        
        fig.add_trace(
            go.Bar(
                x=daily_costs['USAGE_DATE'],
                y=daily_costs['CLOUD_SERVICES_CREDITS'],
                name='Cloud Services Credits',
                marker_color='#ff7f0e'
            ),
            secondary_y=False
        )
        
        # Add active users line
        fig.add_trace(
            go.Scatter(
                x=daily_costs['USAGE_DATE'],
                y=daily_costs['ACTIVE_USERS'],
                mode='lines+markers',
                name='Active Users',
                line=dict(color='#2ca02c', width=3),
                marker=dict(size=6)
            ),
            secondary_y=True
        )
        
        # Update layout
        fig.update_layout(
            title="Daily Costs and Activity",
            barmode='stack',
            height=500,
            template='plotly_white'
        )
        
        fig.update_xaxes(title_text="Date")
        fig.update_yaxes(title_text="Credits", secondary_y=False)
        fig.update_yaxes(title_text="Active Users", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)

    # Cost breakdown pie chart
    if not cost_summary.empty:
        cost_breakdown = pd.DataFrame({
            'Category': ['Compute', 'Cloud Services', 'AI Functions'],
            'Credits': [
                cost_summary['CURRENT_COMPUTE'].iloc[0],
                cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0], 
                ai_current
            ]
        })
        
        fig_pie = px.pie(
            cost_breakdown,
            values='Credits',
            names='Category',
            title="Cost Breakdown by Category",
            color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c']
        )
        
        st.plotly_chart(fig_pie, use_container_width=True)

else:
    st.error("Please select a valid date range to begin analysis.")


