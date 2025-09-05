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
    page_title="AI Cost Management - Snowflake Dashboard",
    page_icon="🤖",
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

st.title("🤖 AI Cost Management")
st.markdown("Track AI function usage, costs, and optimization opportunities")

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
    # AI usage summary
    ai_summary_query = f"""
    WITH ai_summary AS (
        SELECT 
            DATE_TRUNC('day', qh.START_TIME)::DATE as usage_date,
            cf.FUNCTION_NAME,
            COALESCE(cf.MODEL_NAME, 'default') as model_name,
            SUM(cf.TOKENS) as total_tokens,
            SUM(cf.TOKEN_CREDITS) as total_credits,
            COUNT(*) as request_count,
            COUNT(DISTINCT qh.USER_NAME) as unique_users
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
        WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
        GROUP BY 1, 2, 3
    )
    SELECT 
        usage_date,
        function_name,
        model_name,
        total_tokens,
        ROUND(total_credits, 4) as credits,
        request_count,
        unique_users,
        ROUND(total_credits / NULLIF(request_count, 0), 6) as credits_per_request,
        ROUND(total_tokens / NULLIF(request_count, 0), 0) as tokens_per_request
    FROM ai_summary
    ORDER BY usage_date DESC, total_credits DESC
    """
    
    ai_data = session.sql(ai_summary_query).to_pandas()
    
    if not ai_data.empty:
        # AI cost metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_ai_credits = ai_data['CREDITS'].sum()
            st.metric("Total AI Credits", f"{total_ai_credits:.3f}")
        
        with col2:
            total_tokens = ai_data['TOTAL_TOKENS'].sum()
            st.metric("Total Tokens", f"{total_tokens:,}")
        
        with col3:
            total_requests = ai_data['REQUEST_COUNT'].sum()
            st.metric("Total AI Requests", f"{total_requests:,}")
        
        with col4:
            avg_cost_per_request = ai_data['CREDITS_PER_REQUEST'].mean()
            st.metric("Avg Cost/Request", f"{avg_cost_per_request:.6f}")
        
        # AI usage trends
        daily_ai = ai_data.groupby('USAGE_DATE').agg({
            'CREDITS': 'sum',
            'TOTAL_TOKENS': 'sum',
            'REQUEST_COUNT': 'sum',
            'UNIQUE_USERS': 'sum'
        }).reset_index()
        
        # Format dates as YYYY-MM-DD strings
        daily_ai['USAGE_DATE_STR'] = pd.to_datetime(daily_ai['USAGE_DATE']).dt.strftime('%Y-%m-%d')
        
        fig_ai_trend = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Daily AI Credits", "Daily Token Usage", "Daily Requests", "Daily Active Users"),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        fig_ai_trend.add_trace(
            go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['CREDITS'], mode='lines+markers', name='Credits'),
            row=1, col=1
        )
        
        fig_ai_trend.add_trace(
            go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['TOTAL_TOKENS'], mode='lines+markers', name='Tokens'),
            row=1, col=2
        )
        
        fig_ai_trend.add_trace(
            go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['REQUEST_COUNT'], mode='lines+markers', name='Requests'),
            row=2, col=1
        )
        
        fig_ai_trend.add_trace(
            go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['UNIQUE_USERS'], mode='lines+markers', name='Users'),
            row=2, col=2
        )
        
        fig_ai_trend.update_layout(
            height=600, 
            title_text="AI Usage Trends", 
            showlegend=False,
            xaxis_tickformat='%Y-%m-%d',
            xaxis2_tickformat='%Y-%m-%d',
            xaxis3_tickformat='%Y-%m-%d',
            xaxis4_tickformat='%Y-%m-%d'
        )
        st.plotly_chart(fig_ai_trend, use_container_width=True)
        
        # Function and model breakdown
        col1, col2 = st.columns(2)
        
        with col1:
            function_costs = ai_data.groupby('FUNCTION_NAME')['CREDITS'].sum().reset_index()
            fig_functions = px.pie(
                function_costs,
                values='CREDITS',
                names='FUNCTION_NAME',
                title="AI Credits by Function"
            )
            st.plotly_chart(fig_functions, use_container_width=True)
        
        with col2:
            model_costs = ai_data.groupby('MODEL_NAME')['CREDITS'].sum().reset_index()
            fig_models = px.pie(
                model_costs,
                values='CREDITS', 
                names='MODEL_NAME',
                title="AI Credits by Model"
            )
            st.plotly_chart(fig_models, use_container_width=True)
        
        # Detailed AI usage table
        st.subheader("📊 Detailed AI Usage")
        st.dataframe(ai_data, use_container_width=True)
        
        # AI optimization recommendations
        st.subheader("💡 AI Cost Optimization")
        
        # High cost per request
        expensive_ai = ai_data[ai_data['CREDITS_PER_REQUEST'] > ai_data['CREDITS_PER_REQUEST'].quantile(0.9)]
        if not expensive_ai.empty:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>💰 High Cost AI Operations</h4>
                <p>These AI function/model combinations have high cost per request:</p>
            </div>
            """, unsafe_allow_html=True)
            
            for _, op in expensive_ai.head(5).iterrows():
                st.warning(f"🤖 **{op['FUNCTION_NAME']} ({op['MODEL_NAME']})**: {op['CREDITS_PER_REQUEST']:.6f} credits/request - {op['REQUEST_COUNT']} requests")

    else:
        st.info("No AI usage detected for the selected time period.")

else:
    st.error("Please select a valid date range to begin analysis.")


