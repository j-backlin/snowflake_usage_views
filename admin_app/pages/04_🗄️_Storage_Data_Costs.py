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
    page_title="Storage & Data Costs - Snowflake Dashboard",
    page_icon="🗄️",
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

st.title("🗄️ Storage & Data Costs")
st.markdown("Analyze data transfer and scanning costs across your Snowflake account")
st.info("📝 Storage cost analysis requires additional Snowflake account usage views. This section shows data transfer and scanning costs.")

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
    # Data scanning analysis
    data_scanning_query = f"""
    SELECT 
        DATE_TRUNC('day', q.START_TIME)::DATE as usage_date,
        SUM(qh.BYTES_SCANNED) / POWER(1024, 4) as total_tb_scanned,
        COUNT(DISTINCT q.QUERY_ID) as queries_with_scanning,
        AVG(qh.BYTES_SCANNED) / POWER(1024, 3) as avg_gb_per_query,
        SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as scanning_related_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
    JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
    WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
    AND qh.BYTES_SCANNED > 0
    GROUP BY 1
    ORDER BY 1
    """
    
    scanning_data = session.sql(data_scanning_query).to_pandas()
    
    if not scanning_data.empty:
        # Data scanning metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_tb = scanning_data['TOTAL_TB_SCANNED'].sum()
            st.metric("Total TB Scanned", f"{total_tb:.2f} TB")
        
        with col2:
            avg_gb = scanning_data['AVG_GB_PER_QUERY'].mean()
            st.metric("Avg GB per Query", f"{avg_gb:.2f} GB")
        
        with col3:
            total_queries = scanning_data['QUERIES_WITH_SCANNING'].sum()
            st.metric("Queries with Data Scanning", f"{total_queries:,}")
        
        # Data scanning trends
        fig_scanning = px.line(
            scanning_data,
            x='USAGE_DATE',
            y='TOTAL_TB_SCANNED',
            title="Daily Data Scanning Volume (TB)",
            markers=True
        )
        st.plotly_chart(fig_scanning, use_container_width=True)
        
        # Scanning efficiency
        fig_efficiency = make_subplots(
            rows=1, cols=1,
            specs=[[{"secondary_y": True}]],
            subplot_titles=["Data Scanning vs Compute Credits"]
        )
        
        fig_efficiency.add_trace(
            go.Bar(
                x=scanning_data['USAGE_DATE'],
                y=scanning_data['TOTAL_TB_SCANNED'],
                name='TB Scanned',
                marker_color='lightblue'
            ),
            secondary_y=False
        )
        
        fig_efficiency.add_trace(
            go.Scatter(
                x=scanning_data['USAGE_DATE'],
                y=scanning_data['SCANNING_RELATED_CREDITS'],
                mode='lines+markers',
                name='Credits',
                line=dict(color='red', width=3)
            ),
            secondary_y=True
        )
        
        fig_efficiency.update_yaxes(title_text="TB Scanned", secondary_y=False)
        fig_efficiency.update_yaxes(title_text="Credits", secondary_y=True)
        fig_efficiency.update_layout(title="Data Scanning vs Compute Costs")
        
        st.plotly_chart(fig_efficiency, use_container_width=True)
        
        # Large scan queries
        large_scan_query = f"""
        SELECT 
            q.USER_NAME,
            qh.QUERY_TEXT,
            qh.BYTES_SCANNED / POWER(1024, 3) as gb_scanned,
            q.CREDITS_ATTRIBUTED_COMPUTE as credits,
            qh.EXECUTION_TIME / 1000 as execution_seconds,
            q.START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND qh.BYTES_SCANNED > 0
        ORDER BY qh.BYTES_SCANNED DESC
        LIMIT 20
        """
        
        large_scans = session.sql(large_scan_query).to_pandas()
        
        if not large_scans.empty:
            st.subheader("🔍 Top 20 Queries by Data Scanned")
            # Truncate query text for display
            large_scans['QUERY_TEXT_DISPLAY'] = large_scans['QUERY_TEXT'].str[:100] + '...'
            display_cols = ['USER_NAME', 'QUERY_TEXT_DISPLAY', 'GB_SCANNED', 'CREDITS', 'EXECUTION_SECONDS', 'START_TIME']
            st.dataframe(large_scans[display_cols], use_container_width=True)

else:
    st.error("Please select a valid date range to begin analysis.")


