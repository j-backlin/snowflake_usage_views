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
    page_title="Query Efficiency - Snowflake Dashboard",
    page_icon="⚡",
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

st.title("⚡ Query Efficiency")
st.markdown("Analyze query performance, identify failures, and optimize execution efficiency")

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
    # Query efficiency analysis
    efficiency_query = f"""
    WITH query_efficiency AS (
        SELECT 
            q.QUERY_ID,
            q.USER_NAME,
            q.WAREHOUSE_NAME,
            qh.EXECUTION_TIME / 1000 as execution_seconds,
            qh.COMPILATION_TIME / 1000 as compilation_seconds,
            qh.BYTES_SCANNED / POWER(1024, 3) as gb_scanned,
            qh.ROWS_PRODUCED,
            q.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(q.CREDITS_USED_QUERY_ACCELERATION, 0) as total_credits,
            qh.EXECUTION_STATUS,
            qh.ERROR_CODE,
            qh.BYTES_SPILLED_TO_LOCAL_STORAGE + qh.BYTES_SPILLED_TO_REMOTE_STORAGE as total_spillage,
            qh.QUERY_TEXT
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND qh.EXECUTION_TIME > 0
    )
    SELECT 
        COUNT(*) as total_queries,
        SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) as failed_queries,
        SUM(CASE WHEN total_spillage > 0 THEN 1 ELSE 0 END) as queries_with_spillage,
        SUM(CASE WHEN execution_seconds > 300 THEN 1 ELSE 0 END) as long_running_queries,
        AVG(execution_seconds) as avg_execution_seconds,
        AVG(compilation_seconds) as avg_compilation_seconds,
        SUM(total_credits) as total_efficiency_credits,
        AVG(gb_scanned) as avg_gb_scanned
    FROM query_efficiency
    """
    
    efficiency_summary = session.sql(efficiency_query).to_pandas()
    
    if not efficiency_summary.empty:
        # Efficiency metrics
        row = efficiency_summary.iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            failure_rate = (row['FAILED_QUERIES'] / row['TOTAL_QUERIES']) * 100
            st.metric("Query Failure Rate", f"{failure_rate:.1f}%")
        
        with col2:
            spillage_rate = (row['QUERIES_WITH_SPILLAGE'] / row['TOTAL_QUERIES']) * 100
            st.metric("Queries with Spillage", f"{spillage_rate:.1f}%")
        
        with col3:
            long_query_rate = (row['LONG_RUNNING_QUERIES'] / row['TOTAL_QUERIES']) * 100
            st.metric("Long Running (>5min)", f"{long_query_rate:.1f}%")
        
        with col4:
            st.metric("Avg Execution Time", f"{row['AVG_EXECUTION_SECONDS']:.1f}s")
        
        # Problem queries analysis
        problem_queries_query = f"""
        SELECT 
            q.USER_NAME,
            q.WAREHOUSE_NAME,
            qh.EXECUTION_TIME / 1000 as execution_seconds,
            q.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(q.CREDITS_USED_QUERY_ACCELERATION, 0) as credits,
            qh.BYTES_SPILLED_TO_LOCAL_STORAGE + qh.BYTES_SPILLED_TO_REMOTE_STORAGE as spillage_bytes,
            qh.EXECUTION_STATUS,
            qh.ERROR_CODE,
            LEFT(qh.QUERY_TEXT, 200) as query_preview,
            q.START_TIME
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND (
            qh.EXECUTION_STATUS = 'FAILED' 
            OR qh.EXECUTION_TIME > 300000  -- 5 minutes
            OR (qh.BYTES_SPILLED_TO_LOCAL_STORAGE + qh.BYTES_SPILLED_TO_REMOTE_STORAGE) > 0
        )
        ORDER BY credits DESC
        LIMIT 50
        """
        
        problem_queries = session.sql(problem_queries_query).to_pandas()
        
        if not problem_queries.empty:
            st.subheader("🚨 Problem Queries Requiring Attention")
            
            # Create tabs for different problem types
            prob_tab1, prob_tab2, prob_tab3 = st.tabs(["Failed Queries", "Long Running", "Spillage"])
            
            with prob_tab1:
                failed = problem_queries[problem_queries['EXECUTION_STATUS'] == 'FAILED']
                if not failed.empty:
                    st.dataframe(failed[['USER_NAME', 'WAREHOUSE_NAME', 'ERROR_CODE', 'QUERY_PREVIEW', 'START_TIME']], use_container_width=True)
                else:
                    st.success("No failed queries in this period!")
            
            with prob_tab2:
                long_running = problem_queries[problem_queries['EXECUTION_SECONDS'] > 300]
                if not long_running.empty:
                    st.dataframe(long_running[['USER_NAME', 'WAREHOUSE_NAME', 'EXECUTION_SECONDS', 'CREDITS', 'QUERY_PREVIEW']], use_container_width=True)
                else:
                    st.success("No long running queries in this period!")
            
            with prob_tab3:
                spillage = problem_queries[problem_queries['SPILLAGE_BYTES'] > 0]
                if not spillage.empty:
                    spillage['spillage_gb'] = spillage['SPILLAGE_BYTES'] / (1024**3)
                    st.dataframe(spillage[['USER_NAME', 'WAREHOUSE_NAME', 'spillage_gb', 'CREDITS', 'QUERY_PREVIEW']], use_container_width=True)
                else:
                    st.success("No queries with spillage in this period!")
        
        # Efficiency recommendations
        st.subheader("💡 Query Optimization Recommendations")
        
        if row['FAILED_QUERIES'] > 0:
            wasted_credits = row['FAILED_QUERIES'] * 0.1  # Estimate waste from failures
            st.markdown(f"""
            <div class="savings-opportunity">
                <h4>🚨 Query Failures Impact</h4>
                <p>{row['FAILED_QUERIES']} failed queries detected. Estimated wasted credits: {wasted_credits:.2f}</p>
                <p><strong>Recommendation:</strong> Review error patterns and provide query optimization training.</p>
            </div>
            """, unsafe_allow_html=True)
        
        if row['QUERIES_WITH_SPILLAGE'] > 0:
            st.markdown("""
            <div class="savings-opportunity">
                <h4>⚠️ Memory Spillage Detected</h4>
                <p>Queries are spilling to disk, indicating memory pressure.</p>
                <p><strong>Recommendations:</strong></p>
                <ul>
                    <li>Consider larger warehouse sizes for complex queries</li>
                    <li>Optimize JOIN orders and WHERE clauses</li>
                    <li>Use query hints for better memory management</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

else:
    st.error("Please select a valid date range to begin analysis.")


