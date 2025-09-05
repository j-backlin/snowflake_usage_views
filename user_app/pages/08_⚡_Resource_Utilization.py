# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Resource Utilization",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ Resource Utilization & Efficiency")
st.markdown("Analyze warehouse efficiency, query success rates, and resource usage patterns")

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

if start_date and end_date and start_date <= end_date:
    st.header("Warehouse Efficiency Analysis")
    
    # Warehouse efficiency analysis
    query10 = """
    SELECT 
        warehouse_name,
        DATE_TRUNC('day', start_time)::DATE AS usage_date,
        COUNT(DISTINCT query_id) AS unique_queries,
        ROUND(AVG(execution_time)/1000, 2) AS avg_execution_seconds,
        ROUND(AVG(compilation_time)/1000, 2) AS avg_compilation_seconds,
        COUNT(CASE WHEN execution_status = 'FAILED' THEN 1 END) AS failed_queries,
        COUNT(CASE WHEN execution_status = 'SUCCESS' THEN 1 END) AS successful_queries,
        ROUND(AVG(bytes_scanned)/POWER(1024,3), 2) AS avg_gb_scanned,
        COUNT(CASE WHEN bytes_spilled_to_local_storage > 0 OR bytes_spilled_to_remote_storage > 0 THEN 1 END) AS queries_with_spillage
    FROM snowflake_copy_cost_views.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= '{0}' AND start_time <= '{1}'
        AND warehouse_name IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 2 DESC, 3 DESC;
    """.format(start_date, end_date)
    
    df10 = session.sql(query10).to_pandas()
    
    if not df10.empty:
        # Rename columns for better display
        df10_display = df10.rename(columns={
            "WAREHOUSE_NAME": "Warehouse",
            "USAGE_DATE": "Date", 
            "UNIQUE_QUERIES": "Unique Queries",
            "AVG_EXECUTION_SECONDS": "Avg Execution (sec)",
            "AVG_COMPILATION_SECONDS": "Avg Compilation (sec)",
            "FAILED_QUERIES": "Failed Queries",
            "SUCCESSFUL_QUERIES": "Successful Queries",
            "AVG_GB_SCANNED": "Avg GB Scanned",
            "QUERIES_WITH_SPILLAGE": "Queries with Spillage"
        })
        st.dataframe(df10_display)
        
        # Calculate success rate for visualization
        df10['success_rate'] = (df10['SUCCESSFUL_QUERIES'] / (df10['SUCCESSFUL_QUERIES'] + df10['FAILED_QUERIES'])) * 100
        
        # Efficiency visualization
        fig10 = px.scatter(df10,
            x='UNIQUE_QUERIES',
            y='AVG_EXECUTION_SECONDS',
            size='AVG_GB_SCANNED',
            color='WAREHOUSE_NAME',
            title="Warehouse Efficiency: Execution Time vs Query Volume",
            labels={'UNIQUE_QUERIES': 'Number of Queries', 'AVG_EXECUTION_SECONDS': 'Avg Execution Time (sec)'},
            template='plotly_dark')
        st.plotly_chart(fig10)
        
        # Success rate chart
        fig_success = px.bar(df10,
            x='USAGE_DATE',
            y='success_rate',
            color='WAREHOUSE_NAME',
            title="Query Success Rate by Warehouse",
            labels={'USAGE_DATE': 'Date', 'success_rate': 'Success Rate (%)'},
            template='plotly_dark')
        fig_success.update_layout(yaxis=dict(range=[0, 100]))
        st.plotly_chart(fig_success)
        
        # Data scanning patterns
        fig_scan = px.bar(df10,
            x='USAGE_DATE',
            y='AVG_GB_SCANNED',
            color='WAREHOUSE_NAME',
            title="Average Data Scanned by Warehouse",
            labels={'USAGE_DATE': 'Date', 'AVG_GB_SCANNED': 'Avg GB Scanned'},
            template='plotly_dark')
        st.plotly_chart(fig_scan)
        
    else:
        st.info("No resource utilization data found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page shows warehouse efficiency metrics including query success rates, execution times, and data scanning patterns. Use this information to optimize warehouse sizing and query performance.")
