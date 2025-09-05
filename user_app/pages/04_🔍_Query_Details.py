# Import python packages
import streamlit as st
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Query Details",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 Query Details")
st.markdown("Detailed analysis of query execution metrics and performance")

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
    st.header("Query Execution Details")
    
    query4 = """
    SELECT 
        DATE_TRUNC ('day', start_time):: DATE AS usage_date,
        START_TIME,
        QUERY_TEXT,
        TOTAL_ELAPSED_TIME/1000 as EXECUTION_TIME_SECONDS,
        QUERY_ID,
        query_id_url as URL,
        EXECUTION_STATUS,
        ERROR_MESSAGE,
        DATABASE_NAME,
        SCHEMA_NAME,
        WAREHOUSE_NAME,
        BYTES_SCANNED,
        ROWS_PRODUCED,
        CREDITS_USED_CLOUD_SERVICES,
        USER_NAME
    FROM snowflake_copy_cost_views.account_usage.query_history 
    WHERE start_time >= '{0}' and start_time <= '{1}'
    ORDER BY START_TIME DESC;""".format(start_date, end_date)
    
    df4 = session.sql(query4).to_pandas()
    
    if not df4.empty:
        # Remove USAGE_DATE column and rename URL for better display
        st.dataframe(df4, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        }, hide_index=True, use_container_width=True)
    else:
        st.info("No query data found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page shows detailed query execution information. Use the Query Profile links to dive deeper into individual query performance.")
