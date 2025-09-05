# Import python packages
import streamlit as st
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="AI Query Details",
    page_icon="🤖",
    layout="wide"
)

st.title("🤖 AI Query Details")
st.markdown("Detailed analysis of AI function queries including tokens and model usage")

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
    st.header("AI Query Execution Details")
    
    query5 = """
    SELECT 
        DATE_TRUNC ('day', start_time):: DATE AS usage_date,
        START_TIME,
        QUERY_TEXT,
        TOTAL_ELAPSED_TIME,
        FUNCTION_NAME,
        MODEL_NAME,
        EXECUTION_STATUS,
        TOKENS,
        TOKEN_CREDITS,
        QUERY_ID,
        query_id_url as URL,
        WAREHOUSE_NAME,
        USER_NAME
    FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history 
    WHERE start_time >= '{0}' and start_time <= '{1}'
    ORDER BY START_TIME DESC;""".format(start_date, end_date)
    
    df5 = session.sql(query5).to_pandas()
    
    if not df5.empty:
        # Remove USAGE_DATE column and rename URL for better display
        st.dataframe(df5, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        }, hide_index=True, use_container_width=True)
    else:
        st.info("No AI query data found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page shows detailed AI function query information including token usage and costs. Monitor high-token queries for optimization opportunities.")
