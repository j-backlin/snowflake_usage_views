# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Spillage Analysis",
    page_icon="💾",
    layout="wide"
)

st.title("💾 Spillage Analysis")
st.markdown("Identify queries that spill to disk and their performance impact")

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
    st.header("Queries Spilled to Disk")
    
    query4 = """
    SELECT 
        DATE_TRUNC ('day', start_time):: DATE AS usage_date,
        start_time,
        BYTES_SPILLED_TO_REMOTE_STORAGE as "Remote Spillage",
        bytes_spilled_to_local_storage as "Local spillage",
        QUERY_ID,
        query_id_url as URL, 
        WAREHOUSE_NAME, 
        WAREHOUSE_SIZE, 
        BYTES_SCANNED, 
        USER_NAME 
    FROM snowflake_copy_cost_views.account_usage.query_history
    WHERE (bytes_spilled_to_local_storage > 0
    OR bytes_spilled_to_remote_storage > 0 )
    AND start_time >= '{0}' and start_time <= '{1}'
    ORDER BY bytes_spilled_to_remote_storage, bytes_spilled_to_local_storage DESC;
    """.format(start_date, end_date)
    
    df4 = session.sql(query4).to_pandas()
    
    if not df4.empty:
        fig4 = px.bar(df4,
        x='USAGE_DATE',
        y=['Remote Spillage', 'Local spillage'],
        color='WAREHOUSE_NAME',
        title="Queries Spilled to Disk (Bytes)",
        labels={"USAGE_DATE": "Date", "Remote Spillage": "Bytes Spilled", "Local spillage": "Local Bytes Spilled"},
        template="plotly_dark")
        st.plotly_chart(fig4)
        
        # Remove USAGE_DATE column and rename URL for better display
        st.dataframe(df4, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        }, hide_index=True, use_container_width=True)
    else:
        st.info("No queries with spillage found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page identifies queries that spill to disk, which can indicate performance issues. Consider optimizing these queries or increasing warehouse size.")
