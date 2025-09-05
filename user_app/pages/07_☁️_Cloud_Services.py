# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Cloud Services Breakdown",
    page_icon="☁️",
    layout="wide"
)

st.title("☁️ Cloud Services Breakdown")
st.markdown("Analyze cloud services usage patterns and compilation costs")

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
    st.header("Cloud Services Usage Analysis")
    
    # Cloud Services detailed analysis
    query8 = """
    SELECT 
        DATE_TRUNC('day', start_time)::DATE AS usage_date,
        QUERY_TYPE,
        WAREHOUSE_NAME,
        ROUND(SUM(CREDITS_USED_CLOUD_SERVICES), 3) AS cs_credits,
        COUNT(*) AS query_count,
        ROUND(AVG(COMPILATION_TIME)/1000, 2) AS avg_compilation_seconds
    FROM snowflake_copy_cost_views.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= '{0}' AND start_time <= '{1}'
        AND CREDITS_USED_CLOUD_SERVICES > 0
    GROUP BY 1, 2, 3
    ORDER BY 1 DESC, 4 DESC;
    """.format(start_date, end_date)
    
    df8 = session.sql(query8).to_pandas()
    
    if not df8.empty:
        # Rename columns for better display
        df8_display = df8.rename(columns={
            "USAGE_DATE": "Date",
            "QUERY_TYPE": "Query Type",
            "WAREHOUSE_NAME": "Warehouse",
            "CS_CREDITS": "Cloud Services Credits",
            "QUERY_COUNT": "Query Count",
            "AVG_COMPILATION_SECONDS": "Avg Compilation (sec)"
        })
        st.dataframe(df8_display)
        
        # Cloud Services by Query Type
        fig8 = px.pie(df8, 
            values='CS_CREDITS', 
            names='QUERY_TYPE',
            title="Cloud Services Credits by Query Type")
        st.plotly_chart(fig8)
        
        # Cloud Services over time
        fig8_time = px.bar(df8,
            x='USAGE_DATE',
            y='CS_CREDITS',
            color='QUERY_TYPE',
            title="Cloud Services Credits Over Time",
            labels={'USAGE_DATE': 'Date', 'CS_CREDITS': 'Cloud Services Credits'},
            template='plotly_dark')
        st.plotly_chart(fig8_time)
        
    else:
        st.info("No cloud services usage found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** Cloud services charges include query compilation, metadata operations, and other background services. Monitor these costs to understand total usage patterns.")
