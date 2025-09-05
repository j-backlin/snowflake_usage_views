# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Expensive Queries",
    page_icon="💰",
    layout="wide"
)

st.title("💰 Most Expensive Queries")
st.markdown("Identify the highest cost queries to optimize resource usage and costs")

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
    st.header("Most Expensive Compute Queries")
    
    query5 = """
    SELECT 
        DATE_TRUNC('day', t1.start_time):: DATE AS usage_date,
        t1.START_TIME,
        t2.QUERY_TEXT,
        ROUND(SUM(t1.CREDITS_ATTRIBUTED_COMPUTE+IFF(t1.CREDITS_USED_QUERY_ACCELERATION is not null,t1.CREDITS_USED_QUERY_ACCELERATION,0)),4) as CREDITS,
        t2.query_id_url as URL,
        t1.QUERY_ID,
        t2.TOTAL_ELAPSED_TIME/1000 as EXECUTION_TIME_SECONDS,
        t1.WAREHOUSE_NAME,
        t2.EXECUTION_STATUS,
        t1.QUERY_TAG,
        t1.USER_NAME
    FROM snowflake_copy_cost_views.account_usage.query_attribution_history t1
    INNER JOIN snowflake_copy_cost_views.account_usage.query_history t2
    ON t1.query_id = t2.query_id
    WHERE t1.start_time >= '{0}' and t1.start_time <= '{1}'
    GROUP BY ALL ORDER BY CREDITS DESC LIMIT 50;""".format(start_date, end_date)
    
    df5 = session.sql(query5).to_pandas()
    
    if not df5.empty:
        # Remove USAGE_DATE column and rename URL for better display
        st.dataframe(df5, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        }, hide_index=True, use_container_width=True)
        
        fig5 = px.bar(df5.head(20),
        x='USAGE_DATE',
        y='CREDITS',
        title="Top 20 Query Credits by Date",
        labels={'USAGE_DATE': 'Date', 'CREDITS': 'Credits Used'}, 
        template='plotly_dark')
        fig5.update_layout(yaxis_title=None, xaxis_title=None)
        st.plotly_chart(fig5)
    else:
        st.info("No expensive compute queries found for the selected date range.")
    
    st.header("Most Expensive AI Queries")
    
    query6 = """
    SELECT 
        DATE_TRUNC('day', start_time):: DATE AS usage_date,
        START_TIME,
        QUERY_TEXT,
        ROUND (SUM(TOKEN_CREDITS), 3) AS CREDITS,
        TOTAL_ELAPSED_TIME,
        query_id_url as URL,
        QUERY_ID,
        FUNCTION_NAME,
        MODEL_NAME,
        TOKENS,
        TOKEN_CREDITS,
        WAREHOUSE_NAME,
        USER_NAME,
        EXECUTION_STATUS
    FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history
    WHERE start_time >= '{0}' and start_time <= '{1}'
    GROUP BY ALL ORDER BY CREDITS DESC LIMIT 50;""".format(start_date, end_date)
    
    df6 = session.sql(query6).to_pandas()
    
    if not df6.empty:
        # Remove USAGE_DATE column and rename URL for better display
        st.dataframe(df6, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        }, hide_index=True, use_container_width=True)
        
        fig6 = px.bar(df6.head(20),
        x='USAGE_DATE',
        y='CREDITS',
        title="Top 20 AI Query Credits by Date",
        labels={'USAGE_DATE': 'Date', 'CREDITS': 'Credits Used'}, 
        template='plotly_dark')
        fig6.update_layout(yaxis_title=None, xaxis_title=None)
        st.plotly_chart(fig6)
    else:
        st.info("No expensive AI queries found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page identifies the most expensive queries by credit usage. Focus optimization efforts on these high-cost queries for maximum impact.")
