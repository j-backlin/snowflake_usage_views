# Import python packages
import streamlit as st
import plotly.express as px
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Warehouse Usage Analysis",
    page_icon="🏭",
    layout="wide"
)

st.title("🏭 Warehouse Usage Analysis")
st.markdown("Track your warehouse credit usage patterns and identify optimization opportunities")

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
    st.header("Top Warehouse Usage")
    
    query1 = """
    WITH wh_list AS (
    SELECT
        warehouse_name,
        credits_attributed_compute as credits_used,
        DATE_TRUNC ('month', start_time):: DATE AS usage_month,
        DATE_TRUNC ('day', start_time):: DATE AS usage_date
    FROM snowflake_copy_cost_views.account_usage.query_attribution_history
    WHERE usage_date >= '{0}' and usage_date <= '{1}'
    )
    SELECT
        usage_date,
        warehouse_name,
        ROUND (SUM(credits_used), 2) AS "WH Credits"
    FROM wh_list
    GROUP BY 1, 2
    ORDER BY 1;
    """.format(start_date, end_date)
    
    df1 = session.sql(query1).to_pandas()
    
    if not df1.empty:
        # Rename columns for better display
        df1_display = df1.rename(columns={
            "USAGE_DATE": "Date",
            "WAREHOUSE_NAME": "Warehouse"
        })
        st.dataframe(df1_display)
        
        fig1 = px.bar(df1,
        x='USAGE_DATE',
        y='WH Credits',
        color='WAREHOUSE_NAME',
        title="Warehouse Credits Usage",
        labels={'usage_month': None, 'WH Credits': 'Credits Used'}, 
        template='plotly_dark')
        fig1.update_layout(yaxis_title=None, xaxis_title=None)
        st.plotly_chart(fig1)
    else:
        st.info("No warehouse usage data found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page shows warehouse credit usage patterns. Use this data to identify peak usage times and potential optimization opportunities.")
