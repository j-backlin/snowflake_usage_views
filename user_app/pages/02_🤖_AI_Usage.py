# Import python packages
import streamlit as st
import plotly.express as px
import altair as alt
from datetime import date, timedelta
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="AI Usage Analysis",
    page_icon="🤖",
    layout="wide"
)

st.title("🤖 AI Usage Analysis")
st.markdown("Track your AI function usage, token consumption, and Cortex Analyst activity")

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
    st.header("AI Functions Usage")
    
    query2 = """
    WITH ai_list AS (
    SELECT
        IFF(LENGTH(TRIM(MODEL_NAME)) > 0,MODEL_NAME,'default') as MODEL_NAME,
        FUNCTION_NAME,
        TOKENS,
        TOKEN_CREDITS,
        DATE_TRUNC ('month', start_time):: DATE AS usage_month,
        DATE_TRUNC ('day', start_time):: DATE AS usage_date
        
    FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history
    WHERE usage_date >= '{0}' and usage_date <= '{1}'
    )
    SELECT
        TO_CHAR(usage_date, 'YYYY-MM-DD') as usage_date,
        MODEL_NAME,
        FUNCTION_NAME,
        ROUND (SUM(TOKEN_CREDITS), 3) AS "Credits",
        --TOKENS
    FROM ai_list
    WHERE LENGTH(TRIM(FUNCTION_NAME)) > 0
    GROUP BY ALL
    ORDER BY 1;
    """.format(start_date, end_date)
    
    df2 = session.sql(query2).to_pandas()

    if not df2.empty:
        chart2 = alt.Chart(df2, title='AI functions').mark_bar(
        opacity=1,
        ).encode(
            column = alt.Column('USAGE_DATE', spacing = 50, title=None, header = alt.Header(labelAnchor="end", labelOrient = "bottom", labelAngle=-45)),
        x=alt.X('FUNCTION_NAME', sort = ["FUNCTION_NAME","MODEL_NAME"], 
                axis=alt.Axis(labelBaseline="top", title=None, labelAngle=-90),
        ),
        y=alt.Y('Credits:Q'),
        color=alt.Color('MODEL_NAME')
        ).configure_view(stroke='transparent')
        st.altair_chart(chart2)
        
        # Rename columns for better display
        df2_display = df2.rename(columns={
            "USAGE_DATE": "Date",
            "MODEL_NAME": "Model",
            "FUNCTION_NAME": "Function"
        })
        st.dataframe(df2_display)
    else:
        st.info("No AI functions usage found for the selected date range.")

    st.header("Cortex Analyst (Chatbot) Usage")
    
    query3 = """
    SELECT
        DATE_TRUNC ('day', start_time):: DATE AS usage_date,
        ROUND (SUM(CREDITS), 3) AS "Credits",
        SUM(REQUEST_COUNT) as "Requests"
    FROM snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY
    WHERE start_time >= '{0}' and start_time <= '{1}'
    GROUP BY ALL
    ORDER BY 1;
    """.format(start_date, end_date)
    
    df3 = session.sql(query3).to_pandas()

    if not df3.empty:
        chart3 = alt.Chart(df3, title='Cortex Analyst (chatbot)').mark_bar(
        opacity=1,
        ).encode(
            column = alt.Column('USAGE_DATE', spacing = 50, title=None, header = alt.Header(labelAnchor="end", labelOrient = "bottom", labelAngle=-45)),
        y=alt.Y('Credits:Q'),
        color=alt.Color('Requests')
        ).configure_view(stroke='transparent')
        st.altair_chart(chart3)
        
        # Rename columns for better display
        df3_display = df3.rename(columns={
            "USAGE_DATE": "Date",
            "Requests": "Nr of requests"
        })
        st.dataframe(df3_display)
    else:
        st.info("No Cortex Analyst usage found for the selected date range.")

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This page shows AI function and Cortex Analyst usage. Monitor token consumption and costs to optimize AI usage.")
