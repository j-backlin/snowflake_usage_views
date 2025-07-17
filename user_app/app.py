# Import python packages
import streamlit as st
import plotly.express as px
import altair as alt
from datetime import date,timedelta
from snowflake.snowpark.context import get_active_session
st.set_page_config(layout="wide")
st.title(f"Snowflake cost and performance per user:balloon:")
st.write(f"Streamlit version {st.__version__}")

session = get_active_session()
d1 = st.date_input(
    "Select date range",
    (date.today() - timedelta(days = 7), date.today() - timedelta(days = 0)),
)

if(len(d1))>1:
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Warehous Usage Analysis", "AI Usage Analysis", "Spilled to Disk", "Query details", "AI Query details", "Most expensive queries"])
    with tab1:
        st.header("Top Warehouse Usage")
        query1="""
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
        """.format(d1[0],d1[1])
        df1=session.sql(query1).to_pandas()
        st.dataframe(df1, column_config={
            "USAGE_DATE": "Date",
            "WAREHOUSE_NAME": "Warehouse"
        },
        hide_index=True,use_container_width=True)
        fig1= px.bar(df1,
        x='USAGE_DATE',
        y='WH Credits',
        color='WAREHOUSE_NAME',
        title="Warehouse Credits Usage",
        labels={'usage_month': None, 'WH Credits': 'Credits Used'}, template='plotly_dark')
        fig1.update_layout(yaxis_title=None,xaxis_title=None)
        st.plotly_chart(fig1)
    
    with tab2:
        st.header("AI Usage")
        query2 = """
        WITH ai_list AS (
        SELECT
            IFF(LENGTH(TRIM(MODEL_NAME)) > 0,MODEL_NAME,'default') as MODEL_NAME,
            FUNCTION_NAME,
            TOKENS,
            TOKEN_CREDITS,
            CREDITS_USED_CLOUD_SERVICES,
            DATE_TRUNC ('month', start_time):: DATE AS usage_month,
            DATE_TRUNC ('day', start_time):: DATE AS usage_date
            
        FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history
        WHERE usage_date >= '{0}' and usage_date <= '{1}'
        )
        SELECT
            TO_CHAR(usage_date, 'YYYY-MM-DD') as usage_date,
            MODEL_NAME,
            FUNCTION_NAME,
            ROUND (SUM(TOKEN_CREDITS+CREDITS_USED_CLOUD_SERVICES), 3) AS "Credits",
            --TOKENS
        FROM ai_list
        WHERE LENGTH(TRIM(FUNCTION_NAME)) > 0
        GROUP BY ALL
        ORDER BY 1;
        """.format(d1[0],d1[1])
        
        df2 = session.sql(query2).to_pandas()
    
        chart2 = alt.Chart(df2, title='AI functions').mark_bar(
        opacity=1,
        ).encode(
            column = alt.Column('USAGE_DATE', spacing = 50, title=None, header = alt.Header(labelAnchor="end", labelOrient = "bottom",labelAngle=-45)),
        x=alt.X('FUNCTION_NAME', sort = ["FUNCTION_NAME","MODEL_NAME"], 
                axis=alt.Axis(labelBaseline="top",title=None,labelAngle=-90),
        ),
        y=alt.Y('Credits:Q'),
        color=alt.Color('MODEL_NAME')
        ).configure_view(stroke='transparent')
        st.altair_chart(chart2)
        st.dataframe(df2, column_config={
            "USAGE_DATE": "Date",
            "MODEL_NAME": "Model",
            "FUNCTION_NAME": "Function",
            "Credits": "Credits",
        },hide_index=True,use_container_width=True)
    
        query3 = """
        SELECT
            DATE_TRUNC ('day', start_time):: DATE AS usage_date,
            ROUND (SUM(CREDITS), 3) AS "Credits",
            SUM(REQUEST_COUNT) as "Requests",
        FROM snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY
        WHERE usage_date >= '{0}' and usage_date <= '{1}'
        GROUP BY ALL
        ORDER BY 1;
        """.format(d1[0],d1[1])
        df3 = session.sql(query3).to_pandas()
    
        chart3 = alt.Chart(df3, title='Cortex Analyst (chatbot)').mark_bar(
        opacity=1,
        ).encode(
            column = alt.Column('USAGE_DATE', spacing = 50, title=None, header = alt.Header(labelAnchor="end", labelOrient = "bottom",labelAngle=-45)),
        y=alt.Y('Credits:Q'),
        color=alt.Color('Requests')
        ).configure_view(stroke='transparent')
        st.altair_chart(chart3)
        st.dataframe(df3, column_config={
            "USAGE_DATE": "Date",
            "Requests": "Nr of requests"
        },hide_index=True,use_container_width=True)
        
    with tab3:
        st.header("Queries Spilled to Disk (Last 45 Days)")
        query4="""
        SELECT DATE_TRUNC ('day', start_time):: DATE AS usage_date,start_time,
        BYTES_SPILLED_TO_REMOTE_STORAGE as "Remote Spillage",bytes_spilled_to_local_storage as "Local spillage",QUERY_ID,QUERY_ID_URL_ORG as URL, WAREHOUSE_NAME, 
        WAREHOUSE_SIZE, BYTES_SCANNED, USER_NAME FROM snowflake_copy_cost_views.account_usage.query_history
        WHERE (bytes_spilled_to_local_storage > 0
        OR bytes_spilled_to_remote_storage > 0 )
        AND start_time >= '{0}' and start_time <= '{1}'
        ORDER BY bytes_spilled_to_remote_storage,bytes_spilled_to_local_storage DESC;
        """.format(d1[0],d1[1])
        df4=session.sql(query4).to_pandas()
        fig4= px.bar(df4,
        x='USAGE_DATE',
        y=['Remote Spillage', 'Local spillage'],
        color='WAREHOUSE_NAME',
        title="Queries Spilled to Disk (Bytes)",
        labels={"USAGE_DATE": "Date", "Remote Spillage": "Bytes Spilled","Local spillage": "Local Bytes Spilled"},
        template="plotly_dark")
        st.plotly_chart(fig4)
        st.dataframe(df4, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        },hide_index=True,use_container_width=True)
    
    with tab4:
        st.header("Query details")
        query4="""
        SELECT 
            DATE_TRUNC ('day', start_time):: DATE AS usage_date,
            START_TIME,
            QUERY_TEXT,
            TOTAL_ELAPSED_TIME/1000 as EXECUTION_TIME_SECONDS,
            QUERY_ID,
            QUERY_ID_URL_ORG as URL,
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
        ORDER BY START_TIME DESC;""".format(d1[0],d1[1])
        
        df4=session.sql(query4).to_pandas()
        st.dataframe (df4, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        },hide_index=True,use_container_width=True)
    
    with tab5:
        st.header("AI query details")
        query5="""
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
            QUERY_ID_URL_ORG as URL,
            CREDITS_USED_CLOUD_SERVICES,
            WAREHOUSE_NAME,
            USER_NAME
        FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history 
        WHERE start_time >= '{0}' and start_time <= '{1}'
        ORDER BY START_TIME DESC;""".format(d1[0],d1[1])
        df5 = session.sql (query5).to_pandas ()
        st.dataframe (df5, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        },hide_index=True,use_container_width=True)
    
    with tab6:
        st.header("Most expensive queries")
        query5="""
        SELECT 
            DATE_TRUNC('day', t1.start_time):: DATE AS usage_date,
            t1.START_TIME,
            t2.QUERY_TEXT,
            ROUND(SUM(t1.CREDITS_ATTRIBUTED_COMPUTE+IFF(t1.CREDITS_USED_QUERY_ACCELERATION is not null,t1.CREDITS_USED_QUERY_ACCELERATION,0)),4) as CREDITS,
            t2.QUERY_ID_URL_ORG as URL,
            --t2.QUERY_ID_URL_REG,
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
        GROUP BY ALL ORDER BY CREDITS DESC LIMIT 50;""".format(d1[0],d1[1])
        df5 = session.sql(query5).to_pandas()
        st.dataframe (df5, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        },hide_index=True,use_container_width=True)
        fig5=px.bar(df5,
        x='USAGE_DATE',
        y='CREDITS',
        title="Query credits",
        labels={'usage_month': None, 'CREDITS': 'Credits Used'}, template='plotly_dark')
        fig1.update_layout(yaxis_title=None,xaxis_title=None)
        st.plotly_chart(fig5)
        
        query6="""
        SELECT 
            DATE_TRUNC('day', start_time):: DATE AS usage_date,
            START_TIME,
            QUERY_TEXT,
            ROUND (SUM(TOKEN_CREDITS+CREDITS_USED_CLOUD_SERVICES), 3) AS CREDITS,
            TOTAL_ELAPSED_TIME,
            QUERY_ID_URL_ORG as URL,
            --QUERY_ID_URL_REG,
            QUERY_ID,
            FUNCTION_NAME,
            MODEL_NAME,
            TOKENS,
            TOKEN_CREDITS,
            CREDITS_USED_CLOUD_SERVICES,
            WAREHOUSE_NAME,
            USER_NAME,
            EXECUTION_STATUS
        FROM snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history
        WHERE start_time >= '{0}' and start_time <= '{1}'
        GROUP BY ALL ORDER BY CREDITS DESC LIMIT 50;""".format(d1[0],d1[1])
        df6 = session.sql(query6).to_pandas()
        st.dataframe (df6, column_config={
            "USAGE_DATE": None,
            "URL": st.column_config.LinkColumn(display_text='Query profile')
        },hide_index=True,use_container_width=True)
        fig6=px.bar(df6,
        x='USAGE_DATE',
        y='CREDITS',
        title="AI Query credits",
        labels={'usage_month': None, 'CREDITS': 'Credits Used'}, template='plotly_dark')
        fig1.update_layout(yaxis_title=None,xaxis_title=None)
        st.plotly_chart(fig6)
