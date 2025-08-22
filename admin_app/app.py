# Import python packages
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from snowflake.snowpark.context import get_active_session

# Configuration and setup
st.set_page_config(
    page_title="Snowflake Cost Optimization Dashboard",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded"
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

st.title("💰 Snowflake Account Cost Dashboard")
st.markdown("**Account Admin View** - Track costs, identify savings opportunities, and optimize resource usage across your entire Snowflake account")

# Display Streamlit version
try:
    st.sidebar.info(f"Streamlit version {st.__version__}")
except:
    st.sidebar.info("Streamlit version: Unable to detect")

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
        value=date.today() - timedelta(days = -1),
        key="end_date"
    )

if start_date and end_date and start_date <= end_date:
    # Main dashboard tabs
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "💰 Cost Overview", 
        "🏭 Warehouse Optimization", 
        "👥 User Cost Analysis",
        "🗄️ Storage & Data Costs",
        "🤖 AI Cost Management", 
        "⚡ Query Efficiency",
        "📈 Cost Forecasting",
        "🎯 Savings Opportunities"
    ])

    with tab1:
        st.header("💰 Account Cost Overview")
        
        # Get total costs for current period
        total_cost_query = f"""
        SELECT 
            SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) as current_compute,
            SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as current_cloud_services
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
        WHERE qa.START_TIME >= '{start_date}' AND qa.START_TIME <= '{end_date}'
        """
        
        # Get AI costs separately (may not be available in all accounts)
        ai_cost_query = f"""
        SELECT 
            COALESCE(SUM(cf.TOKEN_CREDITS), 0) as current_ai
        FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
        INNER JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
        WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
        """
        
        cost_summary = session.sql(total_cost_query).to_pandas()
        
        # Try to get AI costs, handle gracefully if not available
        ai_current = 0
        try:
            ai_cost_result = session.sql(ai_cost_query).to_pandas()
            if not ai_cost_result.empty:
                ai_current = ai_cost_result['CURRENT_AI'].iloc[0] or 0
        except Exception as e:
            st.sidebar.warning("⚠️ AI cost tracking not available in this account")
            ai_current = 0
        
        if not cost_summary.empty:
            current_total = (
                cost_summary['CURRENT_COMPUTE'].iloc[0] + 
                cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0] + 
                ai_current
            )
            
            # Display key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "Total Credits",
                    f"{current_total:,.1f}"
                )
            
            with col2:
                st.metric(
                    "Compute Credits",
                    f"{cost_summary['CURRENT_COMPUTE'].iloc[0]:,.1f}"
                )
            
            with col3:
                st.metric(
                    "Cloud Services Credits", 
                    f"{cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0]:,.1f}"
                )
            
            with col4:
                st.metric(
                    "AI Credits",
                    f"{ai_current:,.1f}"
                )

        # Daily cost trend
        daily_cost_query = f"""
        SELECT 
            DATE_TRUNC('day', qa.START_TIME)::DATE as usage_date,
            SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0)) as compute_credits,
            SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as cloud_services_credits,
            COUNT(DISTINCT qa.USER_NAME) as active_users,
            COUNT(DISTINCT qa.WAREHOUSE_NAME) as active_warehouses
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
        LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
        WHERE qa.START_TIME >= '{start_date}' AND qa.START_TIME <= '{end_date}'
        GROUP BY 1
        ORDER BY 1
        """
        
        daily_costs = session.sql(daily_cost_query).to_pandas()
        
        if not daily_costs.empty:
            # Add total costs column
            daily_costs['total_credits'] = daily_costs['COMPUTE_CREDITS'] + daily_costs['CLOUD_SERVICES_CREDITS']
            
            # Create dual-axis chart
            fig = make_subplots(
                rows=1, cols=1,
                specs=[[{"secondary_y": True}]],
                subplot_titles=["Daily Cost Trends"]
            )
            
            # Add cost bars
            fig.add_trace(
                go.Bar(
                    x=daily_costs['USAGE_DATE'],
                    y=daily_costs['COMPUTE_CREDITS'],
                    name='Compute Credits',
                    marker_color='#1f77b4'
                ),
                secondary_y=False
            )
            
            fig.add_trace(
                go.Bar(
                    x=daily_costs['USAGE_DATE'],
                    y=daily_costs['CLOUD_SERVICES_CREDITS'],
                    name='Cloud Services Credits',
                    marker_color='#ff7f0e'
                ),
                secondary_y=False
            )
            
            # Add active users line
            fig.add_trace(
                go.Scatter(
                    x=daily_costs['USAGE_DATE'],
                    y=daily_costs['ACTIVE_USERS'],
                    mode='lines+markers',
                    name='Active Users',
                    line=dict(color='#2ca02c', width=3),
                    marker=dict(size=6)
                ),
                secondary_y=True
            )
            
            # Update layout
            fig.update_layout(
                title="Daily Costs and Activity",
                barmode='stack',
                height=500,
                template='plotly_white'
            )
            
            fig.update_xaxes(title_text="Date")
            fig.update_yaxes(title_text="Credits", secondary_y=False)
            fig.update_yaxes(title_text="Active Users", secondary_y=True)
            
            st.plotly_chart(fig, use_container_width=True)

        # Cost breakdown pie chart
        if not cost_summary.empty:
            cost_breakdown = pd.DataFrame({
                'Category': ['Compute', 'Cloud Services', 'AI Functions'],
                'Credits': [
                    cost_summary['CURRENT_COMPUTE'].iloc[0],
                    cost_summary['CURRENT_CLOUD_SERVICES'].iloc[0], 
                    ai_current
                ]
            })
            
            fig_pie = px.pie(
                cost_breakdown,
                values='Credits',
                names='Category',
                title="Cost Breakdown by Category",
                color_discrete_sequence=['#1f77b4', '#ff7f0e', '#2ca02c']
            )
            
            st.plotly_chart(fig_pie, use_container_width=True)

    with tab2:
        st.header("🏭 Warehouse Optimization")
        
        # Warehouse utilization and cost analysis
        warehouse_analysis_query = f"""
        WITH warehouse_stats AS (
            SELECT 
                q.WAREHOUSE_NAME,
                DATE_TRUNC('day', q.START_TIME)::DATE as usage_date,
                SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as daily_credits,
                COUNT(DISTINCT q.QUERY_ID) as query_count,
                AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
                SUM(CASE WHEN qh.EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) as successful_queries,
                COUNT(DISTINCT q.USER_NAME) as unique_users
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
            WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
            AND q.WAREHOUSE_NAME IS NOT NULL
            GROUP BY 1, 2
        ),
        warehouse_summary AS (
            SELECT 
                WAREHOUSE_NAME,
                SUM(daily_credits) as total_credits,
                AVG(daily_credits) as avg_daily_credits,
                SUM(query_count) as total_queries,
                AVG(avg_execution_seconds) as avg_execution_time,
                AVG(unique_users) as avg_daily_users,
                COUNT(DISTINCT usage_date) as active_days,
                MAX(daily_credits) as peak_daily_cost,
                MIN(daily_credits) as min_daily_cost
            FROM warehouse_stats
            GROUP BY 1
        )
        SELECT 
            WAREHOUSE_NAME,
            ROUND(total_credits, 2) as "Total Credits",
            ROUND(avg_daily_credits, 2) as "Avg Daily Credits",
            total_queries as "Total Queries", 
            ROUND(avg_execution_time, 2) as "Avg Execution (sec)",
            ROUND(avg_daily_users, 1) as "Avg Daily Users",
            active_days as "Active Days",
            ROUND(peak_daily_cost, 2) as "Peak Daily Cost",
            ROUND(total_credits / NULLIF(total_queries, 0), 4) as "Credits per Query"
        FROM warehouse_summary
        ORDER BY total_credits DESC
        """
        
        warehouse_data = session.sql(warehouse_analysis_query).to_pandas()
        
        if not warehouse_data.empty:
            st.subheader("Warehouse Cost Summary")
            st.dataframe(warehouse_data, use_container_width=True)
            
            # Visualizations
            col1, col2 = st.columns(2)
            
            with col1:
                # Top 10 warehouses by cost
                top_warehouses = warehouse_data.head(10)
                fig_wh_cost = px.bar(
                    top_warehouses,
                    x='WAREHOUSE_NAME',
                    y='Total Credits',
                    title="Top 10 Warehouses by Total Credits",
                    color='Total Credits',
                    color_continuous_scale='Reds'
                )
                fig_wh_cost.update_xaxes(tickangle=45)
                st.plotly_chart(fig_wh_cost, use_container_width=True)
            
            with col2:
                # Efficiency scatter plot
                fig_efficiency = px.scatter(
                    warehouse_data,
                    x='Total Queries',
                    y='Credits per Query',
                    size='Total Credits',
                    hover_data=['WAREHOUSE_NAME', 'Avg Daily Users'],
                    title="Warehouse Efficiency: Credits per Query vs Query Volume",
                    color='Avg Execution (sec)',
                    color_continuous_scale='RdYlBu_r'
                )
                st.plotly_chart(fig_efficiency, use_container_width=True)
            
            # Optimization recommendations
            st.subheader("💡 Optimization Recommendations")
            
            # High cost per query warehouses
            high_cost_per_query = warehouse_data[warehouse_data['Credits per Query'] > warehouse_data['Credits per Query'].quantile(0.75)]
            if not high_cost_per_query.empty:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>⚠️ High Cost per Query Warehouses</h4>
                    <p>These warehouses have high credits per query ratio - consider query optimization or right-sizing:</p>
                </div>
                """, unsafe_allow_html=True)
                
                for _, wh in high_cost_per_query.iterrows():
                    st.warning(f"🏭 **{wh['WAREHOUSE_NAME']}**: {wh['Credits per Query']:.4f} credits/query - Consider optimizing queries or warehouse size")
            
            # Low utilization warehouses  
            low_utilization = warehouse_data[warehouse_data['Avg Daily Users'] < 2]
            if not low_utilization.empty:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>📉 Low Utilization Warehouses</h4>
                    <p>These warehouses have low user activity - consider consolidation:</p>
                </div>
                """, unsafe_allow_html=True)
                
                for _, wh in low_utilization.iterrows():
                    potential_savings = wh['Total Credits'] * 0.3  # Assume 30% savings from consolidation
                    st.info(f"🏭 **{wh['WAREHOUSE_NAME']}**: {wh['Avg Daily Users']:.1f} avg users/day - Potential savings: {potential_savings:.1f} credits")

    with tab3:
        st.header("👥 User Cost Analysis") 
        
        # User cost analysis
        user_cost_query = f"""
        WITH user_costs AS (
            SELECT 
                q.USER_NAME,
                SUM(q.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(q.CREDITS_USED_QUERY_ACCELERATION, 0)) as compute_credits,
                SUM(COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as cloud_services_credits,
                COUNT(DISTINCT q.QUERY_ID) as total_queries,
                COUNT(DISTINCT q.WAREHOUSE_NAME) as warehouses_used,
                AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
                SUM(CASE WHEN qh.EXECUTION_STATUS = 'FAILED' THEN 1 ELSE 0 END) as failed_queries,
                COUNT(DISTINCT DATE_TRUNC('day', q.START_TIME)) as active_days
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
            WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
            GROUP BY 1
        ),
        user_ai_costs AS (
            SELECT 
                qh.USER_NAME,
                SUM(cf.TOKEN_CREDITS) as ai_credits,
                COUNT(*) as ai_queries
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
            WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
            GROUP BY 1
        )
        SELECT 
            uc.USER_NAME,
            ROUND(uc.compute_credits + uc.cloud_services_credits + COALESCE(ac.ai_credits, 0), 2) as "Total Credits",
            ROUND(uc.compute_credits, 2) as "Compute Credits",
            ROUND(uc.cloud_services_credits, 2) as "Cloud Services Credits", 
            ROUND(COALESCE(ac.ai_credits, 0), 2) as "AI Credits",
            uc.total_queries as "Total Queries",
            COALESCE(ac.ai_queries, 0) as "AI Queries",
            uc.warehouses_used as "Warehouses Used",
            ROUND(uc.avg_execution_seconds, 2) as "Avg Execution (sec)",
            uc.failed_queries as "Failed Queries",
            uc.active_days as "Active Days",
            ROUND((uc.compute_credits + uc.cloud_services_credits + COALESCE(ac.ai_credits, 0)) / NULLIF(uc.total_queries + COALESCE(ac.ai_queries, 0), 0), 4) as "Credits per Query"
        FROM user_costs uc
        LEFT JOIN user_ai_costs ac ON uc.USER_NAME = ac.USER_NAME
        ORDER BY "Total Credits" DESC
        """
        
        user_data = session.sql(user_cost_query).to_pandas()
        
        if not user_data.empty:
            # Top users summary
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Users", len(user_data))
            with col2:
                st.metric("Top User Cost", f"{user_data['Total Credits'].max():,.1f} credits")
            with col3:
                avg_cost = user_data['Total Credits'].mean()
                st.metric("Average User Cost", f"{avg_cost:.1f} credits")
            
            # Top spenders
            st.subheader("💸 Top 20 Cost Contributors")
            top_users = user_data.head(20)
            
            fig_users = px.bar(
                top_users,
                x='USER_NAME',
                y='Total Credits',
                color='Credits per Query',
                title="Top 20 Users by Total Credits",
                color_continuous_scale='Reds',
                hover_data=['Total Queries', 'AI Credits', 'Failed Queries']
            )
            fig_users.update_xaxes(tickangle=45)
            st.plotly_chart(fig_users, use_container_width=True)
            
            # User details table
            st.subheader("📊 User Cost Details")
            st.dataframe(user_data, use_container_width=True)
            
            # Cost distribution analysis
            col1, col2 = st.columns(2)
            
            with col1:
                # Cost distribution histogram
                fig_dist = px.histogram(
                    user_data,
                    x='Total Credits',
                    nbins=20,
                    title="User Cost Distribution",
                    labels={'count': 'Number of Users'}
                )
                st.plotly_chart(fig_dist, use_container_width=True)
            
            with col2:
                # Efficiency vs Activity scatter
                fig_scatter = px.scatter(
                    user_data,
                    x='Total Queries',
                    y='Credits per Query',
                    size='Total Credits',
                    color='Failed Queries',
                    title="User Efficiency: Credits per Query vs Activity",
                    hover_data=['USER_NAME', 'AI Credits']
                )
                st.plotly_chart(fig_scatter, use_container_width=True)
            
            # User optimization recommendations
            st.subheader("💡 User Optimization Opportunities")
            
            # High cost per query users
            high_cost_users = user_data[user_data['Credits per Query'] > user_data['Credits per Query'].quantile(0.9)]
            if not high_cost_users.empty:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>⚠️ Users with High Cost per Query</h4>
                    <p>These users may benefit from query optimization training:</p>
                </div>
                """, unsafe_allow_html=True)
                
                for _, user in high_cost_users.head(5).iterrows():
                    st.warning(f"👤 **{user['USER_NAME']}**: {user['Credits per Query']:.4f} credits/query - {user['Total Queries']} queries, {user['Failed Queries']} failures")
            
            # High failure rate users
            high_failure_users = user_data[user_data['Failed Queries'] > 10]
            if not high_failure_users.empty:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>🚨 Users with High Query Failure Rates</h4>
                    <p>These users have many failed queries, wasting resources:</p>
                </div>
                """, unsafe_allow_html=True)
                
                for _, user in high_failure_users.iterrows():
                    failure_rate = (user['Failed Queries'] / user['Total Queries']) * 100
                    st.error(f"👤 **{user['USER_NAME']}**: {user['Failed Queries']} failures ({failure_rate:.1f}% failure rate)")

    with tab4:
        st.header("🗄️ Storage & Data Costs")
        st.info("📝 Storage cost analysis requires additional Snowflake account usage views. This section shows data transfer and scanning costs.")
        
        # Data scanning analysis
        data_scanning_query = f"""
        SELECT 
            DATE_TRUNC('day', q.START_TIME)::DATE as usage_date,
            SUM(qh.BYTES_SCANNED) / POWER(1024, 4) as total_tb_scanned,
            COUNT(DISTINCT q.QUERY_ID) as queries_with_scanning,
            AVG(qh.BYTES_SCANNED) / POWER(1024, 3) as avg_gb_per_query,
            SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as scanning_related_credits
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        AND qh.BYTES_SCANNED > 0
        GROUP BY 1
        ORDER BY 1
        """
        
        scanning_data = session.sql(data_scanning_query).to_pandas()
        
        if not scanning_data.empty:
            # Data scanning metrics
            col1, col2, col3 = st.columns(3)
            
            with col1:
                total_tb = scanning_data['TOTAL_TB_SCANNED'].sum()
                st.metric("Total TB Scanned", f"{total_tb:.2f} TB")
            
            with col2:
                avg_gb = scanning_data['AVG_GB_PER_QUERY'].mean()
                st.metric("Avg GB per Query", f"{avg_gb:.2f} GB")
            
            with col3:
                total_queries = scanning_data['QUERIES_WITH_SCANNING'].sum()
                st.metric("Queries with Data Scanning", f"{total_queries:,}")
            
            # Data scanning trends
            fig_scanning = px.line(
                scanning_data,
                x='USAGE_DATE',
                y='TOTAL_TB_SCANNED',
                title="Daily Data Scanning Volume (TB)",
                markers=True
            )
            st.plotly_chart(fig_scanning, use_container_width=True)
            
            # Scanning efficiency
            fig_efficiency = make_subplots(
                rows=1, cols=1,
                specs=[[{"secondary_y": True}]],
                subplot_titles=["Data Scanning vs Compute Credits"]
            )
            
            fig_efficiency.add_trace(
                go.Bar(
                    x=scanning_data['USAGE_DATE'],
                    y=scanning_data['TOTAL_TB_SCANNED'],
                    name='TB Scanned',
                    marker_color='lightblue'
                ),
                secondary_y=False
            )
            
            fig_efficiency.add_trace(
                go.Scatter(
                    x=scanning_data['USAGE_DATE'],
                    y=scanning_data['SCANNING_RELATED_CREDITS'],
                    mode='lines+markers',
                    name='Credits',
                    line=dict(color='red', width=3)
                ),
                secondary_y=True
            )
            
            fig_efficiency.update_yaxes(title_text="TB Scanned", secondary_y=False)
            fig_efficiency.update_yaxes(title_text="Credits", secondary_y=True)
            fig_efficiency.update_layout(title="Data Scanning vs Compute Costs")
            
            st.plotly_chart(fig_efficiency, use_container_width=True)
            
            # Large scan queries
            large_scan_query = f"""
            SELECT 
                q.USER_NAME,
                qh.QUERY_TEXT,
                qh.BYTES_SCANNED / POWER(1024, 3) as gb_scanned,
                q.CREDITS_ATTRIBUTED_COMPUTE as credits,
                qh.EXECUTION_TIME / 1000 as execution_seconds,
                q.START_TIME
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
            WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
            AND qh.BYTES_SCANNED > 0
            ORDER BY qh.BYTES_SCANNED DESC
            LIMIT 20
            """
            
            large_scans = session.sql(large_scan_query).to_pandas()
            
            if not large_scans.empty:
                st.subheader("🔍 Top 20 Queries by Data Scanned")
                # Truncate query text for display
                large_scans['QUERY_TEXT_DISPLAY'] = large_scans['QUERY_TEXT'].str[:100] + '...'
                display_cols = ['USER_NAME', 'QUERY_TEXT_DISPLAY', 'GB_SCANNED', 'CREDITS', 'EXECUTION_SECONDS', 'START_TIME']
                st.dataframe(large_scans[display_cols], use_container_width=True)

    with tab5:
        st.header("🤖 AI Cost Management")
        
        # AI usage summary
        ai_summary_query = f"""
        WITH ai_summary AS (
            SELECT 
                DATE_TRUNC('day', qh.START_TIME)::DATE as usage_date,
                cf.FUNCTION_NAME,
                COALESCE(cf.MODEL_NAME, 'default') as model_name,
                SUM(cf.TOKENS) as total_tokens,
                SUM(cf.TOKEN_CREDITS) as total_credits,
                COUNT(*) as request_count,
                COUNT(DISTINCT qh.USER_NAME) as unique_users
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
            WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
            GROUP BY 1, 2, 3
        )
        SELECT 
            usage_date,
            function_name,
            model_name,
            total_tokens,
            ROUND(total_credits, 4) as credits,
            request_count,
            unique_users,
            ROUND(total_credits / NULLIF(request_count, 0), 6) as credits_per_request,
            ROUND(total_tokens / NULLIF(request_count, 0), 0) as tokens_per_request
        FROM ai_summary
        ORDER BY usage_date DESC, total_credits DESC
        """
        
        ai_data = session.sql(ai_summary_query).to_pandas()
        
        if not ai_data.empty:
            # AI cost metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_ai_credits = ai_data['CREDITS'].sum()
                st.metric("Total AI Credits", f"{total_ai_credits:.3f}")
            
            with col2:
                total_tokens = ai_data['TOTAL_TOKENS'].sum()
                st.metric("Total Tokens", f"{total_tokens:,}")
            
            with col3:
                total_requests = ai_data['REQUEST_COUNT'].sum()
                st.metric("Total AI Requests", f"{total_requests:,}")
            
            with col4:
                avg_cost_per_request = ai_data['CREDITS_PER_REQUEST'].mean()
                st.metric("Avg Cost/Request", f"{avg_cost_per_request:.6f}")
            
            # AI usage trends
            daily_ai = ai_data.groupby('USAGE_DATE').agg({
                'CREDITS': 'sum',
                'TOTAL_TOKENS': 'sum',
                'REQUEST_COUNT': 'sum',
                'UNIQUE_USERS': 'sum'
            }).reset_index()
            
            # Format dates as YYYY-MM-DD strings
            daily_ai['USAGE_DATE_STR'] = pd.to_datetime(daily_ai['USAGE_DATE']).dt.strftime('%Y-%m-%d')
            
            fig_ai_trend = make_subplots(
                rows=2, cols=2,
                subplot_titles=("Daily AI Credits", "Daily Token Usage", "Daily Requests", "Daily Active Users"),
                specs=[[{"secondary_y": False}, {"secondary_y": False}],
                       [{"secondary_y": False}, {"secondary_y": False}]]
            )
            
            fig_ai_trend.add_trace(
                go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['CREDITS'], mode='lines+markers', name='Credits'),
                row=1, col=1
            )
            
            fig_ai_trend.add_trace(
                go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['TOTAL_TOKENS'], mode='lines+markers', name='Tokens'),
                row=1, col=2
            )
            
            fig_ai_trend.add_trace(
                go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['REQUEST_COUNT'], mode='lines+markers', name='Requests'),
                row=2, col=1
            )
            
            fig_ai_trend.add_trace(
                go.Scatter(x=daily_ai['USAGE_DATE_STR'], y=daily_ai['UNIQUE_USERS'], mode='lines+markers', name='Users'),
                row=2, col=2
            )
            
            fig_ai_trend.update_layout(
                height=600, 
                title_text="AI Usage Trends", 
                showlegend=False,
                xaxis_tickformat='%Y-%m-%d',
                xaxis2_tickformat='%Y-%m-%d',
                xaxis3_tickformat='%Y-%m-%d',
                xaxis4_tickformat='%Y-%m-%d'
            )
            st.plotly_chart(fig_ai_trend, use_container_width=True)
            
            # Function and model breakdown
            col1, col2 = st.columns(2)
            
            with col1:
                function_costs = ai_data.groupby('FUNCTION_NAME')['CREDITS'].sum().reset_index()
                fig_functions = px.pie(
                    function_costs,
                    values='CREDITS',
                    names='FUNCTION_NAME',
                    title="AI Credits by Function"
                )
                st.plotly_chart(fig_functions, use_container_width=True)
            
            with col2:
                model_costs = ai_data.groupby('MODEL_NAME')['CREDITS'].sum().reset_index()
                fig_models = px.pie(
                    model_costs,
                    values='CREDITS', 
                    names='MODEL_NAME',
                    title="AI Credits by Model"
                )
                st.plotly_chart(fig_models, use_container_width=True)
            
            # Detailed AI usage table
            st.subheader("📊 Detailed AI Usage")
            st.dataframe(ai_data, use_container_width=True)
            
            # AI optimization recommendations
            st.subheader("💡 AI Cost Optimization")
            
            # High cost per request
            expensive_ai = ai_data[ai_data['CREDITS_PER_REQUEST'] > ai_data['CREDITS_PER_REQUEST'].quantile(0.9)]
            if not expensive_ai.empty:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>💰 High Cost AI Operations</h4>
                    <p>These AI function/model combinations have high cost per request:</p>
                </div>
                """, unsafe_allow_html=True)
                
                for _, op in expensive_ai.head(5).iterrows():
                    st.warning(f"🤖 **{op['FUNCTION_NAME']} ({op['MODEL_NAME']})**: {op['CREDITS_PER_REQUEST']:.6f} credits/request - {op['REQUEST_COUNT']} requests")

        else:
            st.info("No AI usage detected for the selected time period.")

    with tab6:
        st.header("⚡ Query Efficiency")
        
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

    with tab7:
        st.header("📈 Cost Forecasting")
        
        # Get historical data for forecasting
        forecast_query = f"""
        WITH daily_costs AS (
            SELECT 
                DATE_TRUNC('day', qa.START_TIME)::DATE as usage_date,
                SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) + COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as daily_credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
            WHERE qa.START_TIME >= '{start_date - timedelta(days=90)}' AND qa.START_TIME <= '{end_date}'
            GROUP BY 1
            ORDER BY 1
        )
        SELECT 
            usage_date,
            daily_credits,
            AVG(daily_credits) OVER (ORDER BY usage_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as seven_day_avg,
            AVG(daily_credits) OVER (ORDER BY usage_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as thirty_day_avg
        FROM daily_costs
        """
        
        forecast_data = session.sql(forecast_query).to_pandas()
        
        if not forecast_data.empty and len(forecast_data) > 7:
            # Basic analysis
            recent_avg = forecast_data.tail(7)['DAILY_CREDITS'].mean()
            
            # Forecast metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Recent 7-day Average", f"{recent_avg:.1f} credits/day")
            
            with col2:
                projected_monthly = recent_avg * 30
                st.metric("Projected Monthly", f"{projected_monthly:,.0f} credits")
            
            # Trend visualization
            fig_forecast = go.Figure()
            
            # Historical data
            fig_forecast.add_trace(go.Scatter(
                x=forecast_data['USAGE_DATE'],
                y=forecast_data['DAILY_CREDITS'],
                mode='lines+markers',
                name='Daily Credits',
                line=dict(color='blue')
            ))
            
            # 7-day moving average
            fig_forecast.add_trace(go.Scatter(
                x=forecast_data['USAGE_DATE'],
                y=forecast_data['SEVEN_DAY_AVG'],
                mode='lines',
                name='7-day Average',
                line=dict(color='orange', width=2)
            ))
            
            # 30-day moving average
            fig_forecast.add_trace(go.Scatter(
                x=forecast_data['USAGE_DATE'],
                y=forecast_data['THIRTY_DAY_AVG'],
                mode='lines',
                name='30-day Average',
                line=dict(color='red', width=2)
            ))
            
            # Simple linear projection for next 30 days
            if len(forecast_data) >= 30:
                # Use linear regression for trend
                x_vals = np.arange(len(forecast_data))
                y_vals = forecast_data['DAILY_CREDITS'].values
                z = np.polyfit(x_vals[-30:], y_vals[-30:], 1)  # Use last 30 days for trend
                
                # Project next 30 days
                future_dates = [forecast_data['USAGE_DATE'].iloc[-1] + timedelta(days=i+1) for i in range(30)]
                future_x = np.arange(len(forecast_data), len(forecast_data) + 30)
                future_y = np.polyval(z, future_x)
                
                fig_forecast.add_trace(go.Scatter(
                    x=future_dates,
                    y=future_y,
                    mode='lines',
                    name='30-day Projection',
                    line=dict(color='green', dash='dash', width=2)
                ))
            
            fig_forecast.update_layout(
                title="Cost Trends and Forecasting",
                xaxis_title="Date",
                yaxis_title="Daily Credits",
                height=500
            )
            
            st.plotly_chart(fig_forecast, use_container_width=True)
            
            # Monthly cost projection
            st.subheader("📊 Monthly Cost Projections")
            
            if len(forecast_data) >= 30:
                # Calculate projections based on different scenarios
                current_month_avg = forecast_data.tail(30)['DAILY_CREDITS'].mean()
                last_week_avg = forecast_data.tail(7)['DAILY_CREDITS'].mean()
                
                scenarios = {
                    "Conservative (30-day avg)": current_month_avg * 30,
                    "Current Trend (7-day avg)": last_week_avg * 30,
                    "Growth Scenario (+20%)": last_week_avg * 30 * 1.2,
                    "Optimization Scenario (-15%)": last_week_avg * 30 * 0.85
                }
                
                scenario_df = pd.DataFrame([
                    {"Scenario": k, "Monthly Credits": v, "vs Current": ((v - current_month_avg * 30) / (current_month_avg * 30)) * 100}
                    for k, v in scenarios.items()
                ])
                
                st.dataframe(scenario_df, use_container_width=True)
                
                # Cost scenarios chart
                fig_scenarios = px.bar(
                    scenario_df,
                    x='Scenario',
                    y='Monthly Credits',
                    color='vs Current',
                    color_continuous_scale='RdYlGn_r',
                    title="Monthly Cost Scenarios"
                )
                st.plotly_chart(fig_scenarios, use_container_width=True)

    with tab8:
        st.header("🎯 Consolidated Savings Opportunities")
        
        st.markdown("""
        <div class="metric-card">
            <h3>💡 Cost Optimization Summary</h3>
            <p>This section consolidates all identified savings opportunities across your Snowflake account.</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Aggregate all savings opportunities
        savings_opportunities = []
        
        # Warehouse optimization savings
        warehouse_savings_query = f"""
        WITH warehouse_inefficiency AS (
            SELECT 
                q.WAREHOUSE_NAME,
                SUM(q.CREDITS_ATTRIBUTED_COMPUTE) as total_credits,
                COUNT(DISTINCT q.QUERY_ID) as total_queries,
                AVG(qh.EXECUTION_TIME)/1000 as avg_execution_seconds,
                COUNT(DISTINCT q.USER_NAME) as unique_users,
                COUNT(DISTINCT DATE_TRUNC('day', q.START_TIME)) as active_days
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
            WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
            AND q.WAREHOUSE_NAME IS NOT NULL
            GROUP BY 1
        )
        SELECT 
            WAREHOUSE_NAME,
            total_credits,
            CASE 
                WHEN unique_users < 2 THEN total_credits * 0.3
                WHEN avg_execution_seconds > 120 THEN total_credits * 0.2  
                WHEN (total_credits / NULLIF(total_queries, 0)) > 0.01 THEN total_credits * 0.15
                ELSE 0
            END as potential_savings,
            CASE 
                WHEN unique_users < 2 THEN 'Low utilization - consider consolidation'
                WHEN avg_execution_seconds > 120 THEN 'Long execution times - consider optimization'
                WHEN (total_credits / NULLIF(total_queries, 0)) > 0.01 THEN 'High cost per query - review sizing'
                ELSE 'No issues identified'
            END as recommendation
        FROM warehouse_inefficiency
        WHERE total_credits > 1
        ORDER BY potential_savings DESC
        """
        
        wh_savings = session.sql(warehouse_savings_query).to_pandas()
        total_wh_savings = wh_savings['POTENTIAL_SAVINGS'].sum() if not wh_savings.empty else 0
        
        # Query efficiency savings
        query_savings_query = f"""
        SELECT 
            SUM(CASE WHEN qh.EXECUTION_STATUS = 'FAILED' THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.5 ELSE 0 END) as failed_query_waste,
            SUM(CASE WHEN (qh.BYTES_SPILLED_TO_LOCAL_STORAGE + qh.BYTES_SPILLED_TO_REMOTE_STORAGE) > 0 
                THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.2 ELSE 0 END) as spillage_waste,
            SUM(CASE WHEN qh.EXECUTION_TIME > 300000 THEN q.CREDITS_ATTRIBUTED_COMPUTE * 0.1 ELSE 0 END) as long_query_waste
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY q
        JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON q.QUERY_ID = qh.QUERY_ID
        WHERE q.START_TIME >= '{start_date}' AND q.START_TIME <= '{end_date}'
        """
        
        query_savings = session.sql(query_savings_query).to_pandas()
        
        if not query_savings.empty:
            failed_savings = query_savings['FAILED_QUERY_WASTE'].iloc[0] or 0
            spillage_savings = query_savings['SPILLAGE_WASTE'].iloc[0] or 0
            long_query_savings = query_savings['LONG_QUERY_WASTE'].iloc[0] or 0
        else:
            failed_savings = spillage_savings = long_query_savings = 0
        
        # AI optimization savings
        ai_savings_query = f"""
        WITH expensive_ai AS (
            SELECT 
                SUM(cf.TOKEN_CREDITS) as total_ai_credits,
                AVG(cf.TOKEN_CREDITS / NULLIF(cf.TOKENS, 0)) as avg_cost_per_token
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_FUNCTIONS_QUERY_USAGE_HISTORY cf
            JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON cf.QUERY_ID = qh.QUERY_ID
            WHERE qh.START_TIME >= '{start_date}' AND qh.START_TIME <= '{end_date}'
        )
        SELECT 
            total_ai_credits,
            total_ai_credits * 0.1 as potential_ai_savings  -- Assume 10% savings from optimization
        FROM expensive_ai
        """
        
        ai_savings_data = session.sql(ai_savings_query).to_pandas()
        ai_savings_amount = ai_savings_data['POTENTIAL_AI_SAVINGS'].iloc[0] if not ai_savings_data.empty else 0
        
        # Total savings calculation
        total_potential_savings = (
            total_wh_savings + 
            failed_savings + 
            spillage_savings + 
            long_query_savings + 
            ai_savings_amount
        )
        
        # Display savings summary
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Potential Savings", f"{total_potential_savings:.1f} credits")
        
        with col2:
            st.metric("Warehouse Optimization", f"{total_wh_savings:.1f} credits")
        
        with col3:
            query_total_savings = failed_savings + spillage_savings + long_query_savings
            st.metric("Query Optimization", f"{query_total_savings:.1f} credits")
        
        with col4:
            st.metric("AI Optimization", f"{ai_savings_amount:.1f} credits")
        
        # Savings breakdown chart
        if total_potential_savings > 0:
            savings_breakdown = pd.DataFrame({
                'Category': ['Warehouse Optimization', 'Query Failures', 'Memory Spillage', 'Long Queries', 'AI Optimization'],
                'Potential Savings': [total_wh_savings, failed_savings, spillage_savings, long_query_savings, ai_savings_amount]
            })
            
            fig_savings = px.pie(
                savings_breakdown,
                values='Potential Savings',
                names='Category',
                title="Potential Savings by Category",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_savings, use_container_width=True)
            
            # Detailed recommendations
            st.subheader("📋 Prioritized Action Plan")
            
            if total_wh_savings > 0:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>🏭 Warehouse Optimization (Highest Impact)</h4>
                    <ul>
                        <li>Review underutilized warehouses for consolidation opportunities</li>
                        <li>Right-size warehouses based on actual workload patterns</li>
                        <li>Implement auto-suspend policies for idle warehouses</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            if query_total_savings > 0:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>⚡ Query Optimization</h4>
                    <ul>
                        <li>Provide query performance training to high-cost users</li>
                        <li>Implement query monitoring and alerting</li>
                        <li>Review and optimize frequently failing queries</li>
                        <li>Address memory spillage through query tuning or warehouse sizing</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            if ai_savings_amount > 0:
                st.markdown("""
                <div class="savings-opportunity">
                    <h4>🤖 AI Cost Management</h4>
                    <ul>
                        <li>Review AI function usage patterns for optimization opportunities</li>
                        <li>Consider model selection based on cost-effectiveness</li>
                        <li>Implement AI usage governance and monitoring</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            # Implementation timeline
            st.subheader("🗓️ Implementation Timeline")
            
            timeline_data = pd.DataFrame({
                'Priority': ['High', 'High', 'Medium', 'Medium', 'Low'],
                'Action': [
                    'Consolidate underutilized warehouses',
                    'Implement auto-suspend policies', 
                    'Query optimization training',
                    'AI usage governance',
                    'Advanced query monitoring'
                ],
                'Estimated Savings': [
                    total_wh_savings * 0.6,
                    total_wh_savings * 0.4,
                    query_total_savings * 0.7,
                    ai_savings_amount,
                    query_total_savings * 0.3
                ],
                'Timeline': ['1-2 weeks', '1 week', '2-4 weeks', '2-3 weeks', '4-6 weeks']
            })
            
            st.dataframe(timeline_data, use_container_width=True)
        
        else:
            st.success("🎉 Great! No major cost optimization opportunities identified. Your Snowflake account appears to be well-optimized!")
            
            st.markdown("""
            <div class="metric-card">
                <h4>✅ Best Practices Checklist</h4>
                <ul>
                    <li>Regular monitoring of warehouse utilization</li>
                    <li>Ongoing query performance optimization</li>
                    <li>AI usage cost tracking</li>
                    <li>User education on efficient query practices</li>
                    <li>Periodic cost trend analysis</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

else:
    st.error("Please select a valid date range to begin analysis.")

# Footer
st.markdown("---")
st.markdown("**Note:** This dashboard requires ACCOUNTADMIN privileges and access to SNOWFLAKE.ACCOUNT_USAGE views. Savings calculations are estimates based on observed patterns and should be validated before implementation.")
