# Import python packages
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Cost Forecasting - Snowflake Dashboard",
    page_icon="📈",
    layout="wide"
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

st.title("📈 Cost Forecasting")
st.markdown("Predict future costs based on historical trends and usage patterns")

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
        value=date.today() - timedelta(days=-1),
        key="end_date"
    )

if start_date and end_date and start_date <= end_date:
    # Get current period data for forecasting
    forecast_query = f"""
    SELECT 
        DATE_TRUNC('day', qa.START_TIME)::DATE as usage_date,
        SUM(qa.CREDITS_ATTRIBUTED_COMPUTE + COALESCE(qa.CREDITS_USED_QUERY_ACCELERATION, 0) + COALESCE(qh.CREDITS_USED_CLOUD_SERVICES, 0)) as daily_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ATTRIBUTION_HISTORY qa
    LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh ON qa.QUERY_ID = qh.QUERY_ID
    WHERE qa.START_TIME >= '{start_date}' AND qa.START_TIME <= '{end_date}'
    GROUP BY 1
    ORDER BY 1
    """
    
    forecast_data = session.sql(forecast_query).to_pandas()
    
    if not forecast_data.empty:
        # Calculate current period statistics
        total_credits = forecast_data['DAILY_CREDITS'].sum()
        period_days = len(forecast_data)
        avg_daily_credits = total_credits / period_days if period_days > 0 else 0
        
        # Forecast metrics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Current Period Total", f"{total_credits:,.1f} credits")
        
        with col2:
            st.metric("Daily Average", f"{avg_daily_credits:.1f} credits/day")
        
        with col3:
            projected_monthly = avg_daily_credits * 30
            st.metric("Projected Monthly", f"{projected_monthly:,.0f} credits")
        
        # Current period visualization with projection
        fig_forecast = go.Figure()
        
        # Current period data
        fig_forecast.add_trace(go.Scatter(
            x=forecast_data['USAGE_DATE'],
            y=forecast_data['DAILY_CREDITS'],
            mode='lines+markers',
            name='Daily Credits',
            line=dict(color='blue'),
            marker=dict(size=6)
        ))
        
        # Add average line for current period
        fig_forecast.add_trace(go.Scatter(
            x=forecast_data['USAGE_DATE'],
            y=[avg_daily_credits] * len(forecast_data),
            mode='lines',
            name=f'Period Average ({avg_daily_credits:.1f})',
            line=dict(color='orange', width=2, dash='dot')
        ))
        
        # Project future 30 days based on current period average
        if period_days >= 3:  # Need at least 3 days for meaningful projection
            # Use linear regression on current period for trend
            x_vals = np.arange(len(forecast_data))
            y_vals = forecast_data['DAILY_CREDITS'].values
            
            # Fit trend line to current period
            if len(x_vals) >= 2:
                z = np.polyfit(x_vals, y_vals, 1)
                
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
            title=f"Current Period Analysis & Projection ({period_days} days)",
            xaxis_title="Date",
            yaxis_title="Daily Credits",
            height=500
        )
        
        st.plotly_chart(fig_forecast, use_container_width=True)
        
        # Monthly cost projection based on current period
        st.subheader("📊 Monthly Cost Projections")
        
        # Calculate projections based on current period data
        base_monthly = avg_daily_credits * 30
        
        scenarios = {
            "Current Period Average": base_monthly,
            "Growth Scenario (+20%)": base_monthly * 1.2,
            "Optimization Scenario (-15%)": base_monthly * 0.85,
            "High Activity (+50%)": base_monthly * 1.5,
            "Cost Reduction (-30%)": base_monthly * 0.7
        }
        
        scenario_df = pd.DataFrame([
            {"Scenario": k, "Monthly Credits": v, "vs Base": ((v - base_monthly) / base_monthly) * 100}
            for k, v in scenarios.items()
        ])
        
        st.dataframe(scenario_df, use_container_width=True)
        
        # Cost scenarios chart
        fig_scenarios = px.bar(
            scenario_df,
            x='Scenario',
            y='Monthly Credits',
            color='vs Base',
            color_continuous_scale='RdYlGn_r',
            title="Monthly Cost Scenarios (Based on Current Period)"
        )
        fig_scenarios.update_xaxes(tickangle=45)
        st.plotly_chart(fig_scenarios, use_container_width=True)
        
        # Summary insights
        st.subheader("📋 Forecasting Insights")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"""
            **Current Period Analysis:**
            - **Period Length**: {period_days} days
            - **Total Usage**: {total_credits:,.1f} credits
            - **Daily Average**: {avg_daily_credits:.1f} credits
            - **Extrapolated Monthly**: {base_monthly:,.0f} credits
            """)
        
        with col2:
            # Calculate some basic trends if we have enough data
            if period_days >= 7:
                first_half = forecast_data.head(period_days//2)['DAILY_CREDITS'].mean()
                second_half = forecast_data.tail(period_days//2)['DAILY_CREDITS'].mean()
                trend = ((second_half - first_half) / first_half) * 100 if first_half > 0 else 0
                
                trend_indicator = "📈 Increasing" if trend > 5 else "📉 Decreasing" if trend < -5 else "➡️ Stable"
                
                st.markdown(f"""
                **Trend Analysis:**
                - **Period Trend**: {trend_indicator} ({trend:+.1f}%)
                - **Highest Day**: {forecast_data['DAILY_CREDITS'].max():.1f} credits
                - **Lowest Day**: {forecast_data['DAILY_CREDITS'].min():.1f} credits
                - **Variability**: {forecast_data['DAILY_CREDITS'].std():.1f} credits std dev
                """)
            else:
                st.markdown("""
                **Trend Analysis:**
                - Need more data points for detailed trend analysis
                - Consider selecting a longer date range for better insights
                """)

else:
    st.error("Please select a valid date range to begin analysis.")
