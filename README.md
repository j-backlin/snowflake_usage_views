# Snowflake Cost and Performance Analytics Dashboard
A Streamlit-based dashboard for analyzing Snowflake cost and performance metrics per user. This application provides insights into warehouse usage, AI function costs, query performance, and resource utilization.

Take note that the default install script will share the Streamlit app and secure views to the Public role (but with row access policies filtering rows per user), enabling all users to track their own individual usage. If that is not desired; modify the install script prior to execution.
## :rocket: Features
### Dashboard Tabs
1. **Warehouse Usage Analysis**
   - Track warehouse credit consumption over time
   - Visualize usage patterns by warehouse
   - Daily breakdown of warehouse costs
2. **AI Usage Analysis**
   - Monitor Cortex AI function usage and costs
   - Track token consumption and credits
   - Cortex Analyst (chatbot) usage statistics
3. **Spilled to Disk**
   - Identify queries that spill to disk storage
   - Monitor both local and remote spillage
   - Performance optimization insights
4. **Query Details**
   - Comprehensive query execution history
   - Performance metrics and error tracking
   - Direct links to Snowflake query profiles
5. **AI Query Details**
   - Detailed AI function query history
   - Token usage and model information
   - Performance analysis for AI workloads
6. **Most Expensive Queries**
   - Top cost-consuming queries
   - Both traditional and AI query costs
   - Resource consumption analysis
## :clipboard: Prerequisites
- **Snowflake Account**: Access to Snowflake with ACCOUNTADMIN role
## :gear: Setup
### Installation
```
CREATE WAREHOUSE IF NOT EXISTS ADHOC_XS WAREHOUSE_SIZE='X-SMALL' INITIALLY_SUSPENDED=TRUE AUTO_SUSPEND=5 AUTO_RESUME=TRUE;
create or replace database snowflake_copy_cost_views;
create schema snowflake_copy_cost_views.stages;

-- Create the API integration with Github
CREATE OR REPLACE API INTEGRATION GITHUB_INTEGRATION_SF_USAGE_VIEWS
  api_provider = git_https_api
  api_allowed_prefixes = ('https://github.com/j-backlin/')
  enabled = true
  comment='Git integration with Johans Github Repository.';

-- Create the integration with the Github demo repository
CREATE GIT REPOSITORY GITHUB_REPO_SF_USAGE
  ORIGIN = 'https://github.com/j-backlin/snowflake_usage_views' 
  API_INTEGRATION = 'GITHUB_INTEGRATION_SF_USAGE_VIEWS' 
  COMMENT = 'Github Repository from Johans with a Streamlit app for viewing usage of Snowflake.';

-- Fetch files from repo
ALTER GIT REPOSITORY GITHUB_REPO_SF_USAGE FETCH;

-- Run setup script from git
EXECUTE IMMEDIATE FROM @snowflake_copy_cost_views.stages.GITHUB_REPO_SF_USAGE/branches/main/setup.sql;
-- If execute immediate above fails, copy the content of the setup.sql file here and run it prior to continuing running the code below.

CREATE OR REPLACE STREAMLIT snowflake_copy_cost_views.streamlit.USER_USAGE_APP
  FROM @snowflake_copy_cost_views.stages.GITHUB_REPO_SF_USAGE/branches/main/user_app/
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = ADHOC_XS
  TITLE = 'User cost and usage'
  COMMENT = 'Streamlit frontend for user based usage';

GRANT USAGE ON STREAMLIT SNOWFLAKE_COPY_COST_VIEWS.STREAMLIT.USER_USAGE_APP TO ROLE PUBLIC;
```
## :rocket: Usage
### Using the Dashboard
1. **Select Date Range**: Use the date picker to choose your analysis period (default: last 7 days)
2. **Navigate Tabs**: Click through the six tabs to explore different aspects of your Snowflake usage
3. **Interactive Visualizations**:
   - Hover over charts for detailed information
   - Click on legend items to filter data
   - Use built-in zoom and pan features
4. **Data Export**: All dataframes can be exported using Streamlit's built-in export functionality
## :bar_chart: Data Sources
All data is sourced from Snowflake's `ACCOUNT_USAGE` schema:
- **Query Attribution History**: Warehouse usage and credit attribution
- **Cortex Functions Query Usage**: AI function usage and costs
- **Cortex Analyst Usage**: Chatbot usage statistics
- **Query History**: Comprehensive query execution data
## :wrench: Configuration
### Layout Settings
- Wide layout for optimal dashboard viewing
- Dark theme for charts (Plotly)
- Responsive design for different screen sizes
### Performance Considerations
- Date range selection impacts query performance
- Larger date ranges may require more processing time
- Consider data retention policies in Snowflake
## :chart_with_upwards_trend: Key Metrics Tracked
- **Credit Usage**: Warehouse and AI function costs
- **Query Performance**: Execution times and resource usage
- **Resource Utilization**: Spillage and scanning metrics
- **User Activity**: Per-user usage patterns
- **Error Tracking**: Failed queries and issues
## :shield: Security Notes
- The secure views have row access policies attached to filter only relevant rows for the user who is executing the query.
- The install script shares the secure views and streamlit app to the Public role by default. If that is not desired, copy the content of setup.sql and modify prior to execution.
## :memo: Notes
- The clickable query profile links in the tables are based on Organization + Account name: ```https://<orgname>-<account_name>.snowflakecomputing.com```
-   If using either of the below modify the code to have functional links.
-   Connection name: ```https://<orgname>-<connectionname>.snowflakecomputing.com```
-   Account locator (legacy): ```https://<accountlocator>.<region>.<cloud>.snowflakecomputing.com```
- Some features might require Snowflake Enterprise Edition (only tested with Enterprise edition)
- AI usage tracking requires Cortex functions to be enabled
- Historical data availability depends on Snowflake retention settings
## :link: Related Resources
- [Snowflake Account Usage Views](https://docs.snowflake.com/en/sql-reference/account-usage)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Snowflake Cortex AI](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- [Plotly Documentation](https://plotly.com/python/)
