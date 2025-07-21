GRANT READ SESSION ON ACCOUNT TO ROLE ACCOUNTADMIN;

create schema snowflake_copy_cost_views.account_usage;
create schema snowflake_copy_cost_views.policies;
create schema snowflake_copy_cost_views.streamlit;

CREATE OR REPLACE ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy
AS (user_name VARCHAR) RETURNS BOOLEAN ->
    user_name = CURRENT_USER();

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY AS
SELECT * FROM snowflake.account_usage.CORTEX_ANALYST_USAGE_HISTORY;
ALTER TABLE snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (username);

/*CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.query_history AS
SELECT *,$base_url||'/#/compute/history/queries/'||QUERY_ID||'/profile' as query_id_url, FROM snowflake.account_usage.query_history;*/
ALTER TABLE snowflake_copy_cost_views.account_usage.query_history ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (user_name);

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history AS (SELECT t1.*, t2.* exclude (query_id, warehouse_id) FROM snowflake_copy_cost_views.account_usage.query_history t1
    INNER JOIN snowflake.account_usage.cortex_functions_query_usage_history t2
    ON t1.query_id = t2.query_id);

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.query_attribution_history AS
SELECT * FROM snowflake.account_usage.query_attribution_history;
ALTER TABLE snowflake_copy_cost_views.account_usage.query_attribution_history ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (user_name);

CREATE OR REPLACE STREAMLIT snowflake_copy_cost_views.streamlit.USER_USAGE_APP
  FROM @snowflake_copy_cost_views.stages.GITHUB_REPO_SF_USAGE/branches/main/user_app/
  MAIN_FILE = 'app.py'
  QUERY_WAREHOUSE = ADHOC_XS
  TITLE = 'User cost and usage'
  COMMENT = 'Streamlit frontend for user based usage';
