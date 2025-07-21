-- drop database snowflake_copy_cost_views;
USE ROLE ACCOUNTADMIN;
GRANT READ SESSION ON ACCOUNT TO ROLE ACCOUNTADMIN;

--create database snowflake_copy_cost_views;
use database snowflake_copy_cost_views;
create schema snowflake_copy_cost_views.account_usage;
create schema snowflake_copy_cost_views.policies;
create schema snowflake_copy_cost_views.streamlit;

CREATE OR REPLACE ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy
AS (user_name VARCHAR) RETURNS BOOLEAN ->
    user_name = CURRENT_USER();

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY AS
SELECT * FROM snowflake.account_usage.CORTEX_ANALYST_USAGE_HISTORY;
ALTER TABLE snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (username);

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.query_history AS
SELECT *,$base_url||'/#/compute/history/queries/'||QUERY_ID||'/profile' as query_id_url, FROM snowflake.account_usage.query_history;
ALTER TABLE snowflake_copy_cost_views.account_usage.query_history ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (user_name);

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history AS (SELECT t1.*, t2.* exclude (query_id, warehouse_id) FROM snowflake_copy_cost_views.account_usage.query_history t1
    INNER JOIN snowflake.account_usage.cortex_functions_query_usage_history t2
    ON t1.query_id = t2.query_id);

CREATE OR REPLACE SECURE VIEW snowflake_copy_cost_views.account_usage.query_attribution_history AS
SELECT * FROM snowflake.account_usage.query_attribution_history;
ALTER TABLE snowflake_copy_cost_views.account_usage.query_attribution_history ADD ROW ACCESS POLICY snowflake_copy_cost_views.policies.user_row_access_policy ON (user_name);

grant usage on database snowflake_copy_cost_views to role identifier($role_access);
grant usage on schema snowflake_copy_cost_views.streamlit to role identifier($role_access);
grant select on view snowflake_copy_cost_views.account_usage.CORTEX_ANALYST_USAGE_HISTORY to role identifier($role_access);
grant select on view snowflake_copy_cost_views.account_usage.query_history to role identifier($role_access);
grant select on view snowflake_copy_cost_views.account_usage.cortex_functions_query_usage_history to role identifier($role_access);
grant select on view snowflake_copy_cost_views.account_usage.query_attribution_history to role identifier($role_access);
