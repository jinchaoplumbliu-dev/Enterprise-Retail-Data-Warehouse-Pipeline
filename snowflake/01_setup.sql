-- =============================================================================
-- Snowflake foundation for the Instacart warehouse.
--
-- Run once in a Snowflake worksheet as ACCOUNTADMIN. Creates the compute, the
-- database/schemas, and a project-scoped role + grants that dbt and Airflow
-- connect as. Replace <YOUR_SNOWFLAKE_USER> at the bottom with the login name.
-- =============================================================================

use role accountadmin;

-- 1. COMPUTE: a small, cost-safe warehouse. Auto-suspends after 60s idle so a
--    trial account does not bleed credits; auto-resumes on the next query.
create warehouse if not exists instacart_wh
    warehouse_size      = 'xsmall'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true
    comment             = 'Compute for the Instacart dbt pipeline';

-- 2. STORAGE: the analytics database. Only `raw` is created here because the
--    pipeline loads it; dbt creates staging / intermediate / marts at run time.
create database if not exists instacart;
create schema   if not exists instacart.raw;

-- 3. SECURITY: a project role carries all the privileges; the user just
--    inherits the role. This is the idiomatic Snowflake pattern.
create role if not exists instacart_role;

grant usage     on warehouse instacart_wh   to role instacart_role;
grant usage     on database  instacart       to role instacart_role;
grant create schema on database instacart    to role instacart_role;  -- dbt needs this
grant all       on schema    instacart.raw   to role instacart_role;

-- Make sure the role keeps full access to whatever it creates later.
grant all on all     schemas in database instacart to role instacart_role;
grant all on future  schemas in database instacart to role instacart_role;

-- 4. Attach the role to your user. <-- EDIT THIS LINE.
grant role instacart_role to user <YOUR_SNOWFLAKE_USER>;

-- Quick sanity check (should run on the new warehouse without error):
use role instacart_role;
use warehouse instacart_wh;
use database instacart;
select current_role(), current_warehouse(), current_database();
