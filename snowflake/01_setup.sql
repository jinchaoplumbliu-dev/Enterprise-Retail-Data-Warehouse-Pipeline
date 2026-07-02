-- Snowflake foundation: warehouse, database, project role + grants.
-- Run once in a worksheet as ACCOUNTADMIN, replacing <YOUR_SNOWFLAKE_USER>
-- at the bottom with your login name.

use role accountadmin;

-- xsmall + 60s auto-suspend so a trial account doesn't bleed credits
create warehouse if not exists instacart_wh
    warehouse_size      = 'xsmall'
    auto_suspend        = 60
    auto_resume         = true
    initially_suspended = true
    comment             = 'Compute for the Instacart dbt pipeline';

-- only raw is created here; dbt creates staging/intermediate/marts at run time
create database if not exists instacart;
create schema   if not exists instacart.raw;

-- a project role carries the privileges, users just inherit the role
create role if not exists instacart_role;

grant usage     on warehouse instacart_wh   to role instacart_role;
grant usage     on database  instacart       to role instacart_role;
grant create schema on database instacart    to role instacart_role;  -- dbt needs this
grant all       on schema    instacart.raw   to role instacart_role;

-- keep full access to schemas the role creates later
grant all on all     schemas in database instacart to role instacart_role;
grant all on future  schemas in database instacart to role instacart_role;

-- EDIT: attach the role to your user
grant role instacart_role to user <YOUR_SNOWFLAKE_USER>;

-- sanity check, should run on the new warehouse without error
use role instacart_role;
use warehouse instacart_wh;
use database instacart;
select current_role(), current_warehouse(), current_database();
