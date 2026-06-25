-- =============================================================================
-- Step 4b - File format + external stage.
--
-- The stage is a named reference to the S3 landing zone, reached THROUGH the
-- storage integration (so no keys live here). Once LIST works, Snowflake can
-- see the files and we can COPY INTO from them (Step 4c, done in Python).
-- =============================================================================

use role instacart_role;
use warehouse instacart_wh;
use database instacart;
use schema raw;

-- How to parse the CSVs: header row, quoted text fields, empty -> NULL.
create file format if not exists csv_ff
    type = csv
    skip_header = 1
    field_optionally_enclosed_by = '"'
    empty_field_as_null = true
    null_if = ('');

-- The external stage: points at the bucket prefix via the integration.
create stage if not exists s3_stage
    storage_integration = s3_int
    url = 's3://snow-bucket-jin/raw/'
    file_format = csv_ff;

-- Verify Snowflake can see the S3 files through the trust handshake.
-- Expect the reference CSVs + wave 001 files we uploaded in Step 2.
list @s3_stage;
