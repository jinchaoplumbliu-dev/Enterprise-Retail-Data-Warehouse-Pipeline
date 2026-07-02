-- File format + external stage. The stage points at the S3 landing zone
-- through the storage integration, so no keys live here.

use role instacart_role;
use warehouse instacart_wh;
use database instacart;
use schema raw;

-- header row, quoted text fields, empty -> NULL
create file format if not exists csv_ff
    type = csv
    skip_header = 1
    field_optionally_enclosed_by = '"'
    empty_field_as_null = true
    null_if = ('');

create stage if not exists s3_stage
    storage_integration = s3_int
    url = 's3://snow-bucket-jin/raw/'
    file_format = csv_ff;

-- if this lists the files, the trust handshake works and COPY INTO will too
list @s3_stage;
