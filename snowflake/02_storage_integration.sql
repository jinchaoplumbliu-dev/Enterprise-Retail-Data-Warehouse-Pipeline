-- Storage integration: the trust handshake between Snowflake and AWS.
-- The two sides reference each other, so the setup is a back-and-forth;
-- follow the numbered steps. Run the Snowflake parts as ACCOUNTADMIN.

use role accountadmin;

-- (1) First, in AWS, create an IAM policy + role. Permission policy
--     (read-only, just this bucket's raw/ prefix):
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       { "Effect": "Allow",
--         "Action": ["s3:GetObject", "s3:GetObjectVersion"],
--         "Resource": "arn:aws:s3:::snow-bucket-jin/raw/*" },
--       { "Effect": "Allow",
--         "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
--         "Resource": "arn:aws:s3:::snow-bucket-jin",
--         "Condition": { "StringLike": { "s3:prefix": ["raw/*"] } } }
--     ]
--   }
--
--   For the role's trust policy, start with a placeholder (your own account as
--   principal); it gets fixed in step (4). Copy the new role's ARN.

-- (2) Create the integration with the role ARN from step (1).
create storage integration if not exists s3_int
    type = external_stage
    storage_provider = 's3'
    enabled = true
    storage_aws_role_arn = 'arn:aws:iam::834551938186:role/snowflake-s3-role'
    storage_allowed_locations = ('s3://snow-bucket-jin/raw/');

-- (3) Describe it and copy two values from the output:
--       STORAGE_AWS_IAM_USER_ARN   (Snowflake's own AWS identity)
--       STORAGE_AWS_EXTERNAL_ID    (a shared secret)
desc integration s3_int;

-- (4) Back in AWS, edit the role's trust relationship to exactly:
--
--   {
--     "Version": "2012-10-17",
--     "Statement": [
--       { "Effect": "Allow",
--         "Principal": { "AWS": "<STORAGE_AWS_IAM_USER_ARN>" },
--         "Action": "sts:AssumeRole",
--         "Condition": { "StringEquals": { "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>" } } }
--     ]
--   }

-- (5) Let the project role use the integration so it can build the stage next.
grant usage on integration s3_int to role instacart_role;
