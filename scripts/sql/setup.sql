
USE DATABASE OLIST_WAREHOUSE;
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION s3_staging_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::724769809986:role/olist-snowflake-s3-access' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://olist-raw-staging-724769809986/'); 

CREATE OR REPLACE STORAGE INTEGRATION s3_staging_dev_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::724769809986:role/olist-snowflake-s3-access-dev' 
  STORAGE_ALLOWED_LOCATIONS = ('s3://olist-raw-staging-dev-724769809986/');

DESCRIBE STORAGE INTEGRATION s3_staging_integration;


CREATE OR REPLACE STAGE my_s3_external_stage
  URL = 's3://olist-raw-staging-724769809986/'
  STORAGE_INTEGRATION = s3_staging_integration;


CREATE OR REPLACE STAGE my_s3_external_dev_stage
  URL = 's3://olist-raw-staging-dev-724769809986/'
  STORAGE_INTEGRATION = s3_staging_dev_integration;



