
create database uc;

use database uc;

create schema tpcds;

CREATE OR REPLACE CATALOG INTEGRATION uc_int_oauth_vended_new
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'default'
  REST_CONFIG = (
    CATALOG_URI = 'https://xxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg'
    WAREHOUSE = 'ahc_demos',
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = OAUTH
    OAUTH_TOKEN_URI = 'https://xxxxx.cloud.databricks.com/oidc/v1/token'
    OAUTH_CLIENT_ID = '69d2fa6d-f9ee-4e18-9372-5e5cf6c7b0ce'
    OAUTH_CLIENT_SECRET = 'dose********'
    OAUTH_ALLOWED_SCOPES = ('all-apis', 'sql')
  )
  ENABLED = TRUE
  REFRESH_INTERVAL_SECONDS = 3600;
  


SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('uc_int_oauth_vended_new');


CREATE OR REPLACE CATALOG INTEGRATION uc_int_pat
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'default'
  REST_CONFIG = (
    CATALOG_URI = 'https://xxxxx.cloud.databricks.com/api/2.1/unity-catalog/iceberg',
    WAREHOUSE = 'ahc_demos'
  )
  REST_AUTHENTICATION = (
    TYPE = BEARER
    BEARER_TOKEN = 'dap*************'
  )
  ENABLED = TRUE;

SELECT SYSTEM$VERIFY_CATALOG_INTEGRATION('uc_int_pat');

