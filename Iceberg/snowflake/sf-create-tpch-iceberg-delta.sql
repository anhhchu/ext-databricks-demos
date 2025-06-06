CREATE OR REPLACE CATALOG INTEGRATION delta_catalog_integration
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = DELTA
  ENABLED = TRUE;

ALTER CATALOG INTEGRATION delta_catalog_integration SET REFRESH_INTERVAL_SECONDS = 86400;

SHOW ICEBERG TABLES; 

CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'ahc-demo-s3-west'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ahc-demo-s3-west/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::332745928618:role/ahc-role-snowflake'
            STORAGE_AWS_EXTERNAL_ID = 'snowflake_iceberg_external_id'
         )
      );

desc external volume iceberg_external_volume;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('iceberg_external_volume');

  
CREATE or replace ICEBERG TABLE region
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'region_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;


  
CREATE or replace ICEBERG TABLE lineitem
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'lineitem_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

select count(*) from lineitem;
  
CREATE or replace ICEBERG TABLE orders
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'orders_ext_new'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

  select * from orders;


CREATE or replace ICEBERG TABLE partsupp
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'partsupp_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE region
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'region_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE part
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'part_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;


CREATE or replace ICEBERG TABLE customer
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'customer_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;


CREATE or replace ICEBERG TABLE supplier
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'supplier_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE nation
  CATALOG = delta_catalog_integration
  BASE_LOCATION = 'nation_ext'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AUTO_REFRESH = TRUE;

  


