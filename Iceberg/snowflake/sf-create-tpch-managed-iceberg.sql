create schema managed_iceberg;

CREATE or REPLACE ICEBERG TABLE region like uc.native.region
  CATALOG = 'snowflake'
  BASE_LOCATION = 'region_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume';

INSERT INTO region SELECT * FROM uc.native.region;

select * from region;

  
CREATE or REPLACE ICEBERG TABLE lineitem 
  CATALOG = 'snowflake'
  BASE_LOCATION = 'lineitem_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.lineitem;


  
CREATE or REPLACE ICEBERG TABLE orders
  CATALOG = 'snowflake'
  BASE_LOCATION = 'orders_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.orders;


CREATE or REPLACE ICEBERG TABLE partsupp
  CATALOG = 'snowflake'
  BASE_LOCATION = 'partsupp_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.partsupp;



CREATE or REPLACE ICEBERG TABLE part
  CATALOG = 'snowflake'
  BASE_LOCATION = 'part_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.part;


CREATE or REPLACE ICEBERG TABLE customer
  CATALOG = 'snowflake'
  BASE_LOCATION = 'customer_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
   AS SELECT * FROM uc.native.customer;


CREATE or REPLACE ICEBERG TABLE supplier
  CATALOG = 'snowflake'
  BASE_LOCATION = 'supplier_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.supplier;

CREATE or REPLACE ICEBERG TABLE nation
  CATALOG = 'snowflake'
  BASE_LOCATION = 'nation_snowflake'
  EXTERNAL_VOLUME = 'iceberg_external_volume'
  AS SELECT * FROM uc.native.nation;
