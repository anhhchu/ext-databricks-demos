-- create schema iceberg;

CREATE or replace ICEBERG TABLE region
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'region'
  AUTO_REFRESH = TRUE;


CREATE or replace ICEBERG TABLE lineitem
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'lineitem'
  AUTO_REFRESH = TRUE;

  select count(*) from lineitem;

CREATE or replace ICEBERG TABLE orders
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'orders'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE partsupp
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'partsupp'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE part
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'part'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE customer
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'customer'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE nation
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'nation'
  AUTO_REFRESH = TRUE;

CREATE or replace ICEBERG TABLE supplier
  CATALOG = uc_int_oauth_vended_new
  CATALOG_NAMESPACE = 'tpch'
  CATALOG_TABLE_NAME = 'supplier'
  AUTO_REFRESH = TRUE;