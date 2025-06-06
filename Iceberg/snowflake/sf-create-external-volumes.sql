CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'ahc-demo-s3-west'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://ahc-demo-s3-west/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::*****:role/ahc-role-snowflake'
            STORAGE_AWS_EXTERNAL_ID = 'snowflake_*******'
         )
      );

desc external volume iceberg_external_volume;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('iceberg_external_volume');