-- Before performing any data analysis, make sure to run the pipeline to materialize the sample datasets. The tables referenced in this notebook depend on that step.


SHOW CATALOGS
SHOW SCHEMAS

CREATE CATALOG nlsp; 


CREATE VOLUME sample_raw;
CREATE SCHEMA IF NOT EXISTS 1_bronze;
CREATE SCHEMA IF NOT EXISTS 2_silver;
CREATE SCHEMA IF NOT EXISTS 3_gold;


USE CATALOG nlsp;
USE SCHEMA 1_bronze;


SELECT 
  current_catalog(),
  current_schema();



-- PERMISSIVE (default): nulls are inserted for fields that could not be parsed correctly
-- DROPMALFORMED: drops lines that contain fields that could not be parsed
-- FAILFAST: aborts the reading if any malformed data is found

SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM read_files(
  '/Volumes/nlsp/0_demo/sample_raw/df_tr.csv',
  format => 'csv',
  delimiter => ',',                               -- Default value: ","
  header => true,
  mode => 'FAILFAST'
)
LIMIT 5;