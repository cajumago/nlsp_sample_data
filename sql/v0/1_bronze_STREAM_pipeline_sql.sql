-- CREATE THE BRONZE STREAMING TABLES IN '1_bronze' SCHEMA AND INGEST THE CSV FILES


-- BRONZE: Trade Report for Non-Nextshares
CREATE OR REFRESH STREAMIN TABLE 1_bronze.tr_bronze_raw_stream
  COMMENT “Ingest Trade Report for Non-Nextshares JSON file from Nasdaq data shared marketplace”
  TBLPROPERTIES (
	  'quality' = 'bronze',				                  -- Adds a simple table property to the table
	  pipelines.reset.allowed = false 		          -- Prevent full table refreshes on the bronze table
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM read_files(
  '/Volumes/nlsp/0_demo/sample_raw/df_tr.csv',  -- Uses the '${source}/df_tr.csv' configuration variable set in the pipeline settings
  format => 'csv',
  delimiter => ',',                             -- Default value: ","
  header => true,
  mode => 'FAILFAST'
);



-- BRONZE: Trade Report for Nextshares
CREATE OR REFRESH STREAMIN TABLE 1_bronze.trn_bronze_raw_stream
  COMMENT "Ingest Trade Report for Nextshares JSON file from Nasdaq data shared marketplace"
  TBLPROPERTIES (
	  'quality' = 'bronze',				                  -- Adds a simple table property to the table
	  pipelines.reset.allowed = false 		          -- Prevent full table refreshes on the bronze table
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM read_files(
  '/Volumes/nlsp/0_demo/sample_raw/df_trn.csv',  -- Uses the '${source}/df_trn.csv' configuration variable set in the pipeline settings
  format => 'csv',
  delimiter => ',',                               -- Default value: ","
  header => true,
  mode => 'FAILFAST'
);



-- BRONZE: Stock Trading Action
CREATE OR REFRESH STREAMIN TABLE 1_bronze.sta_bronze_raw_stream
  COMMENT “Ingest Stock Trading Action JSON file from Nasdaq data shared marketplace”
  TBLPROPERTIES (
	  'quality' = 'bronze',				                  -- Adds a simple table property to the table
	  pipelines.reset.allowed = false 		          -- Prevent full table refreshes on the bronze table
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM read_files(
  '/Volumes/nlsp/0_demo/sample_raw/df_sta.csv',  -- Uses the '${source}/df_sta.csv' configuration variable set in the pipeline settings
  format => 'csv',
  delimiter => ',',                               -- Default value: ","
  header => true,
  mode => 'FAILFAST'
);



-- BRONZE: End of Day Trade Summary
CREATE OR REFRESH STREAMIN TABLE 1_bronze.eods_bronze_raw_stream
  COMMENT “Ingest End of Day Trade Summary JSON file from Nasdaq data shared marketplace”
  TBLPROPERTIES (
	  'quality' = 'bronze',				                  -- Adds a simple table property to the table
	  pipelines.reset.allowed = false 		          -- Prevent full table refreshes on the bronze table
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM read_files(
  '/Volumes/nlsp/0_demo/sample_raw/df_eods.csv',  --Uses the '${source}/df_eods.csv' configuration variable set in the pipeline settings
  format => 'csv',
  delimiter => ',',                               -- Default value: ","
  header => true,
  mode => 'FAILFAST'
);
