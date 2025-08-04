-- CREATE SILVER STREAMING TABLES IN '2_silver' SCHEMAS WITH DATA EXPECTATIONS

-- SILVER: Trade Report for Non-Nextshares
CREATE OR REFRESH STREAMIN TABLE 2_silver.tr_silver
  (
    CONSTRAINT valid_id EXPECT (tracking_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT “Silver clean Trade Report for Non-Nextshares table”
  TBLPROPERTIES (
    'quality' = 'silver',
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
    tracking_id,
    symbol,
    security_class,
    price,
    size,
    sale_condition,
    consolidated_volume
FROM STREAM 1_bronze.tr_bronze_raw;



#-- SILVER: Trade Report for Nextshares
CREATE OR REFRESH STREAMIN TABLE 2_silver.trn_silver
  (
    CONSTRAINT valid_id EXPECT (tracking_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT “Silver clean Trade Report for Nextshares table”
  TBLPROPERTIES (
    'quality' = 'silver',
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
    tracking_id,
    symbol,
    security_class,
    price,
    size,
    sale_condition,
    consolidated_volume
FROM STREAM 1_bronze.trn_bronze_raw;


#-- SILVER: Stock Trading Action
CREATE OR REFRESH STREAMIN TABLE 2_silver.sta_silver
  (
    CONSTRAINT valid_id EXPECT (tracking_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT “Silver clean Stock Trading Action table”
  TBLPROPERTIES (
    'quality' = 'silver',
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
    tracking_id,
    symbol,
    trading_state,
    -- timestamp(order_timestamp) AS order_timestamp
FROM STREAM 1_bronze.sta_bronze_raw;


#-- SILVER: End of Day Trade Summary
CREATE OR REFRESH STREAMIN TABLE 2_silver.eods_silver
  (
    CONSTRAINT valid_id EXPECT (tracking_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT “Silver clean End of Day Trade Summary table”
  TBLPROPERTIES (
    'quality' = 'silver',
    'created.by.user' = 'Carlos', 'created.date' = '07-30-2025'
  )
AS
SELECT
    tracking_id,
    symbol,
    high_price,
    low_price,
    nocp,
    consolidated_volume
FROM STREAM 1_bronze.eods_bronze_raw;