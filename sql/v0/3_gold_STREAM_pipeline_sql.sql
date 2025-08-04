-- GOLD: Joined Materialized View from all tables -nlsp=Nasdaq Last Sale Plus, obt=One Big Table

CREATE OR REFRESH MATERIALIZED VIEW 3_gold.nlsp_obt_gold
  COMMENT “Aggregate gold data for downstream analysis or ML”		
  TBLPROPERTIES (
    'quality' = 'gold',
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
    trading_state,
    high_price,
    low_price,
    nocp,
    consolidated_volume,
    count(DISTINCT symbol) AS total_daily_traders
FROM 2_silver.trn_silver trn           
    FULL JOIN 2_silver.tr_silver tr ON tr.tracking_id = trn.tracking_id
    FULL JOIN 2_silver.sta_silver sta ON trn.tracking_id = sta.tracking_id
    FULL JOIN 2_silver.eods_silver eods ON sta.tracking_id = eods.tracking_id;        
    -- GROUP BY total_daily_traders;