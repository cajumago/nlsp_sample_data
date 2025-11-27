from 2_silver_STREAM_pipeline import *
from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat, current_timestamp, lit, expr


# create a materialized view
# GOLD: Trade Report for Non-Nextshares
@dp.materialized_view(
    name="nlsp_obt_gold",
    comment="Aggregate gold data for downstream analysis or ML"
)
def nlsp_gold():
  # Since we read the bronze table as a stream, this gold table is also updated incrementally.
  return spark.readStream.table("tr_bronze_raw_stream") \
                .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "trading_state", "high_price", "low_price", "nocp", "consolidated_volume") \
                .join(trn_silver_stream, tr_silver_stream.tracking_id == trn_silver_stream.tracking_id, "full") \
                .join(sta_silver_stream, tr_silver_stream.tracking_id == sta_silver_stream.tracking_id, "full") \
                .join(eods_silver_stream, tr_silver_stream.tracking_id == eods_silver_stream.tracking_id, "full")
