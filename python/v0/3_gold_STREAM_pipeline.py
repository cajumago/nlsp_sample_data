from utils import *
from 2_silver_STREAM_pipeline import *
import dlt
from pyspark.sql.functions import col, concat, current_timestamp, lit, expr


# GOLD: Trade Report for Non-Nextshares
@dlt.table(
    name="nlsp_obt_gold",
    comment="Aggregate gold data for downstream analysis or ML"
)
def tr_gold():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  return spark.readStream.table("tr_bronze_raw_stream") \
                .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "trading_state", "high_price", "low_price", "nocp", "consolidated_volume") \
                .join(trn_silver_stream, tr_silver_stream.tracking_id == trn_silver_stream.tracking_id, "full") \
                .join(sta_silver_stream, tr_silver_stream.tracking_id == sta_silver_stream.tracking_id, "full") \
                .join(eods_silver_stream, tr_silver_stream.tracking_id == eods_silver_stream.tracking_id, "full")



# # Process SCD type 2 updates

# @dlt.view
# def users():
#   return spark.readStream.table("cdc_data.users")

# dlt.create_streaming_table("target")

# dlt.create_auto_cdc_flow(
#   target = "target",
#   source = "users",
#   keys = ["userId"],
#   sequence_by = col("sequenceNum"),
#   apply_as_deletes = expr("operation = 'DELETE'"),
#   except_column_list = ["operation", "sequenceNum"],
#   stored_as_scd_type = "2"
# )