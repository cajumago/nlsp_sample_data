from pyspark import pipelines as dp
from pyspark.sql.functions import col, concat, current_timestamp, lit


# create a streaming table
# SILVER: Trade Report for Non-Nextshares
@dp.table(
    name="tr_silver_stream",
    comment="Silver clean Trade Report for Non-Nextshares table"
)
@dp.expect_or_fail("tracking_id", IS NOT NULL)
@dp.expect("non_negative_price", "price >= 0")
def tr_silver():
  # Since we read the bronze table as a stream, this silver table is also updated incrementally.
  return spark.readStream
              .table("tr_bronze_stream")
              .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "consolidated_volume")


# create a streaming table
# SILVER: Trade Report for Nextshares
@dp.table(
    name="trn_silver_stream",
    comment="Silver clean Trade Report for Nextshares table"
)
@dp.expect_or_fail("tracking_id", IS NOT NULL)
@dp.expect("non_negative_price", "price >= 0")
def trn_silver():
  # Since we read the bronze table as a stream, this silver table is also updated incrementally.
  return spark.readStream
              .table("trn_bronze_stream")
              .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "consolidated_volume")


# create a streaming table
# SILVER: Stock Trading Action
@dp.table(
    name="sta_silver_stream",
    comment="Silver clean Stock Trading Action table"
)
@dp.expect_or_fail("tracking_id", IS NOT NULL)
def sta_silver():
  # Since we read the bronze table as a stream, this silver table is also updated incrementally.
  return spark.readStream
              .table("sta_bronze_stream")
              .select("tracking_id", "symbol", "trading_state")


# create a streaming table
# SILVER: End of Day Trade Summary
@dp.table(
    name="eods_silver_stream",
    comment="Silver clean End of Day Trade Summary table"
)
@dp.expect_or_fail("tracking_id", IS NOT NULL)
@dp.expect("non_negative_high_price", "high_price >= 0")
@dp.expect("non_negative_low_price", "low_price >= 0")
@dp.expect("non_negative_nocp", "nocp >= 0")
def eods_silver():
  # Since we read the bronze table as a stream, this silver table is also updated incrementally.
  return spark.readStream
              .table("eods_bronze_stream")
              .select("tracking_id", "symbol", "high_price", "low_price", "nocp", "consolidated_volume", )





## Process SCD type 2 updates
# @dp.view
# def users():
#   return spark.readStream.table("cdc_data.users")

# dp.create_streaming_table("target")

# dp.create_auto_cdc_flow(
#   target = "target",
#   source = "users",
#   keys = ["userId"],
#   sequence_by = col("sequenceNum"),
#   apply_as_deletes = expr("operation = 'DELETE'"),
#   except_column_list = ["operation", "sequenceNum"],
#   stored_as_scd_type = "2"
# )