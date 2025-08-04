from utils import *
from 1_bronze_STREAM_pipeline import *
import dlt
from pyspark.sql.functions import col, concat, current_timestamp, lit

              

# SILVER: Trade Report for Non-Nextshares
@dlt.table(
    name="tr_silver_stream",
    comment="Silver clean Trade Report for Non-Nextshares table"
)
@dlt.expect_or_fail("tracking_id", IS NOT NULL)
def tr_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  return spark.readStream
              .table("tr_bronze_raw_stream")
              .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "consolidated_volume")
              .trigger(processingTime=”55 seconds”)                       # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")


# SILVER: Trade Report for Nextshares
@dlt.table(
    name="trn_silver_stream",
    comment="Silver clean Trade Report for Nextshares table"
)
@dlt.expect_or_fail("tracking_id", IS NOT NULL)
def trn_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  return spark.readStream
              .table("trn_bronze_raw_stream")
              .select("tracking_id", "symbol", "security_class", "price", "size", "sale_condition", "consolidated_volume")
              .trigger(processingTime=”55 seconds”)                       # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")


# SILVER: Stock Trading Action
@dlt.table(
    name="sta_silver_stream",
    comment="Silver clean Stock Trading Action table"
)
@dlt.expect_or_fail("tracking_id", IS NOT NULL)
def sta_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  return spark.readStream
              .table("sta_bronze_raw_stream")
              .select("tracking_id", "symbol", "trading_state")
              .trigger(processingTime=”55 seconds”)                       # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")


# SILVER: End of Day Trade Summary
@dlt.table(
    name="eods_silver_stream",
    comment="Silver clean End of Day Trade Summary table"
)
@dlt.expect_or_fail("tracking_id", IS NOT NULL)
def eods_silver():
  # Since we read the bronze table as a stream, this silver table is also
  # updated incrementally.
  return spark.readStream
              .table("eods_bronze_raw_stream")
              .select("tracking_id", "symbol", "high_price", "low_price", "nocp", "consolidated_volume", )
              .trigger(processingTime=”55 seconds”)                       # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")

