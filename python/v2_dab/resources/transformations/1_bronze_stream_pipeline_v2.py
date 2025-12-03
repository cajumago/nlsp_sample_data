import sys
sys.path.append('/')
from utils import *     # Root directory to the .py file

from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType, LongType, FloatType
from pyspark.sql.functions import col, current_timestamp


# Store the target configuration environment in the variable target
# target = spark.conf.get("target") #--> for testing

# Store the target raw data configuration in the variable source
source = spark.conf.get("source")

# Target table for records output by streaming operations
# @dp.create_streaming_table(name = "<table-name>", ...)


# Create a streaming table for 'Trade Report for Non-Nextshares' data
@dp.table(
    name="tr_bronze_stream",
    comment="Ingest Trade Report for Non-Nextshares CSV file from Nasdaq data shared marketplace"
)
def tr_csv():
    # Define the schema for 'Trade Report for Non-Nextshares' CSV data
    tr_csv_schema = StructType(fields=[
        StructField("_c0", IntegerType(), True),
        StructField("soup_partition", IntegerType(), True),
        StructField("soup_sequence", IntegerType(), True),
        StructField("tracking_id", LongType(), False),
        StructField("msg_type", StringType(), True),
        StructField("market_center", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("security_class", StringType(), True),
        StructField("control_number", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("size", IntegerType(), True),
        StructField("sale_condition", StringType(), True),
        StructField("consolidated_volume", IntegerType(), True),
        ])
    return (spark
            .readStream
            .format(“cloudFiles”)
            .schema(tr_csv_schema)
            .option(“cloudFiles.format”, “csv”)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # "rescue", "failOnNewColumns"
            .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
            .load(“${source}/tr”) # For Lakeflow SDP                      # f"{url_trn}?{api_key}", f"/Volumes/nasdaq_raw/tr-historical"
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("processing_time", col("current_timestamp()"))
            )


# Create a streaming table for 'Trade Report for Nextshares' data
@dp.table(
    name="trn_bronze_stream",
    comment="Ingest Trade Report for Nextshares CSV file from Nasdaq data shared marketplace"
)
def trn_csv():
    # Define the schema for 'Trade Report for Nextshares' CSV data
    trn_csv_schema = StructType(fields=[
        StructField("_c0", IntegerType(), True),
        StructField("soup_partition", IntegerType(), True),
        StructField("soup_sequence", IntegerType(), True),
        StructField("tracking_id", LongType(), False),
        StructField("msg_type", StringType(), True),
        StructField("market_center", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("security_class", StringType(), True),
        StructField("control_number", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("size", IntegerType(), True),
        StructField("nav_premium", IntegerType(), True),
        StructField("sale_condition", StringType(), True),
        StructField("consolidated_volume", IntegerType(), True),
        ])
    return (spark
            .readStream
            .format(“cloudFiles”)
            .schema(trn_csv_schema)
            .option(“cloudFiles.format”, “csv”)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # "rescue", "failOnNewColumns"
            .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
            .load(“${source}/trn”) # For Lakeflow SDP                     # f"{url_trn}?{api_key}", f"/Volumes/nasdaq_raw/trn-historical"
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("processing_time", col("current_timestamp()"))
            )


# Create a streaming table for 'Stock Trading Action' data
@dp.table(
    name="sta_bronze_stream",
    comment="Ingest Stock Trading Action CSV file from Nasdaq data shared marketplace"
)
def sta_csv():
    # Define the schema for 'Stock Trading Action' CSV data
    sta_csv_schema = StructType(fields=[
        StructField("_c0", IntegerType(), True),
        StructField("soup_partition", IntegerType(), True),
        StructField("soup_sequence", IntegerType(), True),
        StructField("tracking_id", LongType(), False),
        StructField("msg_type", StringType(), True),
        StructField("filler", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("market", StringType(), True),
        StructField("trading_state", StringType(), True),
        StructField("reason", StringType(), True),
        ])
    return (spark
            .readStream
            .format(“cloudFiles”)
            .schema(sta_csv_schema)
            .option(“cloudFiles.format”, “csv”)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # "rescue", "failOnNewColumns"
            .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
            .load(“${source}/sta”) # For Lakeflow SDP                     # f"{url_trn}?{api_key}", f"/Volumes/nasdaq_raw/sta-historical"
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("processing_time", col("current_timestamp()"))
            )


# Create a streaming table for 'End of Day Trade Summary' data
@dp.table(
    name="eods_bronze_stream",
    comment="Ingest End of Day Trade Summary CSV file from Nasdaq data shared marketplace"
)
def eods_csv():
    # Define the schema for 'End of Day Trade Summary' CSV data
    eods_csv_schema = StructType(fields=[
        StructField("_c0", IntegerType(), True),
        StructField("soup_partition", IntegerType(), True),
        StructField("soup_sequence", IntegerType(), True),
        StructField("tracking_id", LongType(), False),
        StructField("msg_type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("high_price", FloatType(), True),
        StructField("low_price", FloatType(), True),
        StructField("nocp", FloatType(), True),
        StructField("consolidated_volume", IntegerType(), True),
        ])
    return (spark
            .readStream
            .format(“cloudFiles”)
            .schema(eods_csv_schema)
            .option(“cloudFiles.format”, “csv”)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")    # "rescue", "failOnNewColumns"
            .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
            .load(“${source}/eods”) # For Lakeflow SDP                    # f"{url_trn}?{api_key}", f"/Volumes/nasdaq_raw/eods-historical"
            .withColumn("source_file", col("_metadata.file_name"))
            .withColumn("processing_time", col("current_timestamp()"))
            )





## @dp.table (general-purpose) VS @dp.create_streaming_table (specifically for creating streaming tables)
## Syntax dp.create_streaming_table: [Last updated on Nov 11, 2025]
# @dp.create_streaming_table(
#     name = "<table-name>",
#     comment = "<comment>",
#     spark_conf={"<key>" : "<value", "<key" : "<value>"},
#     table_properties={"<key>" : "<value>", "<key>" : "<value>"},
#     path="<storage-location-path>",
#     partition_cols=["<partition-column>", "<partition-column>"],
#     cluster_by_auto = <bool>,
#     cluster_by = ["<clustering-column>", "<clustering-column>"],
#     schema="schema-definition",
#     expect_all = {"<key>" : "<value", "<key" : "<value>"},
#     expect_all_or_drop = {"<key>" : "<value", "<key" : "<value>"},
#     expect_all_or_fail = {"<key>" : "<value", "<key" : "<value>"},
#     row_filter = "row-filter-clause"
# )