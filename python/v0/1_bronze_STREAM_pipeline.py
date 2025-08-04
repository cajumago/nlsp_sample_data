import dlt
from pyspark.sql.functions import col
#import requests
from utils import *


@dlt.table(
    name="tr_bronze_raw_stream",
    comment="Ingest Trade Report for Non-Nextshares JSON file from Nasdaq data shared marketplace"
)
def tr_raw():
    return (spark
            .readStream
                .format(“cloudFiles”)
                .option(“cloudFiles.format”, “csv”)
                .option("cloudFiles.schemaEvolutionMode", "true")             # "rescue", "failOnNewColumns"
                .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
                .load(“${source}/tr”) # For Lakeflow Declarative Pipeline     # f"{url_trn}?{api_key}", "/Volumes/nasdaq_raw/tr-historical"
                .withColumn("source_file", col("_metadata.file_name"))
                .withColumn("processing_time", col("current_timestamp()"))
                .trigger(processingTime=”55 seconds”)                         # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")
            )


@dlt.table(
    name="trn_bronze_raw_stream",
    comment="Ingest Trade Report for Nextshares JSON file from Nasdaq data shared marketplace"
)
def trn_raw():
    return (spark
            .readStream
                .format(“cloudFiles”)
                .option(“cloudFiles.format”, “csv”)
                .option("cloudFiles.schemaEvolutionMode", "true")             # "rescue", "failOnNewColumns"
                .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
                .load(“${source}/trn”) # For Lakeflow Declarative Pipeline    # f"{url_trn}?{api_key}", "/Volumes/nasdaq_raw/tr-historical"
                .withColumn("source_file", col("_metadata.file_name"))
                .withColumn("processing_time", col("current_timestamp()"))
                .trigger(processingTime=”55 seconds”)                         # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")
            )


@dlt.table(
    name="sta_bronze_raw_stream",
    comment="Ingest Stock Trading Action JSON file from Nasdaq data shared marketplace"
)
def sta_raw():
    return (spark
            .readStream
                .format(“cloudFiles”)
                .option(“cloudFiles.format”, “csv”)
                .option("cloudFiles.schemaEvolutionMode", "true")             # "rescue", "failOnNewColumns"
                .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
                .load(“${source}/sta”) # For Lakeflow Declarative Pipeline    # f"{url_sta}?{api_key}", "/Volumes/nasdaq_raw/tr-historical"
                .withColumn("source_file", col("_metadata.file_name"))
                .withColumn("processing_time", col("current_timestamp()"))
                .trigger(processingTime=”55 seconds”)                         # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")
            )


@dlt.table(
    name="eods_bronze_raw_stream",
    comment="Ingest End of Day Trade Summary JSON file from Nasdaq data shared marketplace"
)
def eods_raw():
    return (spark
            .readStream
                .format(“cloudFiles”)
                .option(“cloudFiles.format”, “csv”)
                .option("cloudFiles.schemaEvolutionMode", "true")             # "rescue", "failOnNewColumns"
                .option("rescuedDataColumn", "_rescued_data")                 # makes sure that you don't lose data
                .load(“${source}/eods”) # For Lakeflow Declarative Pipeline   # f"{url_eods}?{api_key}", "/Volumes/nasdaq_raw/tr-historical"
                .withColumn("source_file", col("_metadata.file_name"))
                .withColumn("processing_time", col("current_timestamp()"))
                .trigger(processingTime=”55 seconds”)                         # DTL Pipeline mode: Triggered, Continuos | .trigger(continuous="1 second")
            )


# #-- Incremental Batch or Streaming - Auto Loader with SQL (DLT)
# CREATE OR REFRESH STREAMING TABLE catalog.schema.table
# SCHEDULE EVERY 1 HOUR
# AS
# SELECT *
# FROM STREAM read_files(
# 	‘<dir_path>’,
# 	format => ‘<file_type>’);