# SECRET_KEY = ""
api_key = "api_key=<YOURAPIKEY>"

urls = "https://data.nasdaq.com/api/v3/datatables/NDAQ/*.json"

url_tr = https://data.nasdaq.com/api/v3/datatables/NDAQ/TR.csv
url_trn = https://data.nasdaq.com/api/v3/datatables/NDAQ/TRN.csv
url_sta = https://data.nasdaq.com/api/v3/datatables/NDAQ/STA.csv
url_eods = https://data.nasdaq.com/api/v3/datatables/NDAQ/EODS.csv



# https://docs.databricks.com/aws/en/notebooks/eda-tutorial
import urllib

urllib.request.urlretrieve(f"{url_tr}?{api_key}", "/Volumes/catalog_name/schema_name/sample_raw/sample_tr.csv")
urllib.request.urlretrieve(f"{url_trn}?{api_key}", "/Volumes/catalog_name/schema_name/sample_raw/sample_trn.csv")
urllib.request.urlretrieve(f"{url_sta}?{api_key}", "/Volumes/catalog_name/schema_name/sample_raw/sample_sta.csv")
urllib.request.urlretrieve(f"{url_eods}?{api_key}", "/Volumes/catalog_name/schema_name/sample_raw/sample_eods.csv")



# CATALOGS, VOLUMES, SCHEMAS
# spark.sql("SHOW VOLUMES IN catalog_name.schema_name").show()
spark.sql("CREATE CATALOG IF NOT EXISTS catalog_name")
spark.sql("CREATE VOLUME IF NOT EXISTS catalog_name.schema_name.volume_name COMMENT 'optional comment'")
spark.sql("CREATE SCHEMA IF NOT EXISTS catalog_name.schema_name")



# From https://www.databricks.com/blog/2022/08/08/identity-columns-to-generate-surrogate-keys-are-now-available-in-a-lakehouse-near-you.html
# CREATE OR REPLACE TABLE demo (
#   id BIGINT GENERATED ALWAYS AS IDENTITY,
#   product_type STRING,
#   sales BIGINT
# );


## From 'C4_W3_Assignment_Spark.ipynb' file:
# import hashlib
# from typing import List
# from pyspark.sql.types import StringType, LongType
# from pyspark.sql.functions import col, udf, array

# @udf(returnType=StringType())
# def surrogateKey(text_values: List[str]):
#     sha256 = hashlib.sha256()
#     data = ''.join(text_values)
#     sha256.update(data.encode())
#     return sha256.hexdigest()