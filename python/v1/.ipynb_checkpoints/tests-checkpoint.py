# Adds the parent directory of the current working directory to the Python search path
import sys
import os

# Get the current working directory
current_path = os.getcwd()

# Get the root folder path by navigating two levels up from the current path
root_folder_path = os.path.dirname(os.path.dirname(current_path))

# Add the toot folder path to the system path (this allows you to import modules from this folder)
sys.path.append(root_folder_path)

## Import the custom functions
from 1_bronze_STREAM_pipeline import *



## Unit Test Example

from pyspark.sql.functions import col, when
from pyspark.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, LongType
from pyspark.testing.utils import assertSchemaEqual, assertDataFrameEqual


# Function you want to test
def add_new_col(df, new, s_col):
    return (df
            .withColumn(new, when(col(s_col) == 0, "Normal")
            .otherwise("Unknown")))

# Unit test goal
def test_add_new_col():
    data = [(0,), (1,), (-1,), (None,)]
    columns = ["value"]
    df = spark.createDataFrame(data, columns)

    actual_df = add_new_col(df, "new_value", "value")

    expected_data = [(0, "Normal"), (1, "Unknown"),
                     (-1, "Unknown"), (None, "Unknown")]

    expected_df = spark.createDataFrame(expected_data, ["value", "new_value"])

    assertDataFrameEqual(actual_df, expected_df)



# Create the first test function 'test_tr_csv_schema_match' to test the 'tr_csv_schema'
def test_tr_csv_schema_match():

    # Get schema from our function
    actual_schema = 1_bronze_STREAM_pipeline.tr_csv_schema

    # Define the expected schema that the function should return. If that function is changed during development the unit test will pick up the error and the test will fail.
    expected_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True)
    ])

    # Assert the actual schema matches the expected schema
    assertSchemaEqual(actual_schema, expected_schema)
    print('Test "test_tr_csv_schema_match" passed!')

test_tr_csv_schema_match()