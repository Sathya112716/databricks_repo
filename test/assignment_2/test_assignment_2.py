# Databricks notebook source
import logging
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType
from pyspark.sql.functions import current_date, col, explode, split
from pyspark.sql import SparkSession


# COMMAND ----------


class JsonProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Json Processor").getOrCreate()

    def process_json_data(self, json_data, custom_schema):
        df = self.spark.createDataFrame([json_data], custom_schema)
        
        # Explode 'data' array column
        df = df.withColumn("data", explode("data"))
        
        # Flatten nested data structure
        df = df.withColumn("id", col("data.id")) \
               .withColumn("email", col("data.email")) \
               .withColumn("first_name", col("data.first_name")) \
               .withColumn("last_name", col("data.last_name")) \
               .withColumn("avatar", col("data.avatar")) \
               .drop("data")
        
        # Derive 'site_address' column from 'email' column
        df = df.withColumn("site_address", split(col("email"), "@")[1])
        
        # Add 'load_date' column with current date
        df = df.withColumn("load_date", current_date())
        
        return df




# COMMAND ----------



# Set up logging
logging.basicConfig(level=logging.INFO)

# Define the unit test
def test_process_json_data():
    # Sample JSON data for testing
    sample_json_data = {
        "page": 2,
        "per_page": 3,
        "total": 12,
        "total_pages": 4,
        "data": [
            {"id": 4, "email": "eve.holt@reqres.in", "first_name": "Eve", "last_name": "Holt", "avatar": "https://s3.amazonaws.com/uifaces/faces/twitter/marcoramires/128.jpg"},
            {"id": 5, "email": "charles.morris@reqres.in", "first_name": "Charles", "last_name": "Morris", "avatar": "https://s3.amazonaws.com/uifaces/faces/twitter/stephenmoon/128.jpg"},
            {"id": 6, "email": "tracey.ramos@reqres.in", "first_name": "Tracey", "last_name": "Ramos", "avatar": "https://s3.amazonaws.com/uifaces/faces/twitter/bigmancho/128.jpg"}
        ],
        "support": {"url": "https://reqres.in/#support-heading", "text": "To keep ReqRes free, contributions towards server costs are appreciated!"}
    }

    # Define the expected schema
    data_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("avatar", StringType(), True)
    ])

    custom_schema = StructType([
        StructField("page", IntegerType(), True),
        StructField("per_page", IntegerType(), True),
        StructField("total", IntegerType(), True),
        StructField("total_pages", IntegerType(), True),
        StructField("data", ArrayType(data_schema), True),
        StructField("support", MapType(StringType(), StringType()), True)
    ])

    # Execute the function with the sample data
    processor = JsonProcessor()
    processed_df = processor.process_json_data(sample_json_data, custom_schema)

    # Perform assertions
    assert processed_df.count() == 3  # Check if the DataFrame has the expected number of rows
    assert "site_address" in processed_df.columns  # Check if the 'site_address' column is present

    # Additional assertions can be added based on your specific requirements
    logging.info("Unit test for process_json_data passed")
