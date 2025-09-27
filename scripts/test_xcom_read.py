import os
from pyspark.sql import SparkSession
import logging
import sys


logger = logging.getLogger(__name__)

creds = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

jdbc_url = os.environ.get("DB_HOST")

spark = SparkSession.builder.appName("test_xcom_read").getOrCreate()

raw_data = os.environ.get("METRIC")
print(f"Type of raw data from XCOM {type(raw_data)}")
print(f"raw data {raw_data}")
#data = ast.literal_eval(str(raw_data))
columns = ["id", "name", "domain", "scope_table_name", "is_exception_allowed"]
#df = spark.createDataFrame(data, schema=columns)
#logger.info(f"Number of rows in DataFrame: {df.count()}")
print("Output Spark DataFrame:")
#df.show(truncate=False)

spark.stop()
