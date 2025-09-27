from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import LongType, IntegerType, StringType, StructType, StructField, DateType, BooleanType
import pyspark.sql.functions as F
import os

jdbc_url = os.environ.get("DB_HOST")
creds = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}
spark = SparkSession.builder.appName("Upload_test_data").getOrCreate()


def read_data(spark, file_path, schema):
    return spark.read \
    .option("header", "true") \
    .option("sep", ";") \
    .option("dateFormat", "dd.MM.yyyy") \
    .schema(schema) \
    .csv(file_path)


property_schema = StructType([
    StructField("id", LongType(), False),
    StructField("metric_id", LongType(), False),
    StructField("description", StringType(), False),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), True), # nullable = True
    StructField("is_exception", BooleanType(), False)
])

cluster_schema = StructType([
    StructField("property_id", LongType(), False),
    StructField("cluster_type_id", LongType(), False),
    StructField("value", LongType(), False),
    StructField("level", LongType(), False)
])

condition_schema = StructType([
    StructField("property_id", LongType(), False),
    StructField("unit_id", LongType(), False),
    StructField("operator", StringType(), False),
    StructField("value", StringType(), True)
])

item_location_schema = StructType([
    StructField("item_id", LongType(), False),
    StructField("location_id", LongType(), False),
    StructField("begin_dt", DateType(), False),
    StructField("end_dt", DateType(), False)
])

promo_item_location_schema = StructType([
    StructField("promo_id", LongType(), False),
    StructField("item_id", LongType(), False),
    StructField("location_id", LongType(), False),
    StructField("begin_dt", DateType(), False),
    StructField("end_dt", DateType(), False)
])

supplier_item_location_schema = StructType([
    StructField("supplier_id", LongType(), False),
    StructField("item_id", LongType(), False),
    StructField("location_id", LongType(), False),
    StructField("begin_dt", DateType(), False),
    StructField("end_dt", DateType(), False)
])

supplier_location_schema = StructType([
    StructField("supplier_id", LongType(), False),
    StructField("location_id", LongType(), False),
    StructField("begin_dt", DateType(), False),
    StructField("end_dt", DateType(), False)
])

property_df = read_data(spark, "data/property.csv", property_schema)
property_df.show(5)
property_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.property", properties=creds)

cluster_df = read_data(spark, "data/cluster.csv", cluster_schema)
cluster_df.show(5)
cluster_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.cluster", properties=creds)

condition_df = read_data(spark, "data/condition.csv", condition_schema)
condition_df.show(5)
condition_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.condition", properties=creds)

item_location_df = read_data(spark, "data/item_location_period.csv", item_location_schema)
item_location_df.show(5)
item_location_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.item_location_period", properties=creds)

promo_item_location_df = read_data(spark, "data/promo_item_location_period.csv", promo_item_location_schema)
promo_item_location_df.show(5)
promo_item_location_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.promo_item_location_period", properties=creds)

supplier_item_location_df = read_data(spark, "data/supplier_item_location_period.csv", supplier_item_location_schema)
supplier_item_location_df.show(5)
supplier_item_location_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.supplier_item_location_period", properties=creds)

supplier_location_df = read_data(spark, "data/supplier_location_period.csv", supplier_location_schema)
supplier_location_df.show(5)
supplier_location_df.write.jdbc(url=jdbc_url, mode="overwrite", table="params.supplier_location_period", properties=creds)

