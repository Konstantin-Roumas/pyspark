import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import logging


logger = logging.getLogger(__name__)

creds = {
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}
jdbc_url = os.environ.get("DB_HOST")
logger.info("=========================")
logger.info(creds)
logger.info(jdbc_url)
logger.info("=========================")
spark = SparkSession.builder.appName("test_con").getOrCreate()
sc = spark.sparkContext
log = sc._jvm.org.apache.log4j.LogManager.getLogger(__name__)

def read_data(table_name: str):
    return spark.read.jdbc(url=jdbc_url, table="params."+table_name, properties=creds)
def write_data(dataframe, table_name: str):
    return dataframe.write.jdbc(url=jdbc_url, mode="append", table="params."+table_name,properties=creds)
try:    
    property_df = read_data("property")
    property_df.show()
except Exception as e:
    log.error(f"Ошибка при работе с Iceberg: {str(e)}")
    raise

finally:
    spark.stop()