import logging
from functools import reduce
from pyspark.sql import  DataFrame
import pyspark.sql.functions as F
from jdbs_db_utils import write_jdbc_data,read_jdbc_data
from spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


CLUSTER_CONFIG = {
    1: {"type": "pivot", "levels": [0, 1, 2, 3, 4], "prefix": "item_category_level"},
    2: {"type": "simple", "alias": "item_expiration_period_uid"},
    3: {"type": "simple", "alias": "promo_period_uid"},
    4: {"type": "pivot", "levels": [0, 1, 2], "prefix": "promo_category_level"},
    5: {"type": "pivot", "levels": [0, 1], "prefix": "store_format_level"},
    6: {"type": "simple", "alias": "delivery_group_uid"},
}
#working tables
property_norm_table = 'property_norm'
property_priority_table= 'property_priority'
property_table='property'
cluster_table='cluster'

def _process_cluster_data(cluster_df: DataFrame, config: dict):
    processed_dfs = []
    for cluster_id, params in config.items():
        logger.info(f"Processing cluster_type_id = {cluster_id}...")
        
        df = cluster_df.filter(F.col("cluster_type_id") == cluster_id)

        if params["type"] == "pivot":
            pivoted_df = df.groupBy("property_id") \
                .pivot("level", params["levels"]) \
                .agg(F.first("value"))
            
            for level in params["levels"]:
                pivoted_df = pivoted_df.withColumnRenamed(
                    str(level), f"{params['prefix']}_{level}_uid"
                )
            processed_dfs.append(pivoted_df)

        elif params["type"] == "simple":
            simple_df = df.select("property_id", F.col("value").alias(params["alias"]))
            processed_dfs.append(simple_df)
            
    return processed_dfs

def main():
    spark = create_spark_session()





    logger.info("Reading source tables from database...")
    property_df = read_jdbc_data(spark=spark,table_name= property_table)
    cluster_df = read_jdbc_data(spark=spark,table_name= cluster_table)
    property_priority_df = read_jdbc_data(spark=spark, table_name= property_priority_table)

    processed_cluster_dfs = _process_cluster_data(cluster_df, CLUSTER_CONFIG)

    logger.info("Joining all processed DataFrames...")
    base_df = property_df.alias("p").join(
        property_priority_df.alias("pp"),
        F.col("p.id") == F.col("pp.property_id"),
        "left"
    )
    
    final_df = reduce(
        lambda df, next_df: df.join(next_df, F.col("p.id") == next_df.property_id, "left"),
        processed_cluster_dfs,
        base_df
    )
    
    logger.info("Building final select expression and casting types...")
    
    new_uid_columns = []
    for params in CLUSTER_CONFIG.values():
        if params["type"] == "pivot":
            for level in params["levels"]:
                new_uid_columns.append(f"{params['prefix']}_{level}_uid")
        else:
            new_uid_columns.append(params["alias"])

    select_exprs = [
        F.col("p.id").alias("property_id"),
        "p.metric_id",
        "p.start_date",
        "p.end_date",
        "p.is_exception",
        F.when(F.col("p.is_exception"), 0).otherwise(F.col("pp.order_number")).alias("order_number"),
    ]
    
    for col_name in new_uid_columns:
        select_exprs.append(F.col(col_name).cast("long").alias(col_name))

    output_df = final_df.select(*select_exprs)

    write_jdbc_data(dataframe=output_df,table_name= property_norm_table,mode='overwrite')
    logger.info("Process completed successfully.")


if __name__ == "__main__":
    main()
