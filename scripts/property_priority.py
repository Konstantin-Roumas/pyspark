import os
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from jdbs_db_utils import write_jdbc_data,read_jdbc_data
from spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


#working tables
output_priority_table= "property_priority"
cluster_types_set_table='cluster_types_set'
priority_table='priority'
property_table='property'
cluster_table='cluster'


def _simulate_recursive_cte(base_df: DataFrame, recursive_df: DataFrame) -> DataFrame:

    final_results_df = base_df
    iteration_df = base_df
    recursive_df.cache()
    logger.info("Starting recursive CTE simulation...")

    while True:
        next_iteration_df = iteration_df.alias("c").join(
            recursive_df.alias("s"),
            F.col("c.cluster_types_set_id") == F.col("s.id")
        ).select(
            F.col("s.id"),
            F.col("s.cluster_type_id"),
            F.col("s.cluster_type_level"),
            F.col("s.cluster_types_set_id"),
            F.col("c.root")
        )

        new_rows_df = next_iteration_df.exceptAll(final_results_df).cache()

        if new_rows_df.isEmpty():
            logger.info("Recursion finished. No new rows found.")
            break

        final_results_df = final_results_df.unionByName(new_rows_df)
        iteration_df = new_rows_df

    recursive_df.unpersist()
    return final_results_df

def _create_cluster_fingerprint(df: DataFrame, group_by_cols: list, id_col: str, level_col: str) -> DataFrame:
    logger.info(f"Generating fingerprints by grouping on: {group_by_cols}")
    
    agg_df = df.groupBy(*group_by_cols).agg(
        F.collect_list(F.col(id_col).cast("string")).alias("id_array"),
        F.collect_list(F.col(level_col).cast("string")).alias("level_array")
    )

    fingerprint_df = agg_df.withColumn(
        "sorted_structs",
        F.array_sort(F.arrays_zip(F.col("id_array"), F.col("level_array")))
    ).withColumn(
        "ct_lvl",
        F.expr("transform(sorted_structs, s -> array(s.id_array, s.level_array))")
    )

    return fingerprint_df.select(*group_by_cols, "ct_lvl")


def main():
    spark = create_spark_session()



    logger.info("Reading source tables from database...")
    cluster_set_df = read_jdbc_data(spark=spark, table_name=cluster_types_set_table)
    priority_df = read_jdbc_data(spark=spark, table_name= priority_table)
    property_df = read_jdbc_data(spark=spark, table_name= property_table)
    cluster_df = read_jdbc_data(spark=spark, table_name=cluster_table)

    priority_start_ids_df = priority_df.select("cluster_types_set_id").distinct()
    
    anchor_df = cluster_set_df.join(
        priority_start_ids_df,
        cluster_set_df.id == priority_start_ids_df.cluster_types_set_id,
        "semi"
    ).withColumn("root", F.col("id"))

    cte_results_df = _simulate_recursive_cte(anchor_df, cluster_set_df)

    arr_cl_set_df = _create_cluster_fingerprint(
        df=cte_results_df,
        group_by_cols=["root"],
        id_col="cluster_type_id",
        level_col="cluster_type_level"
    )

    arr_prior_df = arr_cl_set_df.alias("ac").join(
        priority_df.alias("pr"),
        F.col("ac.root") == F.col("pr.cluster_types_set_id")
    ).select("ac.ct_lvl", "pr.metric_id", "pr.order_number")
    
    logger.info("Successfully generated priority fingerprints.")

    prop_clusters_df = property_df.alias("p").join(
        cluster_df.alias("c"),
        F.col("c.property_id") == F.col("p.id")
    ).where(F.col("p.is_exception") == False)

    arr_prop_df = _create_cluster_fingerprint(
        df=prop_clusters_df,
        group_by_cols=["property_id", "metric_id"],
        id_col="cluster_type_id",
        level_col="level"
    )
    logger.info("Successfully generated property fingerprints.")

    logger.info("Joining priorities and properties on fingerprints...")
    final_df = arr_prior_df.alias("p").join(
        arr_prop_df.alias("pr"),
        (F.col("p.ct_lvl") == F.col("pr.ct_lvl")) & (F.col("p.metric_id") == F.col("pr.metric_id"))
    ).select(
        F.col("pr.property_id"),
        F.col("p.order_number")
    )

    write_jdbc_data(dataframe=final_df,table_name= output_priority_table,mode='overwrite')
    logger.info("Process completed successfully.")


if __name__ == "__main__":
    main()