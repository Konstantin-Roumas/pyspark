import logging
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from jdbs_db_utils import write_jdbc_data,read_jdbc_data
from spark_utils import create_spark_session

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


DELIVERY_LOCATION_ID = -5042515683615240694
#working tables
promo_item_location_period_norm='promo_item_location_period_norm'
item_location_period_norm='item_location_period_norm'
supplier_item_location_period_norm='supplier_item_location_period_norm'
supplier_location_period_norm='supplier_location_period_norm'
overwrite_mode="overwrite"


def _simulate_recursive_cte(base_df: DataFrame, recursive_df: DataFrame) -> DataFrame:
    final_results_df = base_df
    iteration_df = base_df
    recursive_df.cache()
    logger.info("Starting recursive CTE simulation...")
    iteration_count = 0
    while True:
        iteration_count += 1
        next_iteration_df = iteration_df.alias("c").join(
            recursive_df.alias("s"),
            F.col("c.cluster_types_set_id") == F.col("s.id")
        ).select(
            "s.id", "s.cluster_type_id", "s.cluster_type_level",
            "s.cluster_types_set_id", "c.root"
        )
        
        new_rows_df = next_iteration_df.join(final_results_df, on="id", how="left_anti").cache()

        if new_rows_df.isEmpty():
            logger.info(f"Recursion finished after {iteration_count} iterations. No new rows found.")
            new_rows_df.unpersist()
            break

        final_results_df = final_results_df.unionByName(new_rows_df)
        iteration_df = new_rows_df

    recursive_df.unpersist()
    return final_results_df


def _create_normalized_branches(
    common_df: DataFrame,
    tables: Dict[str, DataFrame],
    store_cols: List[str],
    delivery_cols: List[str],
    delivery_join_expr=None
) -> DataFrame:
    store_branch = common_df.join(
        tables["store"].alias("s"), F.col("s.store_uid") == F.col("sc.location_id")
    ).selectExpr(*store_cols)

    delivery_base = common_df.filter(F.col("sc.location_id") == DELIVERY_LOCATION_ID)
    
    if delivery_join_expr is None:
        delivery_join_expr = (F.col("dg.destination_location_uid") == F.col("sc.location_id"))
        
    delivery_branch = delivery_base.join(
        tables["delivery_group"].alias("dg"), delivery_join_expr
    ).join(
        tables["delivery_group_assortment"].alias("dga"),
        (F.col("dga.delivery_group_uid") == F.col("dg.delivery_group_uid")) & (F.col("dga.item_uid") == F.col("sc.item_id"))
    ).selectExpr(*delivery_cols)
    
    return store_branch.unionByName(delivery_branch)

def normalize_data_tables(spark: SparkSession, tables: Dict[str, DataFrame]):
    logger.info("## Starting Stage 2: Normalize Data Tables ##")

    logger.info("Normalizing promo_item_location_period...")
    promo_common_df = tables["promo_item_location_period"].alias("sc").join(
        tables["item"].alias("i"), F.col("i.item_uid") == F.col("sc.item_id")
    ).join(
        tables["item_expiration_period"].alias("ie"),
        F.col("i.total_shelf_life_day_qnty").between(F.col("ie.value_min"), F.col("ie.value_max"))
    ).join(
        tables["item_promo"].alias("ip"), F.col("ip.item_promo_uid") == F.col("sc.promo_id")
    ).join(
        tables["promo_category_hierarchy"].alias("ph"), F.col("ph.promo_category_uid") == F.col("ip.promo_category_uid")
    ).join(
        tables["promo_period"].alias("pd"),
        (F.datediff(F.col("ip.promo_end_dt"), F.col("ip.promo_begin_dt")) + 1).between(F.col("pd.value_min"), F.col("pd.value_max"))
    ).cache()
    
    promo_store_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "pd.promo_period_uid", "ph.promo_category_level_0_uid", "ph.promo_category_level_1_uid", "ph.promo_category_level_2_uid", "s.store_format_level_0_uid", "s.store_format_level_1_uid", "CAST(NULL AS LONG) as delivery_group_uid"]
    promo_delivery_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "pd.promo_period_uid", "ph.promo_category_level_0_uid", "ph.promo_category_level_1_uid", "ph.promo_category_level_2_uid", "CAST(NULL AS LONG) as store_format_level_0_uid", "CAST(NULL AS LONG) as store_format_level_1_uid", "dga.delivery_group_uid"]
    
    promo_norm_df = _create_normalized_branches(promo_common_df, tables, promo_store_cols, promo_delivery_cols)
    write_jdbc_data(promo_norm_df, promo_item_location_period_norm,mode=overwrite_mode)
    promo_common_df.unpersist()

    logger.info("Normalizing item_location_period...")
    item_common_df = tables["item_location_period"].alias("sc").join(
        tables["item"].alias("i"), F.col("i.item_uid") == F.col("sc.item_id")
    ).join(
        tables["item_expiration_period"].alias("ie"),
        F.col("i.total_shelf_life_day_qnty").between(F.col("ie.value_min"), F.col("ie.value_max"))
    ).cache()

    item_store_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "s.store_format_level_0_uid", "s.store_format_level_1_uid", "CAST(NULL AS LONG) as delivery_group_uid"]
    item_delivery_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "CAST(NULL AS LONG) as store_format_level_0_uid", "CAST(NULL AS LONG) as store_format_level_1_uid", "dga.delivery_group_uid"]

    item_norm_df = _create_normalized_branches(item_common_df, tables, item_store_cols, item_delivery_cols)
    write_jdbc_data(item_norm_df, item_location_period_norm,mode=overwrite_mode)
    item_common_df.unpersist()

    logger.info("Normalizing supplier_item_location_period...")
    supplier_item_common_df = tables["supplier_item_location_period"].alias("sc").join(
        tables["item"].alias("i"), F.col("i.item_uid") == F.col("sc.item_id")
    ).join(
        tables["item_expiration_period"].alias("ie"),
        F.col("i.total_shelf_life_day_qnty").between(F.col("ie.value_min"), F.col("ie.value_max"))
    ).cache()
    
    supplier_item_store_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "s.store_format_level_0_uid", "s.store_format_level_1_uid", "CAST(NULL AS LONG) as delivery_group_uid"]
    supplier_item_delivery_cols = ["sc.*", "i.item_category_level_0_uid", "i.item_category_level_1_uid", "i.item_category_level_2_uid", "i.item_category_level_3_uid", "i.item_category_level_4_uid", "ie.item_expiration_period_uid", "CAST(NULL AS LONG) as store_format_level_0_uid", "CAST(NULL AS LONG) as store_format_level_1_uid", "dga.delivery_group_uid"]
    supplier_delivery_join = (F.col("dg.destination_location_uid") == F.col("sc.location_id")) & (F.col("dg.source_location_uid") == F.col("sc.supplier_id"))
    
    supplier_item_norm_df = _create_normalized_branches(supplier_item_common_df, tables, supplier_item_store_cols, supplier_item_delivery_cols, supplier_delivery_join)
    write_jdbc_data(supplier_item_norm_df, supplier_item_location_period_norm,mode=overwrite_mode)
    supplier_item_common_df.unpersist()

    logger.info("Normalizing supplier_location_period...")
    slp_df = tables["supplier_location_period"]
    slp_store = slp_df.alias("sc").join(
        tables["store"].alias("s"), F.col("s.store_uid") == F.col("sc.location_id")
    ).selectExpr("sc.*", "s.store_format_level_0_uid", "s.store_format_level_1_uid", "CAST(NULL AS LONG) as delivery_group_uid")
    
    slp_delivery = slp_df.alias("sc").filter(F.col("sc.location_id") == DELIVERY_LOCATION_ID).join(
        tables["delivery_group"].alias("dg"),
        (F.col("dg.destination_location_uid") == F.col("sc.location_id")) & (F.col("dg.source_location_uid") == F.col("sc.supplier_id"))
    ).selectExpr("sc.*", "CAST(NULL AS LONG) as store_format_level_0_uid", "CAST(NULL AS LONG) as store_format_level_1_uid", "dg.delivery_group_uid")
    
    write_jdbc_data(slp_store.unionByName(slp_delivery), supplier_location_period_norm,mode=overwrite_mode)

    
    logger.info("## Finished Stage 3 ##")

def main():

    spark = None
    try:
        spark = create_spark_session()
        logger.info("Data processing pipeline started.")
        
        table_names = [
            "cluster_types_set", "priority", "property", "cluster",
            "promo_item_location_period", "item", "item_expiration_period",
            "item_promo", "promo_category_hierarchy", "promo_period", "store",
            "delivery_group", "delivery_group_assortment", "item_location_period",
            "supplier_item_location_period", "supplier_location_period"
        ]
        tables = {name: read_jdbc_data(spark=spark, table_name=name) for name in table_names}
        
        normalize_data_tables(spark, tables)
        
        logger.info("Data processing pipeline completed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred during the pipeline execution: {e}", exc_info=True)
        
    finally:
        if spark:
            logger.info("Stopping Spark session.")
            spark.stop()

if __name__ == "__main__":
    main()
