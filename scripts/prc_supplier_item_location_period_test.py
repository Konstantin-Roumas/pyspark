from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import os
import logging
from jdbs_db_utils import write_jdbc_data, read_jdbc_data
from spark_utils import create_spark_session
import reversal_processor as rp

logger = logging.getLogger(__name__)

# --- Specific Configuration ---
SCOPE_PK = ["location_id", "supplier_id"]
RESULT_TABLE = 'result_supplier_location_period_spark'
NORM_SCOPE_TABLE = "supplier_location_period_norm"
ORIGINAL_SCOPE_TABLE = "supplier_location_period"

def extract_data_for_procedure(spark: SparkSession) -> dict:
    dfs = {
        "property_norm": read_jdbc_data("property_norm", spark),
        "cluster_type": read_jdbc_data("cluster_type", spark),
        "metric_cluster_link": read_jdbc_data("metric_cluster_link", spark),
        NORM_SCOPE_TABLE: read_jdbc_data(NORM_SCOPE_TABLE, spark),
        ORIGINAL_SCOPE_TABLE: read_jdbc_data(ORIGINAL_SCOPE_TABLE, spark),
        "condition": read_jdbc_data("condition", spark),
        "unit": read_jdbc_data("unit", spark)
    }
    return dfs

def _prop_clusters_for_supplier_loc(prop_df: DataFrame) -> DataFrame:
    base_cols = ["property_id", "metric_id"]
    out = None
    def add(col, code, level):
        nonlocal out
        df = prop_df.select(*base_cols).where(F.col(col).isNotNull()) \
            .withColumn("cluster_type_code", F.lit(code)) \
            .withColumn("cluster_type_level", F.lit(level).cast("int"))
        out = df if out is None else out.unionByName(df)

    # Store format
    add("store_format_level_0_uid", "LOCATION_FORMAT", 1)
    add("store_format_level_1_uid", "LOCATION_FORMAT", 2)

    # Delivery group
    add("delivery_group_uid", "DELIVERY_GROUP", 1)

    return out if out is not None else prop_df.select(*base_cols).limit(0) \
        .withColumn("cluster_type_code", F.lit(None).cast("string")) \
        .withColumn("cluster_type_level", F.lit(None).cast("int"))

def _mcl_priority_by_property(prop_df: DataFrame, ct_df: DataFrame, mcl_df: DataFrame, metric_id: int) -> DataFrame:
    clusters = _prop_clusters_for_supplier_loc(prop_df.filter(F.col("metric_id") == metric_id))
    if clusters.rdd.isEmpty():
        return prop_df.filter(F.col("metric_id") == metric_id) \
            .select("property_id").withColumn("mcl_order_number", F.lit(None).cast("int"))

    ct = ct_df.select(F.col("id").alias("cluster_type_id"), F.col("code").alias("cluster_type_code"))
    mcl = mcl_df.select("metric_id", "cluster_type_id", "cluster_type_level", "order_number")

    pri = clusters.join(ct, "cluster_type_code", "left") \
        .join(mcl,
              (mcl.metric_id == F.lit(metric_id)) &
              (mcl.cluster_type_id == F.col("cluster_type_id")) &
              (mcl.cluster_type_level == F.col("cluster_type_level")),
              "left") \
        .groupBy("property_id") \
        .agg(F.min("order_number").alias("mcl_order_number"))
    return pri

def find_intersections(
        supplier_location_period_norm_df: DataFrame,
        property_norm_df: DataFrame,
        cluster_type_df: DataFrame,
        mcl_df: DataFrame,
        metric_id: int
) -> DataFrame:
    scope = supplier_location_period_norm_df.alias("scope")
    prop = property_norm_df.alias("prop")

    join_condition = (
        (F.col("prop.metric_id") == metric_id) &
        (F.col("prop.start_date") <= F.col("scope.end_dt")) &
        (F.col("prop.end_date") >= F.col("scope.begin_dt"))
    )

    cluster_cols = [
        "store_format_level_0_uid", "store_format_level_1_uid",
        "delivery_group_uid"
    ]

    for col_name in cluster_cols:
        join_condition = join_condition & (
            F.coalesce(F.col(f"prop.{col_name}"), F.col(f"scope.{col_name}"), F.lit(0)) ==
            F.coalesce(F.col(f"scope.{col_name}"), F.lit(0))
        )

    pri = _mcl_priority_by_property(prop, cluster_type_df, mcl_df, metric_id)

    intersections_df = scope.join(prop, join_condition, "inner") \
        .join(pri, F.col("prop.property_id") == F.col("pri.property_id"), "left") \
        .select(
        F.col("scope.location_id"),
        F.col("scope.supplier_id"),
        F.greatest(F.col("prop.start_date"), F.col("scope.begin_dt")).alias("begin_dt"),
        F.least(F.col("prop.end_date"), F.col("scope.end_dt")).alias("end_dt"),
        F.col("prop.property_id"),
        F.col("prop.is_exception"),
        F.col("prop.order_number"),
        F.col("pri.mcl_order_number")
    )

    if intersections_df.filter(F.col("mcl_order_number").isNull()).head(1):
        raise ValueError("Нет приоритета в metric_cluster_link для части свойств (metric/cluster_type/level). Заполните MCL.")
    return intersections_df

def main(current_metric_id: int):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("INFO: Step 1: Spark session created.")

    p_root_calc_id = rp.create_root_calc_id(spark, "reversal")
    print(f"INFO: Step 2: Root calculation id created: {p_root_calc_id}")

    dfs = extract_data_for_procedure(spark)

    intersections_df = find_intersections(
        supplier_location_period_norm_df=dfs[NORM_SCOPE_TABLE],
        property_norm_df=dfs["property_norm"],
        cluster_type_df=dfs["cluster_type"],
        mcl_df=dfs["metric_cluster_link"],
        metric_id=current_metric_id
    )
    print("INFO: Step 3: Found intersections with computed MCL priorities.")

    final_periods_df = rp.calculate_final_periods(intersections_df, SCOPE_PK)
    print("INFO: Step 4: Calculated final periods after slicing and gluing.")

    gaps_df = rp.calculate_gaps(final_periods_df, dfs[ORIGINAL_SCOPE_TABLE], SCOPE_PK)
    print("INFO: Step 5: Found 'gaps' (periods without property).")

    unmatched_df = rp.find_unmatched_scopes(final_periods_df, dfs[ORIGINAL_SCOPE_TABLE], SCOPE_PK)
    print("INFO: Step 6: Found unmatched scopes.")

    final_result_df = rp.assemble_final_result(
        final_periods_df=final_periods_df,
        gaps_df=gaps_df,
        unmatched_df=unmatched_df,
        condition_df=dfs["condition"],
        unit_df=dfs["unit"],
        metric_id=current_metric_id,
        p_root_calc_id=p_root_calc_id,
        scope_pk_cols=SCOPE_PK
    )
    print(f"INFO: Step 7: Writing final result to {RESULT_TABLE}...")
    write_jdbc_data(final_result_df, RESULT_TABLE)
    print("INFO: Process completed successfully.")
    spark.stop()

if __name__ == "__main__":
    metric_id_env = os.environ.get("METRIC_ID")
    if metric_id_env:
        main(int(metric_id_env))
    else:
        print("ERROR: METRIC_ID environment variable not set.")

