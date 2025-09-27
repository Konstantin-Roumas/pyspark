"""Utilities for priority calculation based on metric cluster linkage."""
import logging
from typing import Iterable, List, Optional, Sequence

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

logger = logging.getLogger(__name__)

# Mapping definitions between cluster metadata and property_norm columns.
#: Cluster types that expand into multiple level-specific columns.
PIVOT_CLUSTER_PREFIX = {
    1: ("item_category_level", (0, 1, 2, 3, 4)),
    4: ("promo_category_level", (0, 1, 2)),
    5: ("store_format_level", (0, 1)),
}

#: Cluster types represented by a single column (no level dimension).
SIMPLE_CLUSTER_COLUMNS = {
    2: "item_expiration_period_uid",
    3: "promo_period_uid",
    6: "delivery_group_uid",
    7: "selection_type_value",
    8: "promo_inout_value",
}


def _pivot_column_name(cluster_type_id: int, level: Optional[int]) -> Optional[str]:
    prefix_info = PIVOT_CLUSTER_PREFIX.get(cluster_type_id)
    if not prefix_info:
        return None

    prefix, valid_levels = prefix_info
    if level is None:
        logger.warning(
            "Cluster type %s expects a level but received None; column skipped.",
            cluster_type_id,
        )
        return None

    if level not in valid_levels:
        logger.warning(
            "Cluster type %s received unsupported level %s; column skipped.",
            cluster_type_id,
            level,
        )
        return None

    return f"{prefix}_{level}_uid"


def resolve_cluster_column(cluster_type_id: int, cluster_type_level: Optional[int]) -> Optional[str]:
    """Return the property_norm column that corresponds to the provided cluster metadata."""
    if cluster_type_id in SIMPLE_CLUSTER_COLUMNS:
        return SIMPLE_CLUSTER_COLUMNS[cluster_type_id]

    column_name = _pivot_column_name(cluster_type_id, cluster_type_level)
    if not column_name:
        logger.warning(
            "No property_norm column mapping found for cluster_type_id=%s, cluster_type_level=%s.",
            cluster_type_id,
            cluster_type_level,
        )
    return column_name


def resolve_priority_columns(cluster_links: Sequence) -> List[str]:
    """Map metric_cluster_link rows to ordered property_norm columns."""
    ordered_columns: List[str] = []
    seen = set()

    for link in cluster_links:
        cluster_type_id = getattr(link, "cluster_type_id", None)
        if cluster_type_id is None:
            logger.warning("metric_cluster_link row missing cluster_type_id: %s", link)
            continue
        level = getattr(link, "cluster_type_level", None)
        column_name = resolve_cluster_column(cluster_type_id, level)
        if not column_name:
            continue
        if column_name in seen:
            continue
        ordered_columns.append(column_name)
        seen.add(column_name)

    return ordered_columns


def required_priority_columns() -> List[str]:
    """Return all property_norm columns that can participate in prioritisation."""
    columns: List[str] = list(SIMPLE_CLUSTER_COLUMNS.values())
    for prefix, levels in PIVOT_CLUSTER_PREFIX.values():
        columns.extend(f"{prefix}_{lvl}_uid" for lvl in levels)
    return columns


def attach_priority_info(
    dataframe: DataFrame,
    *,
    partition_cols: Sequence[str],
    priority_columns: Sequence[str],
    array_col_name: str = "priority_array",
    rank_col_name: str = "order_number",
) -> DataFrame:
    """Inject priority metadata (array and rank) into the provided DataFrame."""
    if not partition_cols:
        raise ValueError("partition_cols cannot be empty for priority calculation")

    df = dataframe
    if rank_col_name in df.columns:
        df = df.drop(rank_col_name)
    if array_col_name in df.columns:
        df = df.drop(array_col_name)

    available_priority_columns = [
        column for column in priority_columns if column in df.columns
    ]

    partition_window = Window.partitionBy(*partition_cols)

    component_cols: List[str] = []
    for idx, column in enumerate(available_priority_columns):
        presence_col = f"__prio_presence_{idx}"
        component_col = f"__prio_component_{idx}"

        df = df.withColumn(
            presence_col,
            F.when(F.col(column).isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )

        df = df.withColumn(
            component_col,
            F.when(
                F.max(F.col(presence_col)).over(partition_window)
                != F.min(F.col(presence_col)).over(partition_window),
                F.lit(1) - F.col(presence_col),
            ).otherwise(F.lit(0)),
        ).drop(presence_col)

        component_cols.append(component_col)

    if component_cols:
        df = df.withColumn(
            array_col_name,
            F.array(*[F.col(col) for col in component_cols]),
        )
    else:
        df = df.withColumn(array_col_name, F.array(F.lit(0)))

    order_expressions = [F.col("is_exception").desc(), F.col(array_col_name), F.col("property_id")]
    rank_window = Window.partitionBy(*partition_cols).orderBy(*order_expressions)
    df = df.withColumn(rank_col_name, F.dense_rank().over(rank_window))

    if component_cols:
        df = df.drop(*component_cols)

    return df


def filter_by_selection_type(
    dataframe: DataFrame,
    *,
    item_df: Optional[DataFrame],
    item_id_col: str = "item_id",
    value_col: str = "selection_type_value",
    item_key_col: str = "item_uid",
    item_type_col: str = "item_type_id",
) -> DataFrame:
    """Filter rows by SELECTION_TYPE cluster rule."""
    if value_col not in dataframe.columns:
        return dataframe
    if item_df is None or item_id_col not in dataframe.columns:
        return dataframe

    lookup_df = item_df.select(
        F.col(item_key_col).alias("__selection_item_uid"),
        F.col(item_type_col).alias("__selection_item_type"),
    )

    df = dataframe.join(
        lookup_df,
        F.col(item_id_col) == F.col("__selection_item_uid"),
        "left",
    )

    df = df.filter(
        (F.col(value_col).isNull())
        | (F.col(item_id_col).isNull())
        | (
            F.col("__selection_item_type").isNotNull()
            & (F.col("__selection_item_type") == F.col(value_col))
        )
    )

    return df.drop("__selection_item_uid", "__selection_item_type")


def filter_by_promo_inout(
    dataframe: DataFrame,
    *,
    item_promo_df: Optional[DataFrame],
    promo_id_col: str = "promo_id",
    value_col: str = "promo_inout_value",
    promo_key_col: str = "item_promo_uid",
    inout_col: str = "warehouse_inout_mark",
) -> DataFrame:
    """Filter rows by PROMO_INOUT cluster rule."""
    if value_col not in dataframe.columns:
        return dataframe
    if item_promo_df is None or promo_id_col not in dataframe.columns:
        return dataframe

    lookup_df = item_promo_df.select(
        F.col(promo_key_col).alias("__promo_item_promo_uid"),
        F.col(inout_col).alias("__promo_inout_mark"),
    )

    df = dataframe.join(
        lookup_df,
        F.col(promo_id_col) == F.col("__promo_item_promo_uid"),
        "left",
    )

    df = df.filter(
        (F.col(value_col).isNull())
        | (F.col(promo_id_col).isNull())
        | (
            F.col("__promo_inout_mark").isNotNull()
            & (F.col("__promo_inout_mark").cast("int") == F.col(value_col).cast("int"))
        )
    )

    return df.drop("__promo_item_promo_uid", "__promo_inout_mark")
