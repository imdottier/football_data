import yaml
from pyspark.sql.functions import (
    sum, avg, first, col, count, when, expr,
)
import pyspark.sql.functions as sf
import os

# Directory of the current script (e.g. /app/helpers/)
script_dir = os.path.dirname(os.path.abspath(__file__))

# Go up one level and point to config.yaml (e.g. /app/config.yaml)
config_path = os.path.abspath(os.path.join(script_dir, '..', 'configs', 'config.yaml'))

with open(config_path, 'r') as f:
    CONFIG = yaml.safe_load(f)

# Custom expressions use base expressions so have to split into 2 passes
def get_derived_expressions(layer: str, table_name: str):
    rules = CONFIG[layer][table_name]
    base_exprs = []
    custom_exprs = []

    # ---- Base flags (from type_display_name, period_display_name) ----
    if "derived_type_display_name" in rules:
        for col_name, values in rules["derived_type_display_name"].items():
            if isinstance(values, list):
                e = col("type_display_name").isin(values)
            else:
                e = col("type_display_name") == values
            base_exprs.append(e.alias(col_name))

    if "derived_period_display_name" in rules:
        for col_name, values in rules["derived_period_display_name"].items():
            if isinstance(values, list):
                e = col("period_display_name").isin(values)
            else:
                e = col("period_display_name") == values
            base_exprs.append(e.alias(col_name))

    # ---- Custom flags (depend on base flags) ----
    if "derived_custom_expressions" in rules:
        for alias, expression_str in rules["derived_custom_expressions"].items():
            custom_exprs.append(
                expr(f"CASE WHEN {expression_str} THEN true ELSE false END").alias(alias)
            )

    return base_exprs, custom_exprs



# build a list of aggs expression from config.yaml
# including sum, avg, first (non agg values)...
def get_expressions(layer: str, table_name: str):
    rules = CONFIG[layer][table_name]

    agg_expressions = []

    # Custom count, sql for simplicity
    if 'custom_expression_counts' in rules['aggregations']:
        for alias, expression_str in rules['aggregations']['custom_expression_counts'].items():
            agg_expressions.append(
                expr(f"COUNT(CASE WHEN {expression_str} THEN true END)").alias(alias)
            )

    # Sum/Avg on boolean or numeric flags
    if 'flags_agg' in rules['aggregations']:
        for col_name, fn in rules['aggregations']['flags_agg'].items():
            if fn == "sum":
                agg_expressions.append(sf.sum(col_name).alias(col_name))
            elif fn == "avg":
                agg_expressions.append(sf.round(sf.avg(col_name), 2).alias(col_name))

    # metadata
    if 'entity_meta' in rules['aggregations']:
        for col_name, fn in rules['aggregations']['entity_meta'].items():
            if fn == "first":
                agg_expressions.append(first(col_name, ignorenulls=True).alias(col_name))

    # count all or count conditional flags
    if 'flag_counts' in rules['aggregations']:
        for alias, source_flag in rules['aggregations']['flag_counts'].items():
            if source_flag == "*":
                agg_expressions.append(
                    count("*").alias(alias)
                )
            else:
                agg_expressions.append(
                    count(when(col(source_flag), True)).alias(alias)
            )

    return agg_expressions


# Build select expressions for final column order stored in lakehouse
def get_select_expressions(layer: str, table_name: str):
    rules = CONFIG[layer][table_name]

    select_exprs = []
    for c in rules["final_column_order"]:
        if isinstance(c, str):
            # like this
            # - start_time_utc
            select_exprs.append(col(c))
        else:
            # like this
            # - expr: pt.team_id
            #   name: team_id
            select_exprs.append(col(c["expr"]).alias(c["name"]))

    return select_exprs


# Build select expressions for final column order stored in lakehouse
def get_group_by_cols(layer: str, table_name: str):
    return CONFIG[layer][table_name]["group_by_cols"]