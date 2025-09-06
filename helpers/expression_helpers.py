import yaml
from pyspark.sql.functions import sum, avg, first, col, count, when

# load global config
with open('config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

# build a list of aggs expression from config.yaml
# including sum, avg, first (non agg values)...
def get_expressions(layer: str, table_name: str):
    rules = CONFIG[layer][table_name]

    agg_expressions = []

    # Sum/Avg on boolean or numeric flags
    if 'flags_agg' in rules['aggregations']:
        for col_name, fn in rules['aggregations']['flags_agg'].items():
            if fn == "sum":
                agg_expressions.append(sum(col_name).alias(col_name))
            elif fn == "avg":
                agg_expressions.append(round(avg(col_name), 2).alias(col_name))

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