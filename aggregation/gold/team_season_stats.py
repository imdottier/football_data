import logging
from pyspark.sql import DataFrame
from utils.expression_utils import (
    get_expressions, get_select_expressions, get_group_by_cols, get_derived_expressions
)


def build_team_season_stats(
    team_match_stats: DataFrame,
) -> DataFrame:
    """
    Aggregates team-match stats to a seasonal level for each competition.
    
    :param team_match_stats: A DataFrame with granular, per-match team statistics.
    :return: A DataFrame with aggregated stats for a team's season in a specific competition.
    """
    logging.info("--- Starting aggregation for team-season statistics ---")
    
    try:
        # --- 1. Load Configuration for Aggregation ---
        group_by_cols = get_group_by_cols("gold", "team_season_stats")
        agg_expressions = get_expressions("gold", "team_season_stats")
        select_expressions = get_select_expressions("gold", "team_season_stats")    

        # --- 2. Perform the Aggregation ---
        logging.info(f"Grouping by {group_by_cols} and aggregating stats...")
        team_season_stats = (
            team_match_stats
            .groupBy(*group_by_cols)
            .agg(*agg_expressions)
            .select(*select_expressions)
        )

        logging.info("--- Successfully aggregated team-season statistics ---")
        return team_season_stats

    except Exception as e:
        logging.error(f"Failed during aggregation of team-season stats. Error: {e}", exc_info=True)
        raise