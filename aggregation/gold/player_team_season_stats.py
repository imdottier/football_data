import logging
from pyspark.sql import DataFrame
from utils.expression_utils import (
    get_expressions, get_select_expressions, get_group_by_cols, get_derived_expressions
)


def build_player_team_season_stats(
    player_match_stats: DataFrame
) -> DataFrame:
    """
    Aggregates player-match stats to a seasonal level for each team a player played for.
    A season refers to 1 competition (EPL, UCL) in a specific season year (2022/2023).
    A player can play for more than 1 team in 1 season

    :param player_match_stats: A DataFrame with granular, per-match player statistics.
    :return: A DataFrame with aggregated stats for a player's season at a specific team.
    """
    logging.info("--- Starting aggregation for player-team-season statistics ---")
    
    try:
        # --- 1. Load Configuration for Aggregation ---
        # This assumes your helper functions are available
        group_by_cols = get_group_by_cols("gold", "player_team_season_stats")
        agg_expressions = get_expressions("gold", "player_team_season_stats")
        select_expressions = get_select_expressions("gold", "player_team_season_stats")

        # --- 2. Perform the Aggregation ---
        logging.info(f"Grouping by {group_by_cols} and aggregating stats...")
        player_team_season_stats = (
            player_match_stats
            .groupBy(*group_by_cols)
            .agg(*agg_expressions)
            .select(*select_expressions)
        )

        logging.info("--- Successfully aggregated player-team-season statistics ---")
        return player_team_season_stats

    except Exception as e:
        logging.error(f"Failed during aggregation of player-team-season stats. Error: {e}", exc_info=True)
        raise