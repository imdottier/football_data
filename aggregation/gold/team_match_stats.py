import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
import pyspark.sql.functions as sf
from pyspark.sql.window import Window

from utils.expression_utils import (
    get_expressions, get_select_expressions, get_group_by_cols, get_derived_expressions
)


def build_team_match_stats(
    player_match_stats: DataFrame,
    match: DataFrame
) -> DataFrame:
    """
    Aggregates player-level stats to the team-match level and enriches the result
    with final scores and match outcomes.
    
    :param player_match_stats: DataFrame with granular, per-match player statistics.
    :param match: DataFrame with match-level summary data (scores, teams, etc.).
    :return: A DataFrame with aggregated stats for each team in each match.
    """
    logging.info("--- Starting aggregation and enrichment for team-match statistics ---")

    try:
        # --- 1. Initial Aggregation ---
        logging.info("Step 1/4: Aggregating player stats to the team-match level...")
        group_by_cols = get_group_by_cols("gold", "team_match_stats")
        agg_expressions = get_expressions("gold", "team_match_stats")
        select_expressions = get_select_expressions("gold", "team_match_stats")    

        team_match_stats = (
            player_match_stats
            .groupBy(*group_by_cols)
            .agg(*agg_expressions)
        )

        # --- 2. Enrich with Match Scores ---
        logging.info("Step 2/4: Joining with match summary to get final scores...")
        match_scores = match.select(
            "match_id", "home_score", "away_score", "home_ht_score", "away_ht_score",
            "home_ft_score", "away_ft_score", "home_et_score", "away_et_score",
            "home_pk_score", "away_pk_score"
        )
        team_match_stats_with_scores = team_match_stats.join(match_scores, "match_id", "left")

        # --- 3. Derive Goals For/Against ---
        # Note: goals_scored can be diff from goals_for in this table because of own_goals
        logging.info("Step 3/4: Calculating goals for/against based on home/away status...")
        team_match_stats = (
            team_match_stats_with_scores
            .withColumn("goals_for", when(col("is_home"), col("home_score")).otherwise(col("away_score")))
            .withColumn("goals_against", when(col("is_home"), col("away_score")).otherwise(col("home_score")))

            .withColumn("goals_for_ht", when(col("is_home"), col("home_ht_score")).otherwise(col("away_ht_score")))
            .withColumn("goals_against_ht", when(col("is_home"), col("away_ht_score")).otherwise(col("home_ht_score")))

            .withColumn("goals_for_ft", when(col("is_home"), col("home_ft_score")).otherwise(col("away_ft_score")))
            .withColumn("goals_against_ft", when(col("is_home"), col("away_ft_score")).otherwise(col("home_ft_score")))

            .withColumn("goals_for_et", when(col("is_home"), col("home_et_score")).otherwise(col("away_et_score")))
            .withColumn("goals_against_et", when(col("is_home"), col("away_et_score")).otherwise(col("home_et_score")))

            .withColumn("goals_for_pk", when(col("is_home"), col("home_pk_score")).otherwise(col("away_pk_score")))
            .withColumn("goals_against_pk", when(col("is_home"), col("away_pk_score")).otherwise(col("home_pk_score")))

            .withColumnRenamed("own_goals", "own_goals_conceded")
        )

        # --- 4. Derive Final Metrics (Possession %, Win/Draw/Loss) ---
        logging.info("Step 4/4: Calculating possession percentages and match results...")
        w_match = Window.partitionBy("match_id")
        team_match_stats = (
            team_match_stats
            # Recalc possession_% to avoid rounding errors
            .withColumn("total_possession", sf.sum("possession").over(w_match))
            .withColumn("possession_percentage", sf.round(col("possession") * 100 / col("total_possession"), 2))
            .drop("total_possession")
            .withColumn("is_win", when(col("goals_for") > col("goals_against"), True).otherwise(False))
            .withColumn("is_draw", when(col("goals_for") == col("goals_against"), True).otherwise(False))
            .withColumn("is_loss", when(col("goals_for") < col("goals_against"), True).otherwise(False))
        )

        final_df = team_match_stats.select(*select_expressions)
        
        logging.info("--- Successfully built team-match statistics ---")
        return final_df

    except Exception as e:
        logging.error(f"Failed during build of team-match stats. Error: {e}", exc_info=True)
        raise