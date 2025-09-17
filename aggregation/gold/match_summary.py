import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from utils.expression_utils import get_select_expressions


def extend_match_summary(
    match: DataFrame,
    stage: DataFrame,
    team: DataFrame
) -> DataFrame:
    """
    Enriches the match summary fact table with dimensional data like team names
    and season/tournament information.
    
    :param match: The fct_match_summary DataFrame from the silver layer.
    :param stage: The dim_stages DataFrame.
    :param team: The dim_teams DataFrame.
    :return: An enriched DataFrame for the gold layer's match_summary table.
    """
    logging.info("--- Starting enrichment of the match summary table ---")
    
    try:
        # --- 1. Load Configuration ---
        select_expressions = get_select_expressions("gold", "match_summary")

        # --- 2. Perform Joins for Enrichment ---
        logging.info("Joining match summary with stage, home team, and away team dimensions...")
        enriched_match_summary = (
            match
            .join(stage, on="stage_id", how="left")
            .join(
                team.alias("home_team"), 
                col("home_team_id") == col("home_team.team_id"),
                how="left"
            )
            .join(
                team.alias("away_team"), 
                col("away_team_id") == col("away_team.team_id"),
                how="left"
            )
            .select(*select_expressions)
        )
        
        logging.info("--- Successfully enriched the match summary table ---")
        return enriched_match_summary

    except Exception as e:
        logging.error(f"Failed during enrichment of match summary. Error: {e}", exc_info=True)
        raise