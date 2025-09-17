import argparse, os
import logging
from pyspark.sql import SparkSession
from utils.spark_session import get_spark

from aggregation.gold.player_match_stats import build_player_match_stats
from aggregation.gold.player_team_season_stats import build_player_team_season_stats
from aggregation.gold.team_match_stats import build_team_match_stats
from aggregation.gold.team_season_stats import build_team_season_stats
from aggregation.gold.match_summary import extend_match_summary

from utils.logging_utils import init_logging
from utils.io_utils import read_df
from aggregation.utils import get_map_from_json, write_to_gold


script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.abspath(os.path.join(script_dir, "..", "configs"))

# again basically inside local configs folder
QUALIFIER_MAPPING_PATH = os.path.join(config_dir, "qualifier_mapping.json")
EVENT_TYPE_MAPPING_PATH = os.path.join(config_dir, "event_type_mapping.json")


def run_gold_pipeline(spark: SparkSession, load_date_hour_filter: str = None):
    """
    Orchestrates the entire Gold layer creation process, from reading Silver
    tables to writing final analytical aggregate and dimension tables.
    """
    logging.info("--- Starting Gold Layer Pipeline Run ---")
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    # ==========================================================================
    # 1. READ SOURCE TABLES FROM SILVER
    # This is a critical step. If any read fails, we abort the entire pipeline.
    # ==========================================================================
    logging.info("--- Step 1/4: Reading Source Tables from Silver ---")
    try:
        read_options = {}
        if load_date_hour_filter:
            logging.info(f"Applying incremental filter: load_date_hour = '{load_date_hour_filter}'")
            read_options["where"] = f"load_date_hour = '{load_date_hour_filter}'"

        player = read_df(spark, "silver", "dim_players", **read_options)
        team = read_df(spark, "silver", "dim_teams", **read_options)
        stage = read_df(spark, "silver", "dim_stages", **read_options)
        match = read_df(spark, "silver", "fct_match_summary", **read_options)
        event = read_df(spark, "silver", "fct_match_events", **read_options)
        participation = read_df(spark, "silver", "fct_player_match_participation", **read_options)

        qualifier_map = get_map_from_json(QUALIFIER_MAPPING_PATH)
        event_type_map = get_map_from_json(EVENT_TYPE_MAPPING_PATH)
        logging.info("✅ All source tables read successfully.")
    except Exception as e:
        logging.critical("FATAL: Failed to read a source Silver table. The pipeline cannot continue.", exc_info=True)
        raise e
    
    # ==========================================================================
    # 2. BUILD AND MERGE THE GRANULAR PLAYER_MATCH_STATS TABLE
    # This is the foundational fact table for the subsequent aggregations.
    # ==========================================================================
    full_player_match_stats = None
    try:
        logging.info("\n--- Step 2/4: Processing gold.player_match_stats ---")
        
        # Build the new batch of stats from the incremental silver data
        new_player_match_stats_batch = build_player_match_stats(
            event, player, match, participation, team, stage, qualifier_map, event_type_map
        )
        
        # Merge this new batch into the main gold table
        write_to_gold(
            spark, new_player_match_stats_batch, "player_match_stats",
            primary_keys=["match_id", "player_id"], write_mode="merge"
        )
        logging.info("✅ Successfully merged new batch into gold.player_match_stats.")
        
        # Now, load the full, updated table for the next steps
        full_player_match_stats = read_df(spark, "gold", "player_match_stats")
        full_player_match_stats.cache()
        logging.info(f"Cached the full gold.player_match_stats table ({full_player_match_stats.count()} rows) for re-aggregation.")

    except Exception as e:
        logging.error("ERROR: Failed to build or read gold.player_match_stats. Skipping all subsequent aggregations.", exc_info=True)
        # We can't proceed with aggregations if the base table fails
        return
    
    # ==========================================================================
    # 3. RE-AGGREGATE SEASONAL AND TEAM-MATCH TABLES
    # These tables are completely overwritten for consistency.
    # ==========================================================================
    try:
        logging.info("\n--- Step 3/4: Re-aggregating Seasonal and Team-Match Tables ---")
        
        # Build and write player_team_season_stats
        try:
            logging.info("  -> Building player_team_season_stats...")
            player_team_season_stats = build_player_team_season_stats(full_player_match_stats)
            write_to_gold(
                spark, df=player_team_season_stats, table_name="player_team_season_stats",
                primary_keys=["season_id", "team_id", "player_id"], write_mode="overwrite"
            )
            logging.info("  ✅ Successfully overwrote gold.player_team_season_stats.")
        except Exception as e:
            logging.error("  ERROR: Failed to build player_team_season_stats.", exc_info=True)

        # Build and write team_match_stats
        try:
            logging.info("  -> Building team_match_stats...")
            team_match_stats = build_team_match_stats(full_player_match_stats, match)
            write_to_gold(
                spark, df=team_match_stats, table_name="team_match_stats",
                primary_keys=["match_id", "team_id"], write_mode="overwrite"
            )
            logging.info("  ✅ Successfully overwrote gold.team_match_stats.")

            # Build and write team_season_stats (depends on the one we just made)
            logging.info("  -> Building team_season_stats...")
            team_season_stats = build_team_season_stats(team_match_stats)
            write_to_gold(
                spark, df=team_season_stats, table_name="team_season_stats",
                primary_keys=["season_id", "team_id"], write_mode="overwrite"
            )
            logging.info("  ✅ Successfully overwrote gold.team_season_stats.")
        except Exception as e:
            logging.error("  ERROR: Failed to build team_match_stats or team_season_stats.", exc_info=True)

    finally:
        # Crucially, unpersist the large table as soon as we're done with it.
        if full_player_match_stats:
            full_player_match_stats.unpersist()
            logging.info("Unpersisted the full_player_match_stats DataFrame.")
            
    # ==========================================================================
    # 4. MERGE DIMENSIONAL AND OTHER FACT TABLES INTO GOLD
    # These are updated incrementally.
    # ==========================================================================
    logging.info("\n--- Step 4/4: Merging Dimensional & Other Fact Tables into Gold ---")
    
    try:
        logging.info("  -> Extending and writing match_summary...")
        extended_match_summary = extend_match_summary(match, stage, team)
        write_to_gold(spark, extended_match_summary, "match_summary", primary_keys=["match_id"], write_mode="merge")
    except Exception as e:
        logging.error("  ERROR: Failed to update match_summary.", exc_info=True)

    try:
        logging.info("  -> Merging dim_players...")
        write_to_gold(spark, player.drop("load_date_hour"), "dim_players", primary_keys=["player_id"], write_mode="merge")
    except Exception as e:
        logging.error("  ERROR: Failed to update dim_players.", exc_info=True)

    try:
        logging.info("  -> Merging dim_teams...")
        write_to_gold(spark, team.drop("load_date_hour"), "dim_teams", primary_keys=["team_id"], write_mode="merge")
    except Exception as e:
        logging.error("  ERROR: Failed to update dim_teams.", exc_info=True)

    try:
        logging.info("  -> Merging dim_stages...")
        write_to_gold(spark, stage.drop("load_date_hour"), "dim_stages", primary_keys=["stage_id"], write_mode="merge")
    except Exception as e:
        logging.error("  ERROR: Failed to update dim_stages.", exc_info=True)

    try:
        # drop unnecessary cols for gold
        # should do all the aggregations before dropping
        columns_to_drop = [
            "load_date_hour", "qualifiers_display_names", "satisfied_events_types_names",
            "qualifiers_values", "satisfied_events_types", "league", "season", "period_display_name"
        ]
        event_to_write = event.drop(*[c for c in columns_to_drop if c in event.columns])
        logging.info("  -> Merging match_events...")
        write_to_gold(spark, event_to_write, "match_events", primary_keys=["match_id", "_event_id"], write_mode="merge")
    except Exception as e:
        logging.error("  ERROR: Failed to update match_events.", exc_info=True)

    logging.info("--- ✅ Gold Layer Pipeline Run Finished ---")
    

# if __name__ == '__main__':
#     # 1. SETUP LOGGING
#     init_logging()

#     # 2. PARSE COMMAND-LINE ARGUMENTS
#     parser = argparse.ArgumentParser(
#         description="Run the Gold layer ETL pipeline for a specific time partition."
#     )
#     parser.add_argument(
#         "--load_date_hour",
#         type=str,
#         help="The specific load partition to process, in 'YYYY-MM-DD-HH' format."
#     )
#     args = parser.parse_args()

#     # 3. INITIALIZE SPARK AND RUN THE PIPELINE
#     spark: SparkSession = None
#     try:
#         logging.info("Initializing Spark session for the Gold pipeline...")
#         spark = get_spark()

#         # spark.sql("SHOW TABLES IN gold")
#         spark.sql("CREATE DATABASE IF NOT EXISTS gold")

#         logging.info(f"Starting Gold pipeline run for partition: {args.load_date_hour}")
#         run_gold_pipeline(spark, load_date_hour_filter=args.load_date_hour)
#         logging.info(f"Successfully completed Gold pipeline run for partition: {args.load_date_hour}")

#     except Exception as e:
#         # Re-raising the exception will cause the script to exit with a non-zero
#         # status code, which is crucial for schedulers like Airflow or Cron to
#         # detect the failure.
#         logging.critical("A fatal, uncaught exception occurred in the pipeline.", exc_info=True)
#         raise
    
#     finally:
#         if spark:
#             logging.info("Stopping Spark session.")
#             spark.stop()