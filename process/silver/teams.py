import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from utils.io_utils import write_to_silver


def write_team_data(spark: SparkSession, new_previews_df: DataFrame, job_run_date: str):
    """
    Extracts unique teams from a DataFrame of new match previews and merges
    them into the silver.dim_teams table.
    """
    logging.info("--- Starting Silver processing for dim_teams ---")

    try:
        # --- Transformation Step ---
        # Select home and away teams from match_preview data
        home_teams_df = new_previews_df.select(
            col("data.homeTeamId").alias("team_id"),
            col("data.homeTeamName").alias("team_name"),
            col("data.homeTeamCountryName").alias("country_name")
        )

        away_teams_df = new_previews_df.select(
            col("data.awayTeamId").alias("team_id"),
            col("data.awayTeamName").alias("team_name"),
            col("data.awayTeamCountryName").alias("country_name")
        )

        # Union, deduplicate, and filter out null team_ids
        final_teams_df = (
            home_teams_df.unionByName(away_teams_df)
                         .dropDuplicates(["team_id"])
                         .filter(col("team_id").isNotNull())
        )
        
        # --- Writing Data ---
        record_count = final_teams_df.count()
        if record_count == 0:
            logging.warning("No unique, non-null teams found in the new data. Skipping write.")
            return
        
        final_teams_df.printSchema()

        logging.info(f"Writing {record_count} unique teams to silver.dim_teams...")
        
        success = write_to_silver(
            spark=spark,
            df=final_teams_df,
            table_name="dim_teams",
            primary_keys=["team_id"],
            partition_by=["load_date_hour"],
            load_date_hour=job_run_date
        )
        
        if not success:
            raise RuntimeError("write_to_silver function reported a failure.")
            
        logging.info("--- Successfully processed and wrote data for dim_teams ---")

    except Exception as e:
        logging.error(f"Failed during transformation or write for dim_teams. Aborting. Error: {e}", exc_info=True)
        raise