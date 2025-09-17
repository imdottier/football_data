import logging
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import (
    col, to_timestamp, split, regexp_extract,
)
from utils.io_utils import write_to_silver


def parse_score(score_column: Column) -> list[Column]:
    """
    A helper function that takes a score string column (e.g., "3 : 2") and
    returns a list of two columns: home_score and away_score.
    """
    number_pattern = r"(\d+)"
    
    # Split the "H : A" string into an array of ["H", "A"]
    score_array = split(score_column, ":")
    
    home_score = regexp_extract(score_array.getItem(0), number_pattern, 1).cast("integer")
    away_score = regexp_extract(score_array.getItem(1), number_pattern, 1).cast("integer")
    
    return [home_score, away_score]


def write_match_summary_data(
    spark: SparkSession, 
    new_previews_df: DataFrame, 
    new_match_data_df: DataFrame, 
    job_run_date: str
):
    """
    Transforms and joins new preview and match data, then merges the result
    into the silver.fct_match_summary table.
    """
    logging.info("--- Starting Silver processing for fct_match_summary ---")
    
    df_to_unpersist = None
    
    try:
        # --- 1. Transformation Steps ---
        # Flatten and prepare the match preview data
        flat_previews_df = new_previews_df.select(
            col("data.id").alias("match_id"), # Alias to make join key clear
            col("data.stageId").alias("stage_id"),
            col("data.homeTeamId").alias("home_team_id"),
            col("data.awayTeamId").alias("away_team_id"),
            to_timestamp(col("data.startTimeUtc"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time_utc")
        )

        # Transform the match data, parsing all the scores
        transformed_match_data_df = new_match_data_df.select(
            col("matchId").alias("match_id"),
            col("matchCentreData.attendance").alias("attendance"),
            col("matchCentreData.venueName").alias("venue_name"),
            parse_score(col("matchCentreData.score"))[0].alias("home_score"),
            parse_score(col("matchCentreData.score"))[1].alias("away_score"),
            parse_score(col("matchCentreData.htScore"))[0].alias("home_ht_score"),
            parse_score(col("matchCentreData.htScore"))[1].alias("away_ht_score"),
            parse_score(col("matchCentreData.ftScore"))[0].alias("home_ft_score"),
            parse_score(col("matchCentreData.ftScore"))[1].alias("away_ft_score"),
            parse_score(col("matchCentreData.etScore"))[0].alias("home_et_score"),
            parse_score(col("matchCentreData.etScore"))[1].alias("away_et_score"),
            parse_score(col("matchCentreData.pkScore"))[0].alias("home_pk_score"),
            parse_score(col("matchCentreData.pkScore"))[1].alias("away_pk_score"),
        )

        # --- 2. Join the two datasets ---
        fct_match_summary_df = flat_previews_df.join(
            transformed_match_data_df,
            on=["match_id"],
            how="left"
        )
        
        # Cache the result of the expensive join operation
        fct_match_summary_df.cache()
        df_to_unpersist = fct_match_summary_df

        # --- 3. Write to Silver ---
        record_count = fct_match_summary_df.count()
        if record_count == 0:
            logging.warning("Join resulted in an empty DataFrame. Skipping write for fct_match_summary.")
            return

        logging.info(f"Writing {record_count} records to silver.fct_match_summary...")

        success = write_to_silver(
            spark=spark,
            df=fct_match_summary_df,
            table_name="fct_match_summary",
            primary_keys=["match_id"],
            partition_by=["load_date_hour"],
            load_date_hour=job_run_date
        )

        if not success:
            raise RuntimeError("write_to_silver function reported a failure.")

        logging.info("--- Successfully processed and wrote data for fct_match_summary ---")

    except Exception as e:
        logging.error(f"Failed during transformation or write for fct_match_summary. Aborting. Error: {e}", exc_info=True)
        raise
    finally:
        if df_to_unpersist:
            df_to_unpersist.unpersist()
            logging.info("Unpersisted the fct_match_summary DataFrame.")