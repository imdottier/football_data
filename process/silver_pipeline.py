import logging
from pyspark.sql import SparkSession
from datetime import datetime
from utils.io_utils import read_new_bronze_data

from process.silver.stages import write_stage_data
from process.silver.teams import write_team_data
from process.silver.match_summary import write_match_summary_data
from process.silver.players import write_player_data
from process.silver.match_events import write_match_events_data


def run_silver_pipeline(spark: SparkSession, last_crawl_time: datetime, job_run_date):
    """
    Main orchestrator for the Bronze-to-Silver layer.
    Calls the processing logic for each Silver table in order.
    """
    logging.info("="*50)
    logging.info("=== STARTING BRONZE-TO-SILVER PIPELINE ===")
    logging.info("="*50)

    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    try:
        # In theory we cache all for less IO overhead
        # But my machine can't handle that all
        new_stage_info_df = read_new_bronze_data(spark, "stage_info", last_crawl_time, should_cache=False)
        # new_stage_info_df.show(5, truncate=False)

        new_match_previews_df = read_new_bronze_data(spark, "match_preview", last_crawl_time, should_cache=False)
        # new_match_previews_df.printSchema()
        # new_match_previews_df.show(5, truncate=False)
        
        new_match_data_df = read_new_bronze_data(spark, "match_data", last_crawl_time, should_cache=False)
        #new_match_data_df.printSchema()
        # new_match_data_df.show(5, truncate=False)
        
        # --- Call each specialist function in order ---
        
        if new_stage_info_df:
            write_stage_data(spark, new_stage_info_df, job_run_date)
        
        if new_match_previews_df:
            write_team_data(spark, new_match_previews_df, job_run_date)
        
        if new_match_previews_df and new_match_data_df:
            write_match_summary_data(spark, new_match_previews_df, new_match_data_df, job_run_date)
            
        # The 'match_data' source is used for both players and events
        if new_match_data_df:
            write_player_data(spark, new_match_data_df, job_run_date)
            write_match_events_data(spark, new_match_data_df, job_run_date)

        # Data size doesn't allow caching here so no
        # if new_match_previews_df:
        #     new_match_previews_df.unpersist()

        logging.info("\n" + "="*50)
        logging.info("=== BRONZE-TO-SILVER PIPELINE COMPLETED SUCCESSFULLY ===")
        logging.info("="*50)

    except Exception as e:
        logging.critical("A fatal error occurred in the Silver pipeline.", exc_info=True)
        raise