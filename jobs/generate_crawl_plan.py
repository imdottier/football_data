import logging
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, date_format
import os

# Assume helpers are imported
from utils.spark_session import get_spark
from utils.io_utils import read_df
from utils.logging_utils import init_logging

# This is to deal with VPN issue
# Unfortunately I have to crawl with VPN so can't do spark read
# and crawl in 1 session
# This will find the matches to be crawled then write into 
# a local file as parquet
def generate_crawl_plan(spark: SparkSession, run_id: str):
    """
    Identifies matches to refresh from gold.match_events and writes the plan
    to the local handoff directory as Parquet files.
    """
    logging.info("--- STAGE 1: Generating Crawl Plan from gold.match_events ---")
    
    try:
        # --- 1. Identify Matches to Refresh ---
        three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3))
        now = (datetime.now(timezone.utc))
        # Gold layer doesn't have home_ft_score, so we filter only by time
        match_summary_df = read_df(spark, "gold", "match_summary")
        
        # Assume 'start_time_utc' is a column in match_events
        filter_condition = (
            ((col("start_time_utc") >= lit(three_days_ago)) & (col("start_time_utc") < lit(now)))
            | ((col("home_ft_score").isNull()) & (col("start_time_utc") < lit(now)))
        )
        
        # Select all columns the crawler will need
        crawl_plan_df = match_summary_df.filter(filter_condition).select(
            "match_id",
            date_format(col("start_time_utc"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("start_time_utc"),
            "stage_id",
            "league",
            "season" # Assuming this column exists for path reconstruction
        ).distinct() # Ensure we only get one plan per match

        logging.info(f"Found {crawl_plan_df.count()} matches to be recrawled.")

        # Directory of the current script (e.g. /app/helpers/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_output_path = os.path.abspath(os.path.join(script_dir, '..', 'handoff', run_id, 'crawl_plan'))

        logging.info(f"Writing crawl plan as Parquet to local path: {local_output_path}")
        
        crawl_plan_df.coalesce(1).write.mode("overwrite").parquet(local_output_path)
        logging.info(f"✅ Crawl plan successfully created.")
        
    except Exception as e:
        logging.critical("Failed to generate crawl plan.", exc_info=True)
        raise


if __name__ == '__main__':
    init_logging()
    run_id = datetime.now().strftime('%Y-%m-%d-%H%M%S')

    script_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.abspath(os.path.join(script_dir, '..', 'handoff'))
    os.makedirs(path, exist_ok=True)

    file_path = os.path.join(path, "latest_run_id.txt")
    with open(file_path, "w") as f:
        f.write(run_id)
        
    spark = get_spark(app_name=f"CrawlPlanner-{run_id}")
    try:
        generate_crawl_plan(spark, run_id)
    finally:
        spark.stop()