# main.py

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession

# --- Internal Project Imports ---
# Foundational utilities
from utils.spark_session import get_spark
from utils.logging_utils import init_logging
from utils.io_utils import read_last_crawl_time

# Entry points for each pipeline layer
from process.bronze_pipeline import run_bronze_pipeline
from process.silver_pipeline import run_silver_pipeline
from aggregation.gold_pipeline import run_gold_pipeline


def run_bronze(spark: SparkSession):
    """Wrapper function to execute the Bronze layer pipeline."""
    logging.info("\n" + "="*50)
    logging.info("=== EXECUTING BRONZE LAYER PIPELINE ===")
    logging.info("="*50)
    run_bronze_pipeline(spark)
    logging.info("=== BRONZE LAYER COMPLETE ===")


def run_silver(spark: SparkSession, job_run_date: str):
    """Wrapper function to execute the Silver layer pipeline."""
    logging.info("\n" + "="*50)
    logging.info("=== EXECUTING SILVER LAYER PIPELINE ===")
    logging.info("="*50)
    last_crawl_time = read_last_crawl_time()
    run_silver_pipeline(spark, last_crawl_time, job_run_date)
    logging.info("=== SILVER LAYER COMPLETE ===")


def run_gold(spark: SparkSession, load_date_hour: str):
    """Wrapper function to execute the Gold layer pipeline."""
    if not load_date_hour:
        raise ValueError("--load_date_hour is a required argument when running the Gold layer.")
    
    logging.info("\n" + "="*50)
    logging.info(f"=== EXECUTING GOLD LAYER PIPELINE for partition: {load_date_hour} ===")
    logging.info("="*50)
    run_gold_pipeline(spark, load_date_hour_filter=load_date_hour)
    logging.info("=== GOLD LAYER COMPLETE ===")


def main():
    """
    Main orchestrator for the football ETL pipeline.
    
    Parses command-line arguments to determine which pipeline layers to run.
    """
    # 1. SETUP LOGGING AND ARGUMENT PARSING
    init_logging()
    parser = argparse.ArgumentParser(description="Run the Football ETL pipeline.")
    parser.add_argument(
        "--layers",
        nargs='+',  # Accepts one or more values
        choices=['bronze', 'silver', 'gold', 'all'],
        default=['all'],
        help="Specify which pipeline layers to run (e.g., --layers silver gold). Defaults to 'all'."
    )
    parser.add_argument(
        "--load_date_hour",
        type=str,
        help="The specific load partition for the Gold layer, in 'YYYY-MM-DD-HH' format."
    )
    args = parser.parse_args()

    # 2. INITIALIZE SPARK AND RUN THE PIPELINE
    spark: SparkSession = None
    try:
        logging.info("Initializing Spark session for the pipeline run...")
        spark = get_spark()

        # Generate a single run date for consistency if needed by multiple layers
        # This will be used if 'all' or 'silver'/'gold' layers are run.
        job_run_date = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd-HH')").first()[0]
        
        layers_to_run = args.layers
        if 'all' in layers_to_run:
            layers_to_run = ['bronze', 'silver', 'gold']

        # Determine the load_date_hour for the Gold layer
        gold_load_date = args.load_date_hour or job_run_date

        # 3. EXECUTE THE REQUESTED LAYERS
        if 'bronze' in layers_to_run:
            run_bronze(spark)
        
        if 'silver' in layers_to_run:
            run_silver(spark, job_run_date)
            
        if 'gold' in layers_to_run:
            run_gold(spark, gold_load_date)

        logging.info("\n" + "*"*50)
        logging.info("PIPELINE RUN COMPLETED SUCCESSFULLY")
        logging.info("*"*50)

    except Exception as e:
        logging.critical("A fatal, uncaught exception occurred in the main pipeline.", exc_info=True)
        raise
    
    finally:
        if spark:
            logging.info("Stopping Spark session.")
            spark.stop()


if __name__ == "__main__":
    main()