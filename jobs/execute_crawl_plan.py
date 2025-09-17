import logging
import json
import os
import pandas as pd
import glob
from datetime import datetime

from crawler.utils import (
    get_match_data, MatchStatus,
    reset_driver, write_crawl_time
)
from utils.logging_utils import init_logging

def find_plan_file(run_id: str) -> str | None:
    """Finds the crawl plan Parquet directory for a specific run_id."""
    plan_dir = os.path.join("handoff", run_id, "crawl_plan")
    if os.path.isdir(plan_dir) and os.path.exists(os.path.join(plan_dir, "_SUCCESS")):
        return plan_dir
    return None


# Turn on VPN if needed then crawl
def execute_crawl_plan(run_id: str, is_hdfs: bool):
    """
    Reads a crawl plan and calls the 'get_match_data' function for each match,
    which handles the scraping and writing to the HDFS landing zone.
    """
    logging.info("--- STAGE 2: Executing Crawl Plan on Windows (with VPN) ---")
    
    plan_directory = find_plan_file(run_id)
    if not plan_directory:
        logging.critical(f"FATAL: Crawl plan directory for run_id '{run_id}' not found.")
        return

    driver = None
    try:
        logging.info(f"Reading crawl plan from Parquet directory: {plan_directory}")
        plan_df = pd.read_parquet(plan_directory)
        matches_to_process = plan_df.to_dict('records')
        
        if not matches_to_process:
            logging.info("Crawl plan is empty. Nothing to crawl.")
            return

        logging.info(f"Found {len(matches_to_process)} matches to crawl.")
        
        driver = reset_driver(None)

        summary = {"total": len(matches_to_process), "fetched": 0, "exists": 0, "future": 0, "failed": 0}
        
        for i, job in enumerate(matches_to_process):
            try:
                match_id = int(job['match_id'])
                start_time_str = job['start_time_utc']
                # Reconstruct the stage_path string from the plan's columns
                stage_path = os.path.join(
                    f"league={job['league']}", 
                    f"season={job['season']}", 
                    f"stage_id={job['stage_id']}"
                )
            except KeyError as e:
                logging.error(f"  ❌ Skipping job due to missing key in crawl plan: {e}. Row: {job}")
                continue
  
            # --- THE CRITICAL PART ---
            # Call your original, self-contained get_match_data function.
            # It will handle the should_write check, scraping, and writing to HDFS.
            # We are telling it that the target is HDFS.
            status = get_match_data(
                match_id=match_id,
                start_time_string=start_time_str,
                stage_path=stage_path,
                driver=driver,
                is_hdfs=is_hdfs # <-- Explicitly set this to True
            )

            if status == MatchStatus.FETCHED:
                summary["fetched"] += 1
            elif status == MatchStatus.FAILED:
                summary["failed"] += 1

        logging.info(f"""
        Batch summary
        Total: {summary['total']}, Fetched: {summary['fetched']}, Failed: {summary['failed']}""")
        
    except Exception as e:
        logging.critical("A fatal error occurred during the crawl execution.", exc_info=True)
        raise
    finally:
        if driver:
            logging.info("Closing browser session.")
            driver.quit()


if __name__ == '__main__':
    init_logging()
    
    hdfs_logger = logging.getLogger('hdfs')
    # Set its logging level to WARNING, so it only prints important messages
    hdfs_logger.setLevel(logging.WARNING)
    write_crawl_time(is_hdfs=True)

    run_id = None
    try:
        handoff_file_path = "handoff/latest_run_id.txt"
        with open(handoff_file_path, "r") as f:
            run_id = f.read().strip()
        if not run_id:
            raise ValueError("run_id is empty.")
        logging.info(f"Found RUN_ID: {run_id}")
            
    except (FileNotFoundError, ValueError) as e:
        logging.critical(f"Could not read valid RUN_ID from '{handoff_file_path}'. Did planner run first? Error: {e}")
        exit(1)

    execute_crawl_plan(run_id, is_hdfs=True)