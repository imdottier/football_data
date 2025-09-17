import json
import logging
from typing import Any
from selenium import webdriver

# --- Import all the previously refactored functions ---
# It's good practice to group them in a file like 'crawler_utils.py' or similar.
from crawler.utils import (
    get_season_ids, get_stage_ids, process_season_stages,
    get_match_configs, get_match_data, reset_driver, MatchStatus, write_crawl_time, load_league_mapping
)
from utils.logging_utils import init_logging

def discover_crawl_tasks(driver: webdriver.Chrome, league_mapping: dict, is_hdfs: bool) -> tuple[list[dict], webdriver.Chrome]:
    """
    Performs the discovery phase: iterates through leagues and seasons to build a
    flat list of all stage-processing tasks to be executed.
    """
    all_stage_tasks = []
    logging.info("--- Starting Discovery Phase: Finding all stages to process ---")
    
    for (region_id, tournament_id, start_year_after), league_name in league_mapping.items():
        try:
            logging.info(f"Discovering seasons for league: {league_name}")
            season_ids, driver = get_season_ids(region_id, tournament_id, start_year_after, driver)
            
            for season_id in season_ids:
                try:
                    stage_ids, driver = get_stage_ids(region_id, tournament_id, season_id, driver)
                    if not stage_ids:
                        logging.warning(f"No stages found for season {season_id}. Skipping.")
                        continue
                    
                    for stage_id in stage_ids:
                        # process_season_stages gets the metadata we need for the task
                        stage_config = process_season_stages(region_id, tournament_id, season_id, stage_id, league_name, driver, is_hdfs)
                        if stage_config:
                            all_stage_tasks.append(stage_config)
                        else:
                            logging.warning(f"Failed to get metadata for stage {stage_id}. Task will be skipped.")
                
                except Exception as e:
                    logging.error(f"Error discovering stages for season {season_id}. Resetting driver and continuing. Error: {e}", exc_info=True)
                    driver = reset_driver(driver) # Reset on season-level failure
        
        except Exception as e:
            logging.error(f"FATAL ERROR discovering seasons for league {league_name}. Skipping to next league. Error: {e}", exc_info=True)
            driver = reset_driver(driver) # Reset on league-level failure
            
    logging.info(f"--- Discovery Complete: Found {len(all_stage_tasks)} total stages to process. ---")
    return all_stage_tasks, driver


def process_matches_for_stage(stage_config: dict[str, Any], driver: webdriver.Chrome, is_hdfs: bool) -> tuple[list[list], webdriver.Chrome]:
    """Processes all matches for a single stage and returns a list of failures."""
    stage_path = stage_config["stage_path"]
    stage_name = stage_config["stage_name"]
    stage_failures = []

    match_configs, driver = get_match_configs(stage_config, driver, is_hdfs=is_hdfs)
    
    # Your summary logic, now encapsulated.
    summary = {"total": len(match_configs), "fetched": 0, "exists": 0, "future": 0, "failed": 0}

    for match_id, start_time_string in match_configs:
        status = get_match_data(match_id, start_time_string, stage_path, driver, is_hdfs=is_hdfs)
        if status == MatchStatus.FETCHED:
            summary["fetched"] += 1
        elif status == MatchStatus.ALREADY_EXISTS:
            summary["exists"] += 1
        elif status == MatchStatus.FUTURE_MATCH:
            summary["future"] += 1
        elif status == MatchStatus.FAILED:
            summary["failed"] += 1
            stage_failures.append([match_id, start_time_string, stage_path])
    
    logging.info(f"""
    Stage '{stage_name}' (ID: {stage_config['stage_id']}) summary:
    Total: {summary['total']}, Fetched: {summary['fetched']}, Existed: {summary['exists']}, Future: {summary['future']}, Failed: {summary['failed']}""")
    
    return stage_failures, driver, summary


def run_full_crawl(is_hdfs: bool = True):
    """Main function to orchestrate the entire batch crawl."""
    driver = None
    all_failures = []
    
    try:
        write_crawl_time(is_hdfs=is_hdfs)
        league_mapping = load_league_mapping()
        
        # --- PHASE 1: DISCOVERY ---
        driver = reset_driver(None) # Start with a fresh driver
        all_tasks, driver = discover_crawl_tasks(driver, league_mapping, is_hdfs)
        
        # --- PHASE 2: EXECUTION ---
        logging.info("\n" + "="*50)
        logging.info("--- Starting Execution Phase: Processing all discovered stages ---")
        
        current_season_id = None
        total_tasks = len(all_tasks)
        
        for i, stage_config in enumerate(all_tasks):
            # Your brilliant logic: reset the driver between seasons for stability
            if stage_config['season_id'] != current_season_id:
                if current_season_id is not None:
                    logging.info(f"End of season {current_season_id}. Resetting driver for stability...")
                    driver = reset_driver(driver)
                current_season_id = stage_config['season_id']
                logging.info(f"--- Processing new Season ID: {current_season_id} ---")
            
            try:
                logging.info(f"Processing Stage {i+1}/{total_tasks}: {stage_config['stage_name']} (ID: {stage_config['stage_id']})")
                stage_failures, driver, summary = process_matches_for_stage(stage_config, driver, is_hdfs)
                
                # Log the summary here for better separation of concerns
                logging.info(f"  -> Stage Summary: Total: {summary['total']}, Fetched: {summary['fetched']}, Existed: {summary['exists']}, Future: {summary['future']}, Failed: {summary['failed']}")
                
                if stage_failures:
                    all_failures.extend(stage_failures)

            except Exception as e:
                logging.error(f"CRITICAL ERROR processing stage {stage_config['stage_id']}. Resetting driver and continuing. Error: {e}", exc_info=True)
                driver = reset_driver(driver) # Reset on stage-level failure
        
        # --- PHASE 3: RETRY & REPORT ---
        retry_and_report(all_failures, driver, is_hdfs)

    finally:
        if driver:
            logging.info("Closing final browser session.")
            driver.quit()


# some matches might still fail so we handle them again in 1 seperate run
def retry_and_report(failures_to_retry: list[list], driver: webdriver.Chrome, is_hdfs: bool):
    """Handles the final retry pass and prints the summary report."""
    final_failures = []
    if failures_to_retry:
        logging.info("\n" + "="*50)
        logging.info(f"RETRYING {len(failures_to_retry)} FAILED MATCHES...")
        
        if not driver: # Ensure we have a driver for the retry pass
            driver = reset_driver(None)
            
        for match_id, start_time_string, stage_path in failures_to_retry:
            status = get_match_data(match_id, start_time_string, stage_path, driver, is_hdfs)
            if status == MatchStatus.FAILED:
                logging.warning(f"  -> RETRY FAILED: Match {match_id} still failing.")
                final_failures.append((match_id, start_time_string, stage_path))
            else:
                logging.info(f"  -> RETRY SUCCESS: Match {match_id} scraped successfully.")

    # --- Final Report ---
    logging.info("\n" + "="*50)
    logging.info("CRAWL COMPLETE.")
    if final_failures:
        logging.warning(f"The following {len(final_failures)} matches could not be scraped after all retries:")
        for match_info in final_failures:
            logging.warning(f"  - Match ID: {match_info[0]}")
    else:
        logging.info("All matches were scraped successfully (or were already present/scheduled).")
    logging.info("="*50)


if __name__ == "__main__":
    init_logging()
    try:
        run_full_crawl(is_hdfs=True)
    except Exception as e:
        # This will catch critical startup errors, like a failed config load.
        logging.critical("The crawl script encountered a fatal top-level error and had to stop.", exc_info=True)