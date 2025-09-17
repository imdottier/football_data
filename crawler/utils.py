import logging
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import time, random
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta, timezone
import os
from typing import Any
from enum import Enum

from utils.io_utils import HdfsClient
from hdfs import HdfsError

from dotenv import load_dotenv

load_dotenv()

HDFS_URL = os.getenv("HDFS_URL", "user")
HDFS_USER = os.getenv("HDFS_USER", "user")
HDFS_BRONZE_BASE_PATH = os.getenv("HDFS_BRONZE_BASE_PATH")
BRONZE_BASE = "bronze_raw"

# Enum for return status of get_match_data
class MatchStatus(Enum):
    FETCHED = 1
    ALREADY_EXISTS = 2
    FAILED = 3
    FUTURE_MATCH = 4

# Chrome options
options = Options()
# comment out this line to run with browser open
# options.add_argument("--headless=new")

options.add_argument("--disable-blink-features=AutomationControlled") # bypass bot detection
options.add_argument("--no-sandbox") # for wsl
options.add_argument("--disable-dev-shm-usage") # or container
options.add_argument("--window-size=1920,1080")
options.add_experimental_option("excludeSwitches", ["enable-automation"]) # anti-detection settings
options.add_experimental_option("useAutomationExtension", False)
options.binary_location = "/usr/bin/google-chrome"

prefs = {
    "profile.managed_default_content_settings.javascript": 2,
    "profile.managed_default_settings.images": 2
}
options.add_experimental_option("prefs", prefs)

driver_service = Service(ChromeDriverManager().install())

def reset_driver(driver: webdriver.Chrome):
    if driver:
        driver.quit()
    
    time.sleep(random.uniform(10, 20))
    print("INFO: Starting a new browser session...")
    # Reuse the already installed service
    driver = webdriver.Chrome(service=driver_service, options=options)
    return driver


script_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.abspath(os.path.join(script_dir, '..', 'configs'))
LEAGUE_MAPPING_PATH = os.path.join(config_path, "league_mapping.json")

def load_league_mapping(file_path: str = LEAGUE_MAPPING_PATH) -> dict[tuple, str]:
    """Loads the league mapping from a JSON file."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            # JSON keys must be strings, so we convert them back to tuples
            return {tuple(map(int, k.split(','))): v for k, v in data.items()}
    except FileNotFoundError:
        logging.critical(f"FATAL: Mapping file not found at '{file_path}'. Cannot proceed.")
        raise
    except (json.JSONDecodeError, ValueError) as e:
        logging.critical(f"FATAL: Could not parse mapping file '{file_path}'. Error: {e}")
        raise


def get_season_ids(region_id: int, tournament_id: int, start_year_after: int, driver: webdriver.Chrome) -> tuple[list[str], webdriver.Chrome]:
    """
    Fetches season IDs using Selenium, preserving the original retry logic while adding robustness with waits.
    """
    league_url = f"https://www.whoscored.com/regions/{region_id}/tournaments/{tournament_id}"
    
    # --- Attempt 1 (Your original logic, enhanced with a wait) ---
    try:
        driver.get(league_url)
        # We add a wait here. This makes the initial attempt much more likely to succeed
        # by waiting for the javascript to populate the dropdown.
        WebDriverWait(driver, 7).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "select#seasons option")))
    except Exception as e:
        logging.warning(f"Initial page load or wait failed for tournament {tournament_id}: {e}. Proceeding to retry block.")
        # If the wait fails, we'll let the next block handle it.
        pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    season_options = soup.select('select#seasons option')

    # --- Retry Block (Your exact logic, preserved) ---
    if not season_options:
        logging.warning(f"Could not find season dropdown for tournament {tournament_id}. Retrying...")
        try:
            driver = reset_driver(driver)
            driver.get(league_url)
            # We add the same robust wait to the retry attempt.
            WebDriverWait(driver, 10).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "select#seasons option")))
        except Exception as e:
            logging.error(f"Retry attempt also failed for tournament {tournament_id}: {e}", exc_info=True)
            # Fall through to the final check, which will handle the empty list.
        
        # Re-parse the page after the retry attempt
        soup = BeautifulSoup(driver.page_source, "html.parser")
        season_options = soup.select('select#seasons option')

    # --- Final Check (Your exact logic, preserved) ---
    if not season_options:
        logging.error(f"Could not find season dropdown for tournament {tournament_id} after retry. Skipping.")
        return [], driver
    
    # --- Parsing Loop (Your exact logic, preserved and made safer) ---
    season_ids = []
    for season_option in season_options:
        value_attr = season_option.get("value")
        display_name = season_option.string
        if value_attr and display_name:
            try:
                # YOUR PROVEN PARSING LOGIC
                season_id = value_attr.split('/')[6]
                
                parts = display_name.split('/')
                start_year = int(parts[0])

                if start_year >= start_year_after: # Using direct positive condition, same logic
                    season_ids.append(season_id)
                    
            except (IndexError, ValueError) as e:
                # This makes your parsing safer without changing the logic.
                logging.warning(f"Could not parse season option. Value: '{value_attr}', Name: '{display_name}'. Error: {e}")

    return season_ids, driver


def get_stage_ids(region_id: int, tournament_id: int, season_id: str, driver: webdriver.Chrome, retries: int = 1) -> tuple[list[str], webdriver.Chrome]:
    """
    Fetches stage IDs for a season, trying a dropdown first and then a canonical link fallback.
    Includes a retry mechanism for robustness.
    """
    season_url = f"https://www.whoscored.com/regions/{region_id}/tournaments/{tournament_id}/seasons/{season_id}"

    for attempt in range(retries + 1):
        try:
            driver.get(season_url)
            
            # --- Plan A: Try to find the dynamic stage dropdown first ---
            try:
                # We wait a short time for the dropdown. If it's there, we use it.
                wait = WebDriverWait(driver, 2) # 5-second max wait
                wait.until(EC.presence_of_element_located((By.ID, "stages")))
                
                # If the wait succeeds, we know the element is there.
                soup = BeautifulSoup(driver.page_source, "html.parser")
                stage_options = soup.select('select#stages option')

                if stage_options:
                    stage_ids = []
                    for stage_option in stage_options:
                        value_attr = stage_option.get("value")
                        if value_attr:
                            try:
                                # YOUR ORIGINAL, PROVEN LOGIC
                                stage_id = value_attr.split('/')[8]
                                stage_ids.append(stage_id)
                            except IndexError:
                                logging.warning(f"Could not parse stage_id from value: {value_attr}")
                    
                    if stage_ids:
                        return stage_ids, driver

            except TimeoutException:
                # This is not an error, it just means the dropdown isn't on the page.
                logging.info(f"Stage dropdown not found for season {season_id}. Proceeding to canonical link fallback.")

            # --- Plan B: If Plan A fails or returns no IDs, try the canonical link ---
            # We are still on the same page, no need to reload.
            soup = BeautifulSoup(driver.page_source, "html.parser")
            canonical_link_tag = soup.find('link', {'rel': 'canonical'})
            if canonical_link_tag and canonical_link_tag.get('href'):
                href = canonical_link_tag['href']
                try:
                    # YOUR ORIGINAL, PROVEN LOGIC
                    stage_id_part = href.split('/stages/')[1]
                    stage_id = stage_id_part.split('/')[0]
                    return [stage_id], driver
                except IndexError:
                    logging.warning(f"Could not parse stage_id from canonical URL: {href}")

        except Exception as e:
            # This catches critical errors with the driver or page load itself.
            logging.error(f"A critical error occurred during attempt {attempt + 1} for season {season_id}: {e}", exc_info=True)

        # If we reach this point, the attempt failed. If retries are left, log and loop.
        if attempt < retries:
            logging.warning(f"No stages found on attempt {attempt + 1}. Resetting driver and retrying...")
            driver = reset_driver(driver)

    # If the loop finishes, all attempts have failed.
    logging.error(f"Failed to find any stage IDs for season {season_id} after all retries. Skipping season.")
    return [], driver


def write_json_to_bronze(file_path: str, is_hdfs: bool, content: dict, overwrite: bool = False) -> bool:
    """
    Writes a dictionary to a JSON file, either locally or on HDFS.

    Args:
        file_path: The relative path to the file (e.g., 'stage_data/league=.../stage_info.json').
        is_hdfs: Boolean flag to determine the target filesystem.
        content: The dictionary to write as JSON.
        overwrite: If False, skips writing if the file already exists.

    Returns:
        True if the write was successful, False otherwise.
    """
    if is_hdfs:
        client = HdfsClient.get_client(hdfs_url=HDFS_URL, user=HDFS_USER)
        if not client:
            logging.error("HDFS client is not available. Cannot write file.")
            return False
            
        # The file_path should be relative to the bronze layer
        hdfs_path = os.path.join(BRONZE_BASE, file_path)
        try:
            if not overwrite and client.status(hdfs_path, strict=False):
                # Using logging.info for skips is better for tracking
                logging.debug(f"SKIP: HDFS file already exists: {hdfs_path}")
                return True # Skipped is a form of success

            # client.write handles directory creation automatically with `makedirs` logic
            with client.write(hdfs_path, encoding="utf-8", overwrite=True) as f:
                json.dump(content, f, ensure_ascii=False)
            return True

        except HdfsError as e:
            logging.error(f"HDFS Error: Failed to write to {hdfs_path}. Details: {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"Unexpected Error: Failed to write to {hdfs_path}. Details: {e}", exc_info=True)
            return False
    
    else:
        try:
            local_path = os.path.join("bronze_raw", file_path)

            if not overwrite and os.path.exists(local_path):
                logging.debug(f"SKIP: Local file already exists: {local_path}")
                return True

            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "w", encoding="utf-8") as f:
                json.dump(content, f, ensure_ascii=False)
            return True

        except (IOError, OSError) as e:
            logging.error(f"File System Error: Failed to write to {file_path}. Details: {e}", exc_info=True)
            return False
        except Exception as e:
            logging.error(f"Unexpected Error: Failed to write to {file_path}. Details: {e}", exc_info=True)
            return False


def process_season_stages(region_id: int, tournament_id: int, season_id: str, stage_id: str, league_name: str, driver: webdriver.Chrome, is_hdfs: bool) -> dict[str, Any]:
    """
    Fetches, parses, and writes stage data, returning key metadata for the next step.
    """
    stage_url = f"https://www.whoscored.com/regions/{region_id}/tournaments/{tournament_id}/seasons/{season_id}/stages/{stage_id}"
    
    try:
        driver.get(stage_url)
        # Wait for the specific data script tag to be present.
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'script[data-hypernova-key="tournamentfixtures"]')))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        script_tag = soup.select_one('script[data-hypernova-key="tournamentfixtures"]')
        
        if not script_tag:
            logging.error(f"Data script tag not found on page: {stage_url}")
            return None

        raw_json = script_tag.string.strip().removeprefix("<!--").removesuffix("-->")
        data = json.loads(raw_json)
        stage_data = data["tournaments"][0]

    except TimeoutException:
        logging.error(f"Data script tag did not appear within 10 seconds for stage: {stage_id} at {stage_url}")
        return None
    except (json.JSONDecodeError, KeyError, IndexError, TypeError) as e:
        logging.error(f"Failed to parse or access key data for stage {stage_id}. Details: {e}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during data extraction for stage {stage_id}: {e}", exc_info=True)
        return None

    # --- Data Processing and Metadata Generation ---
    season_name = stage_data.get("seasonName", "")
    stage_name = stage_data.get("stageName", "UnknownStage")
    season_partition = season_name.replace('/', '-')
    
    # --- Year Parsing Logic (Integrated directly, as requested) ---
    start_year, end_year = None, None
    try:
        parts = season_partition.split('-')
        if len(parts) == 1 and parts[0].isdigit():
            start_year = end_year = int(parts[0])
        elif len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
            start_year, end_year = int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        # Fail gracefully if parsing is not possible
        pass 

    if start_year is None:
        logging.error(f"Could not determine start/end year from seasonName: '{season_name}' for stage {stage_id}. Cannot proceed.")
        return None

    # Your pragmatic fix for EURO 2020.
    # Special case: EURO 2020 was played in 2021.
    if league_name == "international-european-championship" and start_year == 2020:
        logging.info("Applying special year correction for EURO 2020.")
        start_year = 2021
        end_year = 2021
        
    # --- File Output Logic ---
    stage_path = os.path.join(f"league={league_name}", f"season={season_partition}", f"stage_id={stage_id}")
    output_dir = os.path.join("stage_data", stage_path)
    output_file = os.path.join(output_dir, "stage_info.json")

    output_payload = {
        "metadata": {
            "stage_id": stage_id,
            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
            "source_url": stage_url,
        },
        "data": stage_data
    }

    try:
        write_json_to_bronze(output_file, is_hdfs, output_payload, overwrite=True)
        logging.info(f"SUCCESS: Wrote data for stage {stage_id} to {'HDFS' if is_hdfs else 'local'}/{output_file}")
        
        return {
            "season_id": season_id,
            "stage_id": stage_id,
            "stage_name": stage_name,
            "start_year": start_year,
            "end_year": end_year,
            "stage_path": stage_path
        }
        
    except Exception as e:
        logging.critical(f"FATAL ERROR: Could not write file for stage {stage_id} to {output_file}. Details: {e}", exc_info=True)
        return None


def generate_month_keys(start_year: int, end_year: int) -> list[str]:
    """
    Generates a list of year-month keys (e.g., '202301')
    between start_year and end_year (both inclusive).
    """
    if start_year > end_year:
        logging.warning(f"generate_month_keys called with start_year ({start_year}) > end_year ({end_year}). Returning empty list.")
        return []
        
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            key = f"{year}{month:02d}"
            months.append(key)
    return months


def _fetch_and_process_matches_for_period(
    stage_id: str,
    start_year: int,
    end_year: int,
    stage_path: str,
    driver: webdriver.Chrome,
    is_hdfs: bool
) -> tuple[list[tuple[int, str]], webdriver.Chrome]:
    """
    A dedicated function to fetch all matches within a specific year range for a stage.
    """
    all_match_configs = []
    processed_match_ids = set()
    month_keys = generate_month_keys(start_year, end_year)

    for month_key in month_keys:
        monthly_data_url = f"https://www.whoscored.com/tournaments/{stage_id}/data/?d={month_key}&isAggregate=false"
        
        # --- Attempt 1 ---
        try:
            driver.get(monthly_data_url)
            # Your original method, but we will try to parse it immediately.
            raw_text = driver.execute_script("return document.body.innerText || null;")
            data = json.loads(raw_text)
        except Exception: # Catches both Selenium errors and json.JSONDecodeError
            data = None

        # --- Attempt 2 (Retry) ---
        if not data:
            logging.warning(f"Initial fetch/parse failed for {monthly_data_url}, retrying once...")
            try:
                driver = reset_driver(driver)
                driver.get(monthly_data_url)
                raw_text = driver.execute_script("return document.body.innerText || null;")
                data = json.loads(raw_text)
            except Exception as e:
                logging.error(f"Retry also failed for {monthly_data_url}: {e}")
                continue # Skip to the next month

        if not data:
            logging.error(f"Still no JSON after retry for {monthly_data_url}. Skipping month.")
            continue
        
        # --- JSON Processing ---
        try:
            if not isinstance(data, dict) or "tournaments" not in data:
                logging.warning(f"JSON from {monthly_data_url} is not in the expected format.")
                continue

            for stage in data.get("tournaments", []):
                for match in stage.get("matches", []):
                    match_id = match.get("id")
                    start_time_utc = match.get("startTimeUtc")

                    if not all([match_id, start_time_utc]) or match_id in processed_match_ids:
                        continue

                    # File writing logic is unchanged but now inside this more focused function
                    preview_file_path = os.path.join(
                        "match_data", stage_path, f"match_id={match_id}", "match_preview.json"
                    )
                    
                    output_payload = {
                        "metadata": {
                            "match_id": match_id,
                            "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                            "source_url": monthly_data_url,
                        },
                        "data": match
                    }
                    if not write_json_to_bronze(preview_file_path, is_hdfs, output_payload, overwrite=True):
                        logging.error(f"Failed to write preview file for match {match_id}")

                    all_match_configs.append((match_id, start_time_utc))
                    processed_match_ids.add(match_id)
        
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON for {monthly_data_url}. Details: {e}")
        except Exception as e:
            logging.error(f"Unexpected error processing month {month_key}. Details: {e}", exc_info=True)

    return all_match_configs, driver


def get_match_configs(stage_config: dict[str, Any], driver: webdriver.Chrome, is_hdfs: bool) -> tuple[list[tuple[int, str]], webdriver.Chrome]:
    """
    Fetches all match configs for a stage, with a special retry by shifting the year range.
    """
    required_keys = ["stage_id", "start_year", "end_year", "stage_path"]
    if not all(stage_config.get(key) for key in required_keys):
        logging.error(f"get_match_configs received invalid stage_metadata: {stage_config}")
        return [], driver
    
    stage_id = stage_config["stage_id"]
    start_year = stage_config["start_year"]
    end_year = stage_config["end_year"]
    
    logging.info(f"Start getting match_configs for stage {stage_id} for years {start_year}-{end_year}")
    
    # --- First Attempt (Your original logic) ---
    match_configs, driver = _fetch_and_process_matches_for_period(
        stage_id, start_year, end_year, stage_config["stage_path"], driver, is_hdfs
    )

    # --- Year-Shift Retry (Your clever fallback) ---
    if not match_configs:
        logging.warning(f"No matches found for {start_year}-{end_year}. Retrying with shifted years: {start_year + 1}-{end_year + 1}")
        # We re-use the same helper function with the new year range
        match_configs, driver = _fetch_and_process_matches_for_period(
            stage_id, start_year + 1, end_year + 1, stage_config["stage_path"], driver, is_hdfs
        )

    logging.info(f"Found a total of {len(match_configs)} unique matches for stage {stage_id}")
    return match_configs, driver


def extract_json_object(text, start_key):
    start_idx = text.find(start_key)
    if start_idx == -1:
        raise ValueError(f"Key '{start_key}' not found in text.")

    brace_start = text.find('{', start_idx)
    if brace_start == -1:
        raise ValueError(f"Opening '{{' not found after key '{start_key}'.")

    brace_count = 0
    for i in range(brace_start, len(text)):
        if text[i] == '{':
            brace_count += 1
        elif text[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                return text[brace_start:i+1]

    raise ValueError(f"Could not find matching closing '}}' for key '{start_key}'.")


def get_match_data(match_id: int, start_time_string: str, stage_path: str, driver: webdriver.Chrome, is_hdfs: bool) -> 'MatchStatus':
    # Your pathing and initial check logic is perfect.
    output_dir = os.path.join("match_data", stage_path, f"match_id={match_id}")
    output_file = os.path.join(output_dir, "match_data.json")
    
    start_time_UTC = datetime.strptime(start_time_string, '%Y-%m-%dT%H:%M:%SZ')
    if start_time_UTC > datetime.now():
        return MatchStatus.FUTURE_MATCH

    url = f"https://www.whoscored.com/matches/{match_id}/live"
    
    try:
        driver.get(url)
        
        # THE CRITICAL FIX: Wait for the script tag containing the data to appear.
        # XPath is powerful for finding elements based on their text content.
        # We wait for a script that contains the key text "matchCentreData".
        wait = WebDriverWait(driver, 15) # Wait a maximum of 15 seconds
        wait.until(EC.presence_of_element_located((
            By.XPATH, "//script[contains(text(), 'matchCentreData') and contains(text(), 'formationIdNameMappings')]"
        )))

        # Now that we know the script exists, we can parse the page confidently.
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Instead of looping, we can now find the specific script tag much faster
        # because the wait has guaranteed it's there.
        script_tag = soup.find("script", string=lambda text: text and "matchCentreData" in text and "formationIdNameMappings" in text)

        if not script_tag or not script_tag.string:
            logging.warning(f"Could not find the data script tag for match {match_id} even after waiting.")
            return MatchStatus.FAILED

        script_text = script_tag.string
        
        # YOUR BRILLIANT EXTRACTION LOGIC - REMAINS 100% UNCHANGED
        mcd_str = extract_json_object(script_text, "matchCentreData:")
        event_map_str = extract_json_object(script_text, "matchCentreEventTypeJson:")
        formation_map_str = extract_json_object(script_text, "formationIdNameMappings:")

        # --- The rest of your proven logic remains the same ---
        match_centre_data = json.loads(mcd_str)
        event_type_mapping = json.loads(event_map_str)
        formation_mapping = json.loads(formation_map_str)

        final_data_object = {
            "metadata": {
                "match_id": match_id,
                "crawled_at": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                "source_url": url,
            },
            "matchId": match_id,
            "matchCentreData": match_centre_data,
            "matchCentreEventTypeJson": event_type_mapping,
            "formationIdNameMappings": formation_mapping
        }

        # Check the return value from the (previously refactored) write function
        if write_json_to_bronze(output_file, is_hdfs, final_data_object, overwrite=True):
            # logging.info(f"SUCCESS: Wrote data for match {match_id}")
            return MatchStatus.FETCHED
        else:
            logging.error(f"FAILED: Could not write file for match {match_id}")
            return MatchStatus.FAILED
        
    except TimeoutException:
        logging.error(f"Data script for match {match_id} did not load in time. URL: {url}")
        return MatchStatus.FAILED
    except Exception as e:
        # This catches JSON parsing, file writing, or any other unexpected error
        logging.error(f"An unexpected error occurred for match {match_id}. URL: {url}.", exc_info=True)
        return MatchStatus.FAILED
    

def write_crawl_time(is_hdfs: bool):
    crawl_time = datetime.now(timezone.utc)
    crawl_time_str = datetime.strftime(crawl_time, "%Y-%m-%dT%H:%M:%SZ")

    file_path = "last_crawl_timestamp.txt"
    
    try:
        if is_hdfs:
            hdfs_path = f"/user/{HDFS_USER}/bronze_raw/last_crawl_timestamp.txt"

            logging.info(f"Attempting to write crawl timestamp to HDFS: {hdfs_path}")
            
            # --- Use your excellent singleton to get the client ---
            client = HdfsClient.get_client(HDFS_URL, HDFS_USER)
            if not client:
                # The get_client method already logged the critical failure
                raise ConnectionError("Failed to get a valid HDFS client instance.")

            # --- A more robust, explicit write operation ---
            parent_dir = os.path.dirname(hdfs_path)
            
            logging.info(f"Ensuring parent directory '{parent_dir}' exists...")
            client.makedirs(parent_dir)

            client.write(
                hdfs_path=hdfs_path,
                data=crawl_time_str,
                encoding='utf-8',
                overwrite=True
            )
            logging.info(f"Wrote crawl timestamp {crawl_time_str} to HDFS: {hdfs_path}")
        else:
            # always overwrite
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(crawl_time_str)

            logging.info(f"Wrote crawl timestamp {crawl_time_str} to local FS: {file_path}")

    except Exception as e:
        logging.error(f"Could not write crawl timestamp. Details: {e}", exc_info=True)

