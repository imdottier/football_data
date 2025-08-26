import logging
import asyncio

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

import nest_asyncio
nest_asyncio.apply()

import asyncio
from playwright.async_api import async_playwright

async def fetch_json(url):
    json_response = {}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            async def handle_response(response):
                if response.url == url and response.status == 200:
                    try:
                        json_response['data'] = await response.json()
                        logger.info(f"Successfully received JSON from {url}")
                    except Exception as e:
                        logger.error(f"Failed to parse JSON response: {e}")

            page.on("response", handle_response)

            logger.info(f"Navigating to {url}")
            await page.goto(url)
            await asyncio.sleep(2)
            await browser.close()

    except Exception as e:
        logger.exception(f"Exception occurred while fetching JSON from {url}: {e}")


    return json_response.get('data', None)


def get_country_id(country_name):
    url = "https://www.sofascore.com/api/v1/sport/football/categories"
    
    try:
        data = asyncio.run(fetch_json(url))
        if not data or "categories" not in data:
            logger.error("'categories' key missing in response or empty data")
            return -1

        for category in data["categories"]:
            if category.get("name") == country_name:
                return category.get("id", -1)
        
        logger.warning(f"Country '{country_name}' not found in categories")
        return -1

    except Exception as e:
        logger.exception(f"Exception occurred: {e}")
        return -1
    

def get_unique_tournament_ids(country_id):
    url = f"https://www.sofascore.com/api/v1/category/{country_id}/unique-tournaments"

    print(url)

    selected_tournaments = ["V-League 1", "V-League 2", "Vietname Cup", "Vietnamest Super Cup"]
    selected_ids = []

    try:
        data = asyncio.run(fetch_json(url))
        if not data:
            logger.error("No data returned from fetch_json")
            return []

        groups = data.get("groups")
        if not groups or "uniqueTournaments" not in groups:
            logger.error("'uniqueTournaments' key missing in response or empty 'groups'")
            return []

        for unique_tournament in groups["uniqueTournaments"]:
            name = unique_tournament.get("name")
            if name not in selected_tournaments:
                continue
            selected_ids.append(unique_tournament.get("id"))

    except Exception as e:
        logger.exception(f"Exception occurred: {e}")
        return []
    

if __name__ == "__main__":
    country_id = get_country_id("Vietnam")
    tournament_ids = get_unique_tournament_ids(country_id)

    print(tournament_ids)
