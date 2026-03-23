"""
api_fetcher.py
--------------
Responsible only for calling the API and returning parsed data.
Import fetch_data() from this module wherever you need API results.
"""

import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL", "https://jsonplaceholder.typicode.com/posts")

log = logging.getLogger(__name__)


def fetch_data(url=None):
    """
    Calls the API and returns a list of records.
    Accepts an optional URL override; falls back to API_URL in .env.
    """
    target = url or API_URL

    headers = {
        "Accept":     "application/json",
        "User-Agent": "BusinessScraperBot/1.0"
    }

    log.info(f"Fetching from: {target}")

    response = requests.get(target, headers=headers, timeout=15)
    response.raise_for_status()

    data = response.json()

    # Always return a list regardless of what the API sends back
    records = data if isinstance(data, list) else [data]

    log.info(f"Received {len(records)} record(s) from API.")
    return records
