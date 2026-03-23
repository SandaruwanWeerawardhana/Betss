"""
main.py
-------
Entry point. Fetches all 4 APIs and routes each result
to its own store function in db_store.py.
"""

import logging
import time
import os
from dotenv import load_dotenv

from api_fetcher import fetch_all
from db_store import (
    ensure_database_and_table,
    store_records,       # API-1: meetings / races / runners
    store_api2_records,  # API-2: update with your actual function name
    store_api3_records,  # API-3: update with your actual function name
    store_api4_records,  # API-4: update with your actual function name
)

load_dotenv()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 0))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)


def run_once():
    results = fetch_all()

    if results["api_1"]:
        store_records(results["api_1"])

    if results["api_2"]:
        store_api2_records(results["api_2"])

    if results["api_3"]:
        store_api3_records(results["api_3"])

    if results["api_4"]:
        store_api4_records(results["api_4"])


def main():
    ensure_database_and_table()

    if POLL_INTERVAL > 0:
        log.info(f"Real-time mode: polling every {POLL_INTERVAL} second(s). Press Ctrl+C to stop.")
        while True:
            try:
                run_once()
            except Exception as err:
                log.error(f"Cycle error: {err}")
            time.sleep(POLL_INTERVAL)
    else:
        log.info("Single-run mode.")
        run_once()


if __name__ == "__main__":
    main()