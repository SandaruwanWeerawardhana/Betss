"""
main.py
-------
Entry point. Imports from api_fetcher and db_store,
then coordinates the fetch-and-store cycle.
"""

import logging
import time
import os
from dotenv import load_dotenv

from api_fetcher import fetch_data
from db_store import ensure_database_and_table, store_records

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
    records = fetch_data()
    store_records(records)


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
