"""
main.py
-------
Entry point. Fetches all 4 APIs and routes each result
to its own store function in horse_racing_db.py.
"""

import logging
import time
import os
from dotenv import load_dotenv

from api_fetcher import fetch_api_1, fetch_api_2, fetch_api_3, fetch_api_4, fetch_all
from horse_racing_db import (
    ensure_database_and_table,
    store_records,       # API-1: meetings / races / runners
    store_api2_records,  # API-2: update with your actual function name
    store_api3_records,  # API-3: update with your actual function name
    store_api4_records,  # API-4: update with your actual function name
)

load_dotenv()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 0))

# Optional: per-API intervals (seconds). If any of these is > 0, the scheduler
# mode is used and each API is called on its own cadence.
API_1_INTERVAL = int(os.getenv("API_1_INTERVAL", 0))
API_2_INTERVAL = int(os.getenv("API_2_INTERVAL", 0))
API_3_INTERVAL = int(os.getenv("API_3_INTERVAL", 0))
API_4_INTERVAL = int(os.getenv("API_4_INTERVAL", 0))

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


def _run_scheduler():
    tasks = [
        {
            "name": "API-1",
            "interval": API_1_INTERVAL,
            "fetch": fetch_api_1,
            "store": store_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-2",
            "interval": API_2_INTERVAL,
            "fetch": fetch_api_2,
            "store": store_api2_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-3",
            "interval": API_3_INTERVAL,
            "fetch": fetch_api_3,
            "store": store_api3_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-4",
            "interval": API_4_INTERVAL,
            "fetch": fetch_api_4,
            "store": store_api4_records,
            "next_run": time.monotonic(),
        },
    ]

    enabled = [t for t in tasks if t["interval"] > 0]
    if not enabled:
        log.info("Scheduler enabled, but no API_*_INTERVAL is > 0. Nothing to do.")
        return

    summary = ", ".join([f"{t['name']}={t['interval']}s" for t in enabled])
    log.info(f"Scheduler mode: {summary}. Press Ctrl+C to stop.")

    while True:
        now = time.monotonic()

        for task in enabled:
            if now < task["next_run"]:
                continue

            try:
                data = task["fetch"]()
                if data:
                    task["store"](data)
            except Exception as err:
                log.error(f"{task['name']} error: {err}")
            finally:
                task["next_run"] = time.monotonic() + task["interval"]

        next_due = min(t["next_run"] for t in enabled)
        sleep_for = max(0.1, min(1.0, next_due - time.monotonic()))
        time.sleep(sleep_for)


def main():
    ensure_database_and_table()

    scheduler_mode = any(
        v > 0
        for v in (API_1_INTERVAL, API_2_INTERVAL, API_3_INTERVAL, API_4_INTERVAL)
    )

    if scheduler_mode:
        _run_scheduler()
        return

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