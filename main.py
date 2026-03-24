"""
main.py
-------
Entry point. Fetches all 4 APIs and routes each result
to its own store function in horse_racing_db.py.
"""

import logging
import time
import os
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

from api_fetcher import (
    fetch_api_1,
    fetch_api_2,
    fetch_api_3,
    fetch_api_4,
    fetch_all,
    fetch_race_runners_by_race,
    fetch_today_meeting_data_by_id,
)
from horse_racing_db import (
    ensure_database_and_table,
    store_records,       # API-1: meetings / races / runners
    store_api2_records,  # API-2: update with your actual function name
    store_api3_records,  # API-3: update with your actual function name
    store_api4_records,  # API-4: update with your actual function name
    get_race_ids,
    iter_race_ids,
)

load_dotenv()

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 0))

# Per-race detail calls (driven by race ids from the `races` table)
FETCH_RACE_DETAILS = os.getenv("FETCH_RACE_DETAILS", "0").strip().lower() in ("1", "true", "yes", "y")
FETCH_TODAY_MEETING_DATA_BY_ID = os.getenv("FETCH_TODAY_MEETING_DATA_BY_ID", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "y",
)
MAX_RACE_IDS_PER_CYCLE = int(os.getenv("MAX_RACE_IDS_PER_CYCLE", 50))
FETCH_ALL_RACE_IDS = os.getenv("FETCH_ALL_RACE_IDS", "0").strip().lower() in ("1", "true", "yes", "y")
RACE_ID_BATCH_SIZE = int(os.getenv("RACE_ID_BATCH_SIZE", 500))

# Optional: per-API intervals (seconds). If any of these is > 0, the scheduler
# mode is used and each API is called on its own cadence.
HORSE_API_1_INTERVAL = int(os.getenv("HORSE_API_1_INTERVAL", 0))
HORSE_API_2_INTERVAL = int(os.getenv("HORSE_API_2_INTERVAL", 0))
HORSE_API_3_INTERVAL = int(os.getenv("HORSE_API_3_INTERVAL", 0))
HORSE_API_4_INTERVAL = int(os.getenv("HORSE_API_4_INTERVAL", 0))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)


def _fetch_and_store_race_runners_for_races(race_ids):
    return _fetch_and_store_race_runners_for_races_internal(race_ids)


def _fetch_and_store_race_runners_for_races_internal(race_ids, *, treat_as_all: bool | None = None):
    if not FETCH_RACE_DETAILS and not FETCH_TODAY_MEETING_DATA_BY_ID:
        return
    if not race_ids:
        log.warning("No race ids available; skipping per-race detail calls.")
        return

    effective_fetch_all = FETCH_ALL_RACE_IDS if treat_as_all is None else bool(treat_as_all)

    count = 0
    if effective_fetch_all:
        log.info("Fetching per-race details for ALL race ids (one-by-one).")
    else:
        log.info("Fetching per-race details for up to %s race(s).", MAX_RACE_IDS_PER_CYCLE)

    use_parallel_per_race = bool(FETCH_TODAY_MEETING_DATA_BY_ID and FETCH_RACE_DETAILS)

    executor = None
    if use_parallel_per_race:
        executor = ThreadPoolExecutor(max_workers=2)

    for race_id in race_ids:
        if race_id is None:
            continue
        try:
            race_id = int(race_id)
        except (TypeError, ValueError):
            continue

        count += 1
        if not effective_fetch_all and MAX_RACE_IDS_PER_CYCLE > 0 and count > MAX_RACE_IDS_PER_CYCLE:
            break

        meeting_payload = None
        rr_payload = None

        if use_parallel_per_race and executor is not None:
            futures = {
                "today_meeting": executor.submit(fetch_today_meeting_data_by_id, race_id),
                "race_runners": executor.submit(fetch_race_runners_by_race, race_id),
            }

            for name, fut in futures.items():
                try:
                    payload = fut.result()
                    if name == "today_meeting":
                        meeting_payload = payload
                    else:
                        rr_payload = payload
                except Exception as err:
                    log.error("Per-race fetch failed (%s) for race_id=%s: %s", name, race_id, err)
        else:
            try:
                if FETCH_TODAY_MEETING_DATA_BY_ID:
                    meeting_payload = fetch_today_meeting_data_by_id(race_id)
                if FETCH_RACE_DETAILS:
                    rr_payload = fetch_race_runners_by_race(race_id)
            except Exception as err:
                log.error("Per-race fetch failed for race_id=%s: %s", race_id, err)

        try:
            if meeting_payload:
                store_records(meeting_payload)
            if rr_payload:
                store_records(rr_payload)
        except Exception as err:
            log.error("Per-race store failed for race_id=%s: %s", race_id, err)

    if executor is not None:
        executor.shutdown(wait=True)


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

    # After featured data is stored, read race ids from DB and fetch runners.
    try:
        if FETCH_ALL_RACE_IDS:
            ids = iter_race_ids(batch_size=RACE_ID_BATCH_SIZE)
        else:
            ids = get_race_ids(limit=MAX_RACE_IDS_PER_CYCLE)
    except Exception as err:
        log.error("Failed reading race ids from DB: %s", err)
        ids = []

    _fetch_and_store_race_runners_for_races(ids)


def _run_scheduler():
    tasks = [
        {
            "name": "API-1",
            "interval": HORSE_API_1_INTERVAL,
            "fetch": fetch_api_1,
            "store": store_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-2",
            "interval": HORSE_API_2_INTERVAL,
            "fetch": fetch_api_2,
            "store": store_api2_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-3",
            "interval": HORSE_API_3_INTERVAL,
            "fetch": fetch_api_3,
            "store": store_api3_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-4",
            "interval": HORSE_API_4_INTERVAL,
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

                    # Keep race runner details up to date based on what's stored.
                    # When FETCH_ALL_RACE_IDS=1, do a full backfill only after API-1; after API-2/3/4
                    # do a small refresh (top N ids) so newly inserted races get details quickly.
                    try:
                        if FETCH_ALL_RACE_IDS and task["name"] == "API-1":
                            ids = iter_race_ids(batch_size=RACE_ID_BATCH_SIZE)
                            _fetch_and_store_race_runners_for_races_internal(ids, treat_as_all=True)
                        else:
                            ids = get_race_ids(limit=MAX_RACE_IDS_PER_CYCLE)
                            _fetch_and_store_race_runners_for_races_internal(ids, treat_as_all=False)
                    except Exception as err:
                        log.error("Failed reading race ids from DB: %s", err)
            except Exception as err:
                log.error(f"{task['name']} error: {err}")
            finally:
                task["next_run"] = time.monotonic() + task["interval"]

        next_due = min(t["next_run"] for t in enabled)
        sleep_for = max(0.1, min(1.0, next_due - time.monotonic()))
        time.sleep(sleep_for)


def main():
    ensure_database_and_table()

    log.info(
        "Per-race settings: FETCH_RACE_DETAILS=%s, FETCH_TODAY_MEETING_DATA_BY_ID=%s, FETCH_ALL_RACE_IDS=%s, MAX_RACE_IDS_PER_CYCLE=%s",
        int(FETCH_RACE_DETAILS),
        int(FETCH_TODAY_MEETING_DATA_BY_ID),
        int(FETCH_ALL_RACE_IDS),
        MAX_RACE_IDS_PER_CYCLE,
    )
    if not FETCH_TODAY_MEETING_DATA_BY_ID:
        log.info("TodayMeetingDataById calls are disabled (set FETCH_TODAY_MEETING_DATA_BY_ID=1 to enable).")

    scheduler_mode = any(
        v > 0
        for v in (HORSE_API_1_INTERVAL, HORSE_API_2_INTERVAL, HORSE_API_3_INTERVAL, HORSE_API_4_INTERVAL)
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