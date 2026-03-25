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
    fetch_api_5,
    fetch_api_6,
    fetch_api_7,
    fetch_all,
    fetch_race_runners_by_race,
    fetch_race_details_by_id,
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
FETCH_RACE_DETAILS_BY_ID = os.getenv("FETCH_RACE_DETAILS_BY_ID", "0").strip().lower() in (
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
HAENESS_API_1_INTERVAL = int(os.getenv("HAENESS_API_1_INTERVAL", 0))
HARNESS_API_2_INTERVAL = int(os.getenv("HARNESS_API_2_INTERVAL", 0))
HARNESS_API_3_INTERVAL = int(os.getenv("HARNESS_API_3_INTERVAL", 0))



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
    if not FETCH_RACE_DETAILS and not FETCH_TODAY_MEETING_DATA_BY_ID and not FETCH_RACE_DETAILS_BY_ID:
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

    enabled_fetchers = []
    if FETCH_TODAY_MEETING_DATA_BY_ID:
        enabled_fetchers.append(("today_meeting", fetch_today_meeting_data_by_id))
    if FETCH_RACE_DETAILS:
        enabled_fetchers.append(("race_runners", fetch_race_runners_by_race))
    if FETCH_RACE_DETAILS_BY_ID:
        enabled_fetchers.append(("race_details", fetch_race_details_by_id))

    executor_mode = "parallel" if len(enabled_fetchers) > 1 else "sequential"
    if len(enabled_fetchers) > 0:
        endpoint_names = ", ".join([name for name, _ in enabled_fetchers])
        log.info("Per-race mode: %s execution of [%s]", executor_mode, endpoint_names)

    executor = None
    if len(enabled_fetchers) > 1:
        executor = ThreadPoolExecutor(max_workers=len(enabled_fetchers))

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

        payloads: dict[str, object] = {}

        if executor is not None:
            log.info("Race %s: fetching %s endpoints in parallel", race_id, len(enabled_fetchers))
            futures = {name: executor.submit(fn, race_id) for (name, fn) in enabled_fetchers}
            for name, fut in futures.items():
                try:
                    payloads[name] = fut.result()
                    log.debug("Race %s: %s fetch completed", race_id, name)
                except Exception as err:
                    log.error("Per-race fetch failed (%s) for race_id=%s: %s", name, race_id, err)
        else:
            for name, fn in enabled_fetchers:
                try:
                    payloads[name] = fn(race_id)
                    log.debug("Race %s: %s fetch completed", race_id, name)
                except Exception as err:
                    log.error("Per-race fetch failed (%s) for race_id=%s: %s", name, race_id, err)

        try:
            stored_count = 0
            for name in ("today_meeting", "race_details", "race_runners"):
                payload = payloads.get(name)
                if payload:
                    store_records(payload)
                    stored_count += 1
            if stored_count > 0:
                log.info("Race %s: stored %s payload(s)", race_id, stored_count)
        except Exception as err:
            log.error("Per-race store failed for race_id=%s: %s", race_id, err)

    if executor is not None:
        executor.shutdown(wait=True)

    log.info("Per-race detail fetch complete: processed %s race(s).", count)


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

    if results.get("api_5"):
        store_records(results["api_5"])

    if results.get("api_6"):
        store_records(results["api_6"])

    if results.get("api_7"):
        store_records(results["api_7"])

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
        {
            "name": "API-5 (HA Tomorrow)",
            "interval": HAENESS_API_1_INTERVAL,
            "fetch": fetch_api_5,
            "store": store_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-6 (HA Today)",
            "interval": HARNESS_API_2_INTERVAL,
            "fetch": fetch_api_6,
            "store": store_records,
            "next_run": time.monotonic(),
        },
        {
            "name": "API-7 (HA Future)",
            "interval": HARNESS_API_3_INTERVAL,
            "fetch": fetch_api_7,
            "store": store_records,
            "next_run": time.monotonic(),
        },
    ]

    enabled = [t for t in tasks if t["interval"] > 0]
    if not enabled:
        log.info("Scheduler enabled, but no API interval is > 0. Nothing to do.")
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
        "Per-race settings: FETCH_RACE_DETAILS=%s, FETCH_TODAY_MEETING_DATA_BY_ID=%s, FETCH_RACE_DETAILS_BY_ID=%s, FETCH_ALL_RACE_IDS=%s, MAX_RACE_IDS_PER_CYCLE=%s",
        int(FETCH_RACE_DETAILS),
        int(FETCH_TODAY_MEETING_DATA_BY_ID),
        int(FETCH_RACE_DETAILS_BY_ID),
        int(FETCH_ALL_RACE_IDS),
        MAX_RACE_IDS_PER_CYCLE,
    )
    if not FETCH_TODAY_MEETING_DATA_BY_ID:
        log.info("TodayMeetingDataById calls are disabled (set FETCH_TODAY_MEETING_DATA_BY_ID=1 to enable).")
    if not FETCH_RACE_DETAILS_BY_ID:
        log.info("RaceDetailsById calls are disabled (set FETCH_RACE_DETAILS_BY_ID=1 to enable).")

    scheduler_mode = any(
        v > 0
        for v in (
            HORSE_API_1_INTERVAL,
            HORSE_API_2_INTERVAL,
            HORSE_API_3_INTERVAL,
            HORSE_API_4_INTERVAL,
            HAENESS_API_1_INTERVAL,
            HARNESS_API_2_INTERVAL,
            HARNESS_API_3_INTERVAL,
        )
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