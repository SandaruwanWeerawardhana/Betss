"""
main.py
-------
Entry point. Fetches all 4 APIs and routes each result
to its own store function in horse_racing_db.py.
"""

import logging
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import schedule
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
    fetch_api_8,
    fetch_api_9,
    fetch_api_10,
    fetch_api_11,
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
    get_candidate_races_for_results,
    mark_race_results_fetched,
    set_race_results_fetch_error,
)

load_dotenv(override=True)

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


HORSE_API_1_INTERVAL = int(os.getenv("HORSE_API_1_INTERVAL", 0))
HORSE_API_2_INTERVAL = int(os.getenv("HORSE_API_2_INTERVAL", 0))
HORSE_API_3_INTERVAL = int(os.getenv("HORSE_API_3_INTERVAL", 0))
HORSE_API_4_INTERVAL = int(os.getenv("HORSE_API_4_INTERVAL", 0))
HARNESS_API_1_INTERVAL = int(os.getenv("HARNESS_API_1_INTERVAL", 0))
HARNESS_API_2_INTERVAL = int(os.getenv("HARNESS_API_2_INTERVAL", 0))
HARNESS_API_3_INTERVAL = int(os.getenv("HARNESS_API_3_INTERVAL", 0))
GRAYHOUND_API_1_INTERVAL = int(os.getenv("GRAYHOUND_API_1_INTERVAL", 0))
GRAYHOUND_API_2_INTERVAL = int(os.getenv("GRAYHOUND_API_2_INTERVAL", 0))
GRAYHOUND_API_3_INTERVAL = int(os.getenv("GRAYHOUND_API_3_INTERVAL", 0))

VHORSE_API_1_INTERVAL = int(os.getenv("VHORSE_API_1_INTERVAL", 0))

# Per-race timing controls
RESULT_FETCH_DELAY_MINUTES = int(os.getenv("RESULT_FETCH_DELAY_MINUTES", 15))
RESULT_CHECK_INTERVAL_SECONDS = int(os.getenv("RESULT_CHECK_INTERVAL_SECONDS", 30))
RESULT_CANDIDATE_MAX_ROWS = int(os.getenv("RESULT_CANDIDATE_MAX_ROWS", 500))



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger(__name__)

RACES_SECTION_RACING = "RACING"
RACES_SECTION_VIRTUAL = "VIRTUAL"


def _fetch_and_store_race_runners_for_races(race_ids):
    return _fetch_and_store_race_runners_for_races_internal(race_ids)


def _fetch_and_store_race_runners_for_races_internal(
    race_ids,
    *,
    treat_as_all: bool | None = None,
    mark_fetched: bool = False,
):
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
        errors: list[str] = []
        had_results = False

        def _payload_has_results(payload: object) -> bool:
            if not payload:
                return False
            if isinstance(payload, dict):
                payload = [payload]
            if not isinstance(payload, list):
                return False

            for item in payload:
                if not isinstance(item, dict):
                    continue

                # Race detail payloads may include results[] or isSettled.
                if isinstance(item.get("results"), list) and item.get("results"):
                    return True
                if bool(item.get("isSettled")) is True:
                    return True

                # Meeting payloads contain races[] with results.
                races = item.get("races")
                if isinstance(races, list):
                    for race in races:
                        if not isinstance(race, dict):
                            continue
                        if isinstance(race.get("results"), list) and race.get("results"):
                            return True
                        if bool(race.get("isSettled")) is True:
                            return True

                # Race runner detail payloads contain result objects per runner.
                if isinstance(item.get("result"), dict) and item.get("result"):
                    pos = item["result"].get("position")
                    if pos is not None:
                        return True

            return False

        if executor is not None:
            log.info("Race %s: fetching %s endpoints in parallel", race_id, len(enabled_fetchers))
            futures = {name: executor.submit(fn, race_id) for (name, fn) in enabled_fetchers}
            for name, fut in futures.items():
                try:
                    payloads[name] = fut.result()
                    log.debug("Race %s: %s fetch completed", race_id, name)
                    had_results = had_results or _payload_has_results(payloads[name])
                except Exception as err:
                    log.error("Per-race fetch failed (%s) for race_id=%s: %s", name, race_id, err)
                    errors.append(f"fetch:{name}:{err}")
        else:
            for name, fn in enabled_fetchers:
                try:
                    payloads[name] = fn(race_id)
                    log.debug("Race %s: %s fetch completed", race_id, name)
                    had_results = had_results or _payload_has_results(payloads[name])
                except Exception as err:
                    log.error("Per-race fetch failed (%s) for race_id=%s: %s", name, race_id, err)
                    errors.append(f"fetch:{name}:{err}")

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
            errors.append(f"store:{err}")

        if mark_fetched:
            err_msg = "; ".join(errors) if errors else None
            try:
                if had_results:
                    mark_race_results_fetched(race_id, error=err_msg)
                elif err_msg:
                    set_race_results_fetch_error(race_id, error=err_msg)
            except Exception as err:
                log.error("Failed updating results fetch status for race_id=%s: %s", race_id, err)

    if executor is not None:
        executor.shutdown(wait=True)

    log.info("Per-race detail fetch complete: processed %s race(s).", count)


def run_once():
    results = fetch_all()

    if results["api_1"]:
        store_records(results["api_1"], section=RACES_SECTION_RACING)

    if results["api_2"]:
        store_api2_records(results["api_2"], section=RACES_SECTION_RACING)

    if results["api_3"]:
        store_api3_records(results["api_3"], section=RACES_SECTION_RACING)

    if results["api_4"]:
        store_api4_records(results["api_4"], section=RACES_SECTION_RACING)

    if results.get("api_5"):
        store_records(results["api_5"], section=RACES_SECTION_RACING)

    if results.get("api_6"):
        store_records(results["api_6"], section=RACES_SECTION_RACING)

    if results.get("api_7"):
        store_records(results["api_7"], section=RACES_SECTION_RACING)

    if results.get("api_8"):
        store_records(results["api_8"], section=RACES_SECTION_RACING)

    if results.get("api_9"):
        store_records(results["api_9"], section=RACES_SECTION_RACING)

    if results.get("api_10"):
        store_records(results["api_10"], section=RACES_SECTION_RACING)

    if results.get("api_11"):
        store_records(results["api_11"], section=RACES_SECTION_VIRTUAL)


def _timezone_for_country_code(country_code: str | None) -> str:
    if not country_code:
        return "UTC"
    code = str(country_code).strip().upper()
    mapping = {
        "GB": "Europe/London",
        "GBR": "Europe/London",
        "UK": "Europe/London",
        "IE": "Europe/Dublin",
        "IRE": "Europe/Dublin",
        "IRL": "Europe/Dublin",
        "FR": "Europe/Paris",
        "FRA": "Europe/Paris",
        "AU": "Australia/Sydney",
        "AUS": "Australia/Sydney",
        "NZ": "Pacific/Auckland",
        "NZL": "Pacific/Auckland",
        "US": "America/New_York",
        "USA": "America/New_York",
    }
    return mapping.get(code, "UTC")


def fetch_due_race_results_cycle():
    if not FETCH_RACE_DETAILS and not FETCH_TODAY_MEETING_DATA_BY_ID and not FETCH_RACE_DETAILS_BY_ID:
        return

    try:
        candidates = get_candidate_races_for_results(limit=RESULT_CANDIDATE_MAX_ROWS)
    except Exception as err:
        log.error("Failed selecting candidate races for results: %s", err)
        return

    if not candidates:
        return

    due_race_ids: list[int] = []
    delay = timedelta(minutes=max(0, RESULT_FETCH_DELAY_MINUTES))

    for row in candidates:
        race_id = row.get("race_id")
        start_time = row.get("start_time")
        country_code = row.get("country_code")

        if race_id is None or start_time is None:
            continue

        tz_name = _timezone_for_country_code(country_code)
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = ZoneInfo("UTC")

        # We store start_time in DB as a local time for that meeting.
        # Compare it to "now" in the same local timezone.
        start_local = start_time.replace(tzinfo=tz)
        now_local = datetime.now(tz)
        if now_local >= (start_local + delay):
            try:
                due_race_ids.append(int(race_id))
            except (TypeError, ValueError):
                continue

    if not due_race_ids:
        return

    log.info("Per-race due cycle: %s race(s) due (delay=%s min)", len(due_race_ids), RESULT_FETCH_DELAY_MINUTES)
    _fetch_and_store_race_runners_for_races_internal(due_race_ids, treat_as_all=True, mark_fetched=True)


def _run_scheduler():
    def _job(name, fetch_fn, store_fn):
        try:
            data = fetch_fn()
            if data:
                store_fn(data)
        except Exception as err:
            log.error("%s error: %s", name, err)

    tasks = [
        ("API-1", HORSE_API_1_INTERVAL, fetch_api_1, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-2", HORSE_API_2_INTERVAL, fetch_api_2, lambda data: store_api2_records(data, section=RACES_SECTION_RACING)),
        ("API-3", HORSE_API_3_INTERVAL, fetch_api_3, lambda data: store_api3_records(data, section=RACES_SECTION_RACING)),
        ("API-4", HORSE_API_4_INTERVAL, fetch_api_4, lambda data: store_api4_records(data, section=RACES_SECTION_RACING)),
        ("API-5 (HA Tomorrow)", HARNESS_API_1_INTERVAL, fetch_api_5, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-6 (HA Today)", HARNESS_API_2_INTERVAL, fetch_api_6, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-7 (HA Future)", HARNESS_API_3_INTERVAL, fetch_api_7, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-8 (DG Today)", GRAYHOUND_API_1_INTERVAL, fetch_api_8, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-9 (DG Tomorrow)", GRAYHOUND_API_2_INTERVAL, fetch_api_9, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-10 (DG Future)", GRAYHOUND_API_3_INTERVAL, fetch_api_10, lambda data: store_records(data, section=RACES_SECTION_RACING)),
        ("API-11 (VHR Today)", VHORSE_API_1_INTERVAL, fetch_api_11, lambda data: store_records(data, section=RACES_SECTION_RACING)),
    ]

    enabled = [t for t in tasks if t[1] and t[1] > 0]
    if not enabled:
        log.info("Scheduler enabled, but no API interval is > 0. Nothing to do.")
        return

    schedule.clear()
    for (name, interval, fetch_fn, store_fn) in enabled:
        schedule.every(interval).seconds.do(_job, name, fetch_fn, store_fn)

    # Periodically check for races that are due for per-race detail calls.
    if RESULT_CHECK_INTERVAL_SECONDS > 0:
        schedule.every(RESULT_CHECK_INTERVAL_SECONDS).seconds.do(fetch_due_race_results_cycle)

    summary = ", ".join([f"{t[0]}={t[1]}s" for t in enabled])
    log.info("Scheduler mode: %s. Due-check=%ss, delay=%s min. Press Ctrl+C to stop.", summary, RESULT_CHECK_INTERVAL_SECONDS, RESULT_FETCH_DELAY_MINUTES)

    # Seed featured data once immediately so races exist for the due-check.
    try:
        run_once()
    except Exception as err:
        log.error("Startup featured fetch failed: %s", err)

    while True:
        try:
            schedule.run_pending()
        except Exception as err:
            log.error("Scheduler loop error: %s", err)
        time.sleep(1)


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
            HARNESS_API_1_INTERVAL,
            HARNESS_API_2_INTERVAL,
            HARNESS_API_3_INTERVAL,
            GRAYHOUND_API_1_INTERVAL,
            GRAYHOUND_API_2_INTERVAL,
            GRAYHOUND_API_3_INTERVAL,
            VHORSE_API_1_INTERVAL,
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
                fetch_due_race_results_cycle()
            except Exception as err:
                log.error(f"Cycle error: {err}")
            time.sleep(POLL_INTERVAL)
    else:
        log.info("Single-run mode.")
        run_once()
        fetch_due_race_results_cycle()


if __name__ == "__main__":
    main()