"""api_fetcher.py
-----------------
Fetches data from multiple API endpoints.

Each function is independent -- add headers, auth tokens,
or query params per API without affecting the others.
"""

import requests
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# API ENDPOINT URLS  (set these in your .env)
# ─────────────────────────────────────────────
HORSE_API_1_URL = os.getenv("HORSE_API_1_URL", "")
HORSE_API_2_URL = os.getenv("HORSE_API_2_URL", "")
HORSE_API_3_URL = os.getenv("HORSE_API_3_URL", "")
HORSE_API_4_URL = os.getenv("HORSE_API_4_URL", "")
HARNESS_API_1_URL = os.getenv("HARNESS_API_1_URL", os.getenv("HORSE_API_5_URL", ""))

# Per-race detail templates (optional)
RACE_RUNNERS_BY_RACE_URL_TEMPLATE = os.getenv("RACE_RUNNERS_BY_RACE_URL_TEMPLATE", "")
TODAY_MEETING_DATA_BY_ID_URL_TEMPLATE = os.getenv("TODAY_MEETING_DATA_BY_ID_URL_TEMPLATE", "")
RACE_DETAILS_BY_ID_URL_TEMPLATE = os.getenv("RACE_DETAILS_BY_ID_URL_TEMPLATE", "")

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# SHARED HELPER
# ─────────────────────────────────────────────

def _get(url, headers=None, params=None, label="API"):
    """
    Performs a GET request and returns a list of records.
    Wraps a single dict response in a list automatically.
    """
    if not url:
        log.warning(f"{label}: URL is not set. Skipping.")
        return []

    default_headers = {
        "Accept":     "application/json",
        "User-Agent": "BusinessScraperBot/1.0",
    }
    if headers:
        default_headers.update(headers)

    log.info(f"{label}: Fetching from {url}")
    response = requests.get(url, headers=default_headers, params=params, timeout=15)
    response.raise_for_status()

    data = response.json()
    records = data if isinstance(data, list) else [data]
    log.info(f"{label}: Received {len(records)} record(s).")
    return records


def _format_url_template(template: str, race_id: int) -> str:
    if not template:
        return ""
    if "{raceId}" in template:
        return template.format(raceId=race_id)
    return template.rstrip("/") + f"/{race_id}"


# ─────────────────────────────────────────────
# INDIVIDUAL API FETCH FUNCTIONS
# Customise headers, params, or auth per API.
# ─────────────────────────────────────────────

def fetch_api_1():
    """
    Meetings / racing data API.
    Add any required headers or query params below.
    """
    return _get(
        url=HORSE_API_1_URL,
        label="API-1 (Meetings)"
        # headers={"Authorization": "Bearer YOUR_TOKEN"},
        # params={"date": "2026-03-23"}
    )


def fetch_api_2():
    """
    Second API -- update label and params to match your endpoint.
    """
    return _get(
        url=HORSE_API_2_URL,
        label="API-2"
        # headers={"x-api-key": "YOUR_KEY"},
    )


def fetch_api_3():
    """
    Third API -- update label and params to match your endpoint.
    """
    return _get(
        url=HORSE_API_3_URL,
        label="API-3"
    )


def fetch_api_4():
    """
    Fourth API -- update label and params to match your endpoint.
    """
    return _get(
        url=HORSE_API_4_URL,
        label="API-4"
    )


def fetch_api_5():
    """Featured races for Harness (HA) - tomorrow."""
    return _get(
        url=HARNESS_API_1_URL,
        label="API-5 (HA Tomorrow)",
    )


# ─────────────────────────────────────────────
# FETCH ALL
# Returns a dict so main.py knows which data
# belongs to which API without guessing.
# ─────────────────────────────────────────────

def fetch_all():
    return {
        "api_1": fetch_api_1(),
        "api_2": fetch_api_2(),
        "api_3": fetch_api_3(),
        "api_4": fetch_api_4(),
        "api_5": fetch_api_5(),
    }


# ─────────────────────────────────────────────
# PER-RACE DETAIL FETCHERS
# ─────────────────────────────────────────────


def fetch_race_runners_by_race(race_id: int):
    url = _format_url_template(RACE_RUNNERS_BY_RACE_URL_TEMPLATE, race_id)
    return _get(url=url, label=f"API (RaceRunnersByRace {race_id})")


def fetch_today_meeting_data_by_id(race_id: int):
    url = _format_url_template(TODAY_MEETING_DATA_BY_ID_URL_TEMPLATE, race_id)
    return _get(url=url, label=f"API (TodayMeetingDataById {race_id})")


def fetch_race_details_by_id(race_id: int):
    url = _format_url_template(RACE_DETAILS_BY_ID_URL_TEMPLATE, race_id)
    return _get(url=url, label=f"API (RaceDetailsById {race_id})")