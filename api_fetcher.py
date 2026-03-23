"""
api_fetcher.py
--------------
Fetches data from four separate API endpoints.
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
API_1_URL = os.getenv("API_1_URL", "")
API_2_URL = os.getenv("API_2_URL", "")
API_3_URL = os.getenv("API_3_URL", "")
API_4_URL = os.getenv("API_4_URL", "")

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
        url=API_1_URL,
        label="API-1 (Meetings)"
        # headers={"Authorization": "Bearer YOUR_TOKEN"},
        # params={"date": "2026-03-23"}
    )


def fetch_api_2():
    """
    Second API -- update label and params to match your endpoint.
    """
    return _get(
        url=API_2_URL,
        label="API-2"
        # headers={"x-api-key": "YOUR_KEY"},
    )


def fetch_api_3():
    """
    Third API -- update label and params to match your endpoint.
    """
    return _get(
        url=API_3_URL,
        label="API-3"
    )


def fetch_api_4():
    """
    Fourth API -- update label and params to match your endpoint.
    """
    return _get(
        url=API_4_URL,
        label="API-4"
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
    }