
from __future__ import annotations

import json
import logging
import os
from typing import Any

import requests
from dotenv import load_dotenv, find_dotenv

# Ensure we load the repo's .env regardless of current working directory.
load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=True)

log = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _normalize_payload(payload: Any) -> Any:
    if isinstance(payload, list) and len(payload) == 1 and isinstance(payload[0], dict):
        return payload[0]
    return payload


def send_payload_to_backend(payload: Any, *, label: str = "scrap-to-server") -> bool:
    url = (os.getenv("BACKEND_PORT") or "").strip()
    enabled = _env_bool("BACKEND_ENABLED", True)

    if not enabled:
        log.debug("Backend sender disabled via BACKEND_ENABLED=0; skipping %s", label)
        return False

    if not url:
        log.warning("BACKEND_PORT is not set; cannot send %s", label)
        return False

    timeout = _env_int("BACKEND_TIMEOUT_SECONDS", 20)
    verify_ssl_raw = os.getenv("BACKEND_VERIFY_SSL")
    if verify_ssl_raw is None:
        # Local-dev friendly default (self-signed certs are common).
        url_lower = url.lower()
        verify_ssl = not (url_lower.startswith("https://localhost") or url_lower.startswith("https://127.0.0.1"))
    else:
        verify_ssl = _env_bool("BACKEND_VERIFY_SSL", True)

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "BusinessScraperBot/1.0",
    }

    payload = _normalize_payload(payload)

    try:
        body = json.dumps(payload, ensure_ascii=False)
    except TypeError as err:
        log.error("Payload is not JSON-serializable; cannot send %s: %s", label, err)
        return False

    try:
        # Backend endpoint is POST.
        resp = requests.post(url, data=body.encode("utf-8"), headers=headers, timeout=timeout, verify=verify_ssl)

        if 200 <= resp.status_code < 300:
            log.info("Backend %s: sent successfully (%s)", label, resp.status_code)
            return True

        # Include a small tail of response for debugging.
        text = (resp.text or "").strip()
        tail = text[-500:] if len(text) > 500 else text
        log.error("Backend %s: HTTP %s. Response: %s", label, resp.status_code, tail)
        return False

    except requests.RequestException as err:
        log.error("Backend %s: request failed: %s", label, err)
        return False
