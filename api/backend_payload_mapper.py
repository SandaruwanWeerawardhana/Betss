

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


def _as_list(payload: Any) -> list[Any]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    return [payload]


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    # Handle trailing Z.
    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt
    except ValueError:
        return None


@dataclass(frozen=True)
class BackendRaceBody:
    body: dict[str, Any]


def map_payload_to_backend_body(*, race_id: int, payload: Any) -> BackendRaceBody | None:
    """Return a single backend body for the given race_id, or None if not mappable."""

    # If the payload is already in backend shape, just pass through.
    if isinstance(payload, dict) and "raceName" in payload and "results" in payload:
        results = payload.get("results")
        if isinstance(results, list):
            body = dict(payload)
            body["placeCount"] = len(results)
            return BackendRaceBody(body=body)
        return BackendRaceBody(body=payload)

    # Try to locate the race object.
    race_obj: dict[str, Any] | None = None
    meeting_name: str | None = None

    if isinstance(payload, dict):
        # Meeting payload: { races: [...] }
        races = payload.get("races")
        if isinstance(races, list):
            for r in races:
                if not isinstance(r, dict):
                    continue
                rid = _to_int(r.get("id") or r.get("raceId") or r.get("race_id"))
                if rid == int(race_id):
                    race_obj = r
                    break
            meeting_name = _to_str(payload.get("meetingName") or payload.get("name") or payload.get("meeting_name"))

        # Race payload: { id/raceId: ..., raceRunners: [...] }
        if race_obj is None:
            rid = _to_int(payload.get("id") or payload.get("raceId") or payload.get("race_id"))
            if rid == int(race_id) or rid is not None:
                race_obj = payload
            meeting_name = meeting_name or _to_str(payload.get("meetingName") or payload.get("meetingDescription"))

    # Race-runner detail payload: list of race runners for a race.
    rr_items = [x for x in _as_list(payload) if isinstance(x, dict) and _to_int(x.get("raceId") or x.get("race_id")) == int(race_id)]

    if race_obj is None and rr_items:
        # Build a minimal race object from runner detail rows.
        sample = rr_items[0]
        meeting_name = meeting_name or _to_str(sample.get("meetingName") or sample.get("meetingDescription"))
        race_obj = {
            "id": race_id,
            "raceId": race_id,
            "raceName": sample.get("race_id"),
            "raceNumber": sample.get("raceNumber"),
            "startTime": sample.get("startTime") or sample.get("eventDate"),
            "startTimeLocal": sample.get("startTimeLocal"),
            "eachwayPlaces": sample.get("eachwayPlaces"),
            "expectedPlaces": sample.get("expectedPlaces"),
            "raceRunners": rr_items,
        }

    if race_obj is None or not isinstance(race_obj, dict):
        return None

    race_name = _to_str(race_obj.get("raceName"))
    race_number = _to_int(race_obj.get("raceNumber"))
    start_dt_local = _parse_dt(race_obj.get("startTimeLocal"))
    start_dt = start_dt_local or _parse_dt(race_obj.get("startTime") or race_obj.get("offTime"))

    if not race_name:
        if race_number is not None:
            race_name = f"Race {race_number}"
        else:
            race_name = f"Race {race_id}"

    race_date = None
    race_time = None
    if start_dt is not None:
        race_date = start_dt.date().isoformat()
        race_time = start_dt.time().replace(microsecond=0).isoformat()

    place_count = _to_int(race_obj.get("placeCount") or race_obj.get("eachwayPlaces") or race_obj.get("expectedPlaces"))

    is_past = False
    if start_dt_local is not None:
        is_past = start_dt_local.date() < datetime.now().date()

    section = _to_str(race_obj.get("section"))

    betting_center = (
        _to_str(race_obj.get("bettingCenter"))
        or meeting_name
        or _to_str(race_obj.get("meetingName"))
        or ""
    )

    results_out: list[dict[str, Any]] = []

    # Prefer mapping from raceRunners (has runner + prices + result position).
    race_runners = race_obj.get("raceRunners")
    if isinstance(race_runners, list):
        for rr in race_runners:
            if not isinstance(rr, dict):
                continue

            rr_result = rr.get("result") if isinstance(rr.get("result"), dict) else None
            position = None
            if rr_result is not None:
                position = _to_str(rr_result.get("position"))

            # Some endpoints return results[] at the race level, not per runner.
            if position is None:
                continue

            runner = rr.get("runner") if isinstance(rr.get("runner"), dict) else {}
            runner_name = _to_str(runner.get("runnerName") or runner.get("name") or rr.get("runnerName")) or ""
            number = _to_int(rr.get("number") or rr.get("runnerNumber") or rr.get("raceRunnerNumber"))

            selection = runner_name
            if number is not None:
                selection = f"({number}) {runner_name}" if runner_name else f"({number})"

            latest_price = rr.get("latestPrice") if isinstance(rr.get("latestPrice"), dict) else {}
            win_odds = latest_price.get("winValue") or latest_price.get("decimalValue")
            place_odds = latest_price.get("placeValue")

            result_item = {
                "selection": selection,
                "place": position,
                "win_odds": _to_str(win_odds) or "",
                "place_odds": _to_str(place_odds) or "",
                "win_odd": _to_str(win_odds) or "",
                "win_place_odd": _to_str(place_odds) or "",
            }
            results_out.append(result_item)

    # Fallback: race-level results objects.
    if not results_out:
        race_results = race_obj.get("results")
        if isinstance(race_results, list):
            for r in race_results:
                if not isinstance(r, dict):
                    continue
                position = _to_str(r.get("position") or r.get("place"))
                if position is None:
                    continue

                number = _to_int(r.get("runnerNumber") or r.get("raceRunnerNumber"))
                runner_name = _to_str(r.get("runnerName")) or ""
                selection = runner_name
                if number is not None:
                    selection = f"({number}) {runner_name}" if runner_name else f"({number})"

                win_odds = r.get("odd") or r.get("win")
                place_odds = r.get("place")

                results_out.append(
                    {
                        "selection": selection,
                        "place": position,
                        "win_odds": _to_str(win_odds) or "",
                        "place_odds": _to_str(place_odds) or "",
                        "win_odd": _to_str(win_odds) or "",
                        "win_place_odd": _to_str(place_odds) or "",
                        "section": section,
                    }
                )

    body: dict[str, Any] = {
        "id": race_name,
        "raceName": race_name,
        "bettingCenter": betting_center,
        "raceDate": race_date or "",
        "raceTime": race_time or "",
        "placeCount": len(results_out),
        "raceType": section,
        "isPast": bool(is_past),
        "results": results_out,
    }

    return BackendRaceBody(body=body)
