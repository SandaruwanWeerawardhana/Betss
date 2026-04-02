"""send_sample_to_backend.py

Manual test: sends the expected backend JSON body to BACKEND_PORT.

Run:
  python -m api.send_sample_to_backend
"""

from __future__ import annotations

from api.backend_sender import send_payload_to_backend


def main() -> None:
    payload = {
        "raceName": "1m 2f Stakes",
        "bettingCenter": "Colombo Race Center",
        "raceDate": "2026-03-31",
        "raceTime": "18:25:00",
        "placeCount": 3,
        "raceEntries": [],
        "raceType": "WIN",
        "isPast": True,
        "scraperId": 101,
        "results": [
            {
                "selection": "(1) Sea Biscuit",
                "place": "1",
                "win_odds": "2.5",
                "place_odds": "1.6",
                "win_odd": "2.5",
                "win_place_odd": "1.6",
            }
        ],
    }

    ok = send_payload_to_backend(payload, label="sample")
    print("sent=", ok)


if __name__ == "__main__":
    main()
