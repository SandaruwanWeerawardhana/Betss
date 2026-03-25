<h1 align="center">Racing API to MySQL Scraper</h1>

A Python project that fetches racing data from an API and stores it in MySQL.

The app:
- Calls an API endpoint and reads JSON records
- Creates the database and tables automatically (if missing)
- Upserts meetings, races, and race runners into MySQL
- Supports single-run mode or polling mode
- Logs to both console and `scraper.log`

## Project Structure

- `main.py` - Entry point. Coordinates fetch and store.
- `api_fetcher.py` - API request logic.
- `horse_racing_db.py` - MySQL schema setup and insert/upsert logic.
- `.env` - Runtime configuration.
- `scraper.log` - Runtime logs.

## Prerequisites

- Python 3.9+
- MySQL Server 8+
- A user with permission to create databases/tables

## Python Dependencies

Install these packages:

```bash
pip install requests python-dotenv mysql-connector-python schedule tzdata
```

Optional `requirements.txt`:

```txt
requests>=2.31.0
python-dotenv>=1.0.0
mysql-connector-python>=8.0.0
schedule>=1.2.0
tzdata>=2024.1
```

## Environment Configuration

Create `.env` in the project root with values like this:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=scraper_db

HORSE_API_1_URL=https://
HORSE_API_2_URL=https://
HORSE_API_3_URL=https://
HORSE_API_4_URL=https://

# 0 = run once and exit
# >0 = polling mode, value is seconds between cycles
POLL_INTERVAL=0
```

Notes:
- Keep `.env` as plain `KEY=VALUE` lines only.
- Do not include markdown fences or shell commands inside `.env`.

## Run the Project

### Windows PowerShell

```powershell
cd d:\Project\Botss
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install requests python-dotenv mysql-connector-python
python .\main.py
```

### Mac / Linux

```bash
cd /path/to/Botss
python3 -m venv .venv
source .venv/bin/activate
pip install requests python-dotenv mysql-connector-python
python main.py
```

## Runtime Modes

`POLL_INTERVAL` controls execution mode:

- `POLL_INTERVAL=0`: single-run mode (fetch/store once, then exit)
- `POLL_INTERVAL=30`: polling mode (run every 30 seconds)

### Per-API call gaps (recommended)

If you want different time gaps for different data types, set `HORSE_API_1_INTERVAL` ... `HORSE_API_4_INTERVAL` (seconds).

When **any** `HORSE_API_*_INTERVAL` is > 0, the app switches to **scheduler mode** and calls each API on its own cadence.

Example:

```env
HORSE_API_1_INTERVAL=15   # live odds/prices
HORSE_API_2_INTERVAL=30   # race status
HORSE_API_3_INTERVAL=300  # meeting/race card
HORSE_API_4_INTERVAL=3600 # historical/reference
```

Recommended starting points:
- Live race odds / prices: 10–15 seconds
- Race status updates: 30 seconds
- Meeting / race card info: 5 minutes
- Results / settled races: 2 minutes
- Historical / reference data: 1 hour (or once daily)

### Time-gated per-race detail calls (+15 minutes)

Per-race endpoints (like `GetRaceRunnersByRace/{raceId}`) are **not** called immediately after featured races are stored.
Instead, the app periodically scans the `races` table and only calls per-race endpoints when:

- The race `start_time` exists, and
- The meeting-local current time is at least `RESULT_FETCH_DELAY_MINUTES` after `start_time`, and
- The race has not already been marked as fetched (`races.results_fetched_at IS NULL`)

Config:

```env
RESULT_FETCH_DELAY_MINUTES=15
RESULT_CHECK_INTERVAL_SECONDS=30
RESULT_CANDIDATE_MAX_ROWS=500
```

## Database Behavior

On startup, `ensure_database_and_table()` will:

1. Create database `DB_NAME` if missing
2. Create these tables if missing:
   - `meetings`
   - `runners`
   - `races`
   - `race_runners`
   - `prices`
   - `results`

Inserts use `ON DUPLICATE KEY UPDATE`, so reruns are safe and rows are updated when IDs already exist.

## Logging

The app logs to:

- Console output
- `scraper.log` file

If something fails, check `scraper.log` first.

## Common Issues

### 1) Import "mysql.connector" could not be resolved

This usually means the VS Code interpreter does not have `mysql-connector-python` installed.

Fix:

```bash
pip install mysql-connector-python
```

Then in VS Code, select the same interpreter:
- Command Palette -> Python: Select Interpreter

### 2) MySQL access denied

- Verify `DB_USER` and `DB_PASSWORD`
- Ensure MySQL is running on `DB_HOST:DB_PORT`
- Ensure user has DB create/table privileges

### 3) API request fails

- Check `HORSE_API_1_URL` ... `HORSE_API_4_URL` in `.env`
- Confirm internet/network access
- Inspect `scraper.log` for HTTP status and stack traces

## How It Works (High Level)

1. `main.py` starts and loads `.env`
2. `horse_racing_db.ensure_database_and_table()` ensures schema exists
3. `api_fetcher.fetch_all()` downloads API payload from up to 4 endpoints
4. `horse_racing_db.store_records()` parses and upserts meetings/races/runners
5. App exits or repeats based on `POLL_INTERVAL`


