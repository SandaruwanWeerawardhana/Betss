"""
db_store.py
-----------
Handles MySQL setup and storage for the racing API response.
Three tables are created automatically:
    - meetings
    - races
    - race_runners
"""

import mysql.connector
import logging
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER",     "root"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
}

DATABASE_NAME = os.getenv("DB_NAME", "scraper_db")

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# TABLE DEFINITIONS
# ─────────────────────────────────────────────

CREATE_DB_SQL = f"""
    CREATE DATABASE IF NOT EXISTS `{DATABASE_NAME}`
    CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

CREATE_MEETINGS_TABLE = """
CREATE TABLE IF NOT EXISTS `meetings` (
    `id`                BIGINT          NOT NULL PRIMARY KEY,
    `meeting_name`      VARCHAR(255)    NULL,
    `code`              VARCHAR(50)     NULL,
    `country_code`      VARCHAR(10)     NULL,
    `coverage_code`     VARCHAR(10)     NULL,
    `sport_code`        VARCHAR(20)     NULL,
    `category`          VARCHAR(20)     NULL,
    `sub_code`          VARCHAR(10)     NULL,
    `date`              DATETIME        NULL,
    `going`             VARCHAR(20)     NULL,
    `is_evening`        TINYINT(1)      DEFAULT 0,
    `is_deleted`        TINYINT(1)      DEFAULT 0,
    `no_of_events`      INT             DEFAULT 0,
    `fetched_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_meeting_date` (`date`),
    INDEX `idx_country`      (`country_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_RACES_TABLE = """
CREATE TABLE IF NOT EXISTS `races` (
    `id`                BIGINT          NOT NULL PRIMARY KEY,
    `meeting_id`        BIGINT          NOT NULL,
    `race_name`         VARCHAR(500)    NULL,
    `race_number`       INT             NULL,
    `start_time`        DATETIME        NULL,
    `start_time_utc`    DATETIME        NULL,
    `start_time_local`  VARCHAR(50)     NULL,
    `distance`          VARCHAR(20)     NULL,
    `no_of_runners`     INT             DEFAULT 0,
    `eachway_places`    INT             DEFAULT 0,
    `expected_places`   INT             DEFAULT 0,
    `is_handicap`       TINYINT(1)      DEFAULT 0,
    `off_time`          DATETIME        NULL,
    `status`            VARCHAR(10)     NULL,
    `surface`           VARCHAR(50)     NULL,
    `progress_code`     VARCHAR(10)     NULL,
    `progress_message`  VARCHAR(100)    NULL,
    `broadcast_channel` VARCHAR(100)    NULL,
    `is_quinella`       TINYINT(1)      DEFAULT 0,
    `is_exacta`         TINYINT(1)      DEFAULT 0,
    `is_trifecta`       TINYINT(1)      DEFAULT 0,
    `is_first_four`     TINYINT(1)      DEFAULT 0,
    `is_settled`        TINYINT(1)      DEFAULT 0,
    `is_deleted`        TINYINT(1)      DEFAULT 0,
    `source`            INT             NULL,
    `channel`           INT             NULL,
    `fetched_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_meeting_id`   (`meeting_id`),
    INDEX `idx_race_status`  (`status`),
    INDEX `idx_start_time`   (`start_time`),
    FOREIGN KEY (`meeting_id`) REFERENCES `meetings`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_RACE_RUNNERS_TABLE = """
CREATE TABLE IF NOT EXISTS `race_runners` (
    `id`                BIGINT          NOT NULL PRIMARY KEY,
    `race_id`           BIGINT          NOT NULL,
    `runner_id`         BIGINT          NULL,
    `number`            INT             NULL,
    `drawn`             INT             NULL,
    `is_non_runner`     TINYINT(1)      DEFAULT 0,
    `is_withdrawn`      TINYINT(1)      DEFAULT 0,
    `latest_price_id`   BIGINT          NULL,
    `weight_pounds`     DECIMAL(8,2)    DEFAULT 0.00,
    `weight_stones`     DECIMAL(8,2)    DEFAULT 0.00,
    `runner_name`       VARCHAR(255)    NULL,
    `jockey`            VARCHAR(255)    NULL,
    `trainer`           VARCHAR(255)    NULL,
    `silk_url`          TEXT            NULL,
    `last_runs`         VARCHAR(100)    NULL,
    `age`               INT             DEFAULT 0,
    `decimal_value`     DECIMAL(10,2)   DEFAULT 0.00,
    `win_value`         DECIMAL(10,2)   DEFAULT 0.00,
    `place_value`       DECIMAL(10,2)   DEFAULT 0.00,
    `win_pool`          DECIMAL(12,2)   DEFAULT 0.00,
    `place_pool`        DECIMAL(12,2)   DEFAULT 0.00,
    `open_decimal`      DECIMAL(10,2)   DEFAULT 0.00,
    `fluc1`             DECIMAL(10,2)   DEFAULT 0.00,
    `fluc2`             DECIMAL(10,2)   DEFAULT 0.00,
    `race_source`       INT             NULL,
    `raw_json`          JSON            NULL,
    `fetched_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_race_id`      (`race_id`),
    INDEX `idx_runner_id`    (`runner_id`),
    INDEX `idx_non_runner`   (`is_non_runner`),
    FOREIGN KEY (`race_id`) REFERENCES `races`(`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ─────────────────────────────────────────────
# INSERT STATEMENTS
# ─────────────────────────────────────────────

INSERT_MEETING_SQL = """
INSERT INTO `meetings`
    (`id`, `meeting_name`, `code`, `country_code`, `coverage_code`,
     `sport_code`, `category`, `sub_code`, `date`, `going`,
     `is_evening`, `is_deleted`, `no_of_events`, `fetched_at`)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    `meeting_name`  = VALUES(`meeting_name`),
    `going`         = VALUES(`going`),
    `no_of_events`  = VALUES(`no_of_events`),
    `is_deleted`    = VALUES(`is_deleted`),
    `fetched_at`    = VALUES(`fetched_at`);
"""

INSERT_RACE_SQL = """
INSERT INTO `races`
    (`id`, `meeting_id`, `race_name`, `race_number`, `start_time`,
     `start_time_utc`, `start_time_local`, `distance`, `no_of_runners`,
     `eachway_places`, `expected_places`, `is_handicap`, `off_time`,
     `status`, `surface`, `progress_code`, `progress_message`,
     `broadcast_channel`, `is_quinella`, `is_exacta`, `is_trifecta`,
     `is_first_four`, `is_settled`, `is_deleted`, `source`, `channel`, `fetched_at`)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    `race_name`         = VALUES(`race_name`),
    `status`            = VALUES(`status`),
    `progress_code`     = VALUES(`progress_code`),
    `progress_message`  = VALUES(`progress_message`),
    `no_of_runners`     = VALUES(`no_of_runners`),
    `off_time`          = VALUES(`off_time`),
    `is_settled`        = VALUES(`is_settled`),
    `fetched_at`        = VALUES(`fetched_at`);
"""

INSERT_RUNNER_SQL = """
INSERT INTO `race_runners`
    (`id`, `race_id`, `runner_id`, `number`, `drawn`, `is_non_runner`,
     `is_withdrawn`, `latest_price_id`, `weight_pounds`, `weight_stones`,
     `runner_name`, `jockey`, `trainer`, `silk_url`, `last_runs`, `age`,
     `decimal_value`, `win_value`, `place_value`, `win_pool`, `place_pool`,
     `open_decimal`, `fluc1`, `fluc2`, `race_source`, `raw_json`, `fetched_at`)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    `is_non_runner`  = VALUES(`is_non_runner`),
    `is_withdrawn`   = VALUES(`is_withdrawn`),
    `decimal_value`  = VALUES(`decimal_value`),
    `win_value`      = VALUES(`win_value`),
    `place_value`    = VALUES(`place_value`),
    `win_pool`       = VALUES(`win_pool`),
    `place_pool`     = VALUES(`place_pool`),
    `fluc1`          = VALUES(`fluc1`),
    `fluc2`          = VALUES(`fluc2`),
    `raw_json`       = VALUES(`raw_json`),
    `fetched_at`     = VALUES(`fetched_at`);
"""


# ─────────────────────────────────────────────
# CONNECTION HELPER
# ─────────────────────────────────────────────

def get_connection(with_db=True):
    config = DB_CONFIG.copy()
    if with_db:
        config["database"] = DATABASE_NAME
    return mysql.connector.connect(**config)


# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────

def ensure_database_and_table():
    """
    Creates the database and all three tables if they do not already exist.
    Safe to call on every programme start.
    """
    try:
        conn = get_connection(with_db=False)
        cursor = conn.cursor()

        cursor.execute(CREATE_DB_SQL)
        log.info(f"Database `{DATABASE_NAME}` is ready.")

        cursor.execute(f"USE `{DATABASE_NAME}`;")
        cursor.execute(CREATE_MEETINGS_TABLE)
        log.info("Table `meetings` is ready.")

        cursor.execute(CREATE_RACES_TABLE)
        log.info("Table `races` is ready.")

        cursor.execute(CREATE_RACE_RUNNERS_TABLE)
        log.info("Table `race_runners` is ready.")

        cursor.close()
        conn.close()

    except mysql.connector.Error as err:
        log.error(f"Database setup failed: {err}")
        raise


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def _parse_dt(value):
    """
    Converts an ISO datetime string to a Python datetime.
    Returns None for missing values or the placeholder date 0001-01-01.
    """
    if not value or value.startswith("0001"):
        return None
    try:
        return datetime.fromisoformat(value[:19])
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────
# STORE
# ─────────────────────────────────────────────

def store_records(records):
    """
    Accepts a list of meeting objects from the API.
    Iterates meetings -> races -> runners and stores each level.
    Uses ON DUPLICATE KEY UPDATE so re-running is always safe.
    """
    if not records:
        log.warning("No records received. Nothing stored.")
        return

    conn = get_connection()
    cursor = conn.cursor()
    fetched_at = datetime.utcnow()

    total_meetings = total_races = total_runners = 0

    try:
        for meeting in records:

            # ── MEETING ──────────────────────────────────
            cursor.execute(INSERT_MEETING_SQL, (
                meeting.get("id"),
                meeting.get("meetingName"),
                meeting.get("code"),
                meeting.get("countryCode"),
                meeting.get("coverageCode"),
                meeting.get("sportCode"),
                meeting.get("category"),
                meeting.get("subCode"),
                _parse_dt(meeting.get("date")),
                meeting.get("going"),
                int(meeting.get("isEveningMeeting", False)),
                int(meeting.get("isDeleted", False)),
                meeting.get("noOfEvents", 0),
                fetched_at
            ))
            total_meetings += 1

            # ── RACES ─────────────────────────────────────
            for race in meeting.get("races", []):

                cursor.execute(INSERT_RACE_SQL, (
                    race.get("id"),
                    meeting.get("id"),
                    race.get("raceName"),
                    race.get("raceNumber"),
                    _parse_dt(race.get("startTime")),
                    _parse_dt(race.get("startTimeUtc")),
                    race.get("startTimeLocal"),
                    race.get("distance"),
                    race.get("noOfRunners", 0),
                    race.get("eachwayPlaces", 0),
                    race.get("expectedPlaces", 0),
                    int(race.get("isHandiCap", False)),
                    _parse_dt(race.get("offTime")),
                    race.get("status"),
                    race.get("surface"),
                    race.get("progressCode"),
                    race.get("progressMessage"),
                    race.get("broadcastChannel"),
                    int(race.get("isQuinella", False)),
                    int(race.get("isExacta", False)),
                    int(race.get("isTrifecta", False)),
                    int(race.get("isFirstFour", False)),
                    int(race.get("isSettled", False)),
                    int(race.get("isDeleted", False)),
                    race.get("source"),
                    race.get("channel"),
                    fetched_at
                ))
                total_races += 1

                # ── RUNNERS ───────────────────────────────
                for rr in race.get("raceRunners", []):
                    runner       = rr.get("runner") or {}
                    latest_price = rr.get("latestPrice") or {}

                    cursor.execute(INSERT_RUNNER_SQL, (
                        rr.get("id"),
                        race.get("id"),
                        rr.get("runnerId"),
                        rr.get("number"),
                        rr.get("drawn"),
                        int(rr.get("isNonRunner", False)),
                        int(rr.get("isWithdrawn", False)),
                        rr.get("latestPriceId"),
                        rr.get("weightPounds", 0.0),
                        rr.get("weightStones", 0.0),
                        runner.get("runnerName"),
                        runner.get("jockey"),
                        runner.get("trainer"),
                        runner.get("silk"),
                        runner.get("lastRuns"),
                        runner.get("age", 0),
                        latest_price.get("decimalValue", 0.0),
                        latest_price.get("winValue", 0.0),
                        latest_price.get("placeValue", 0.0),
                        latest_price.get("winPool", 0.0),
                        latest_price.get("placePool", 0.0),
                        rr.get("openDecimal", 0.0),
                        rr.get("fluc1", 0.0),
                        rr.get("fluc2", 0.0),
                        rr.get("raceSource"),
                        json.dumps(rr),
                        fetched_at
                    ))
                    total_runners += 1

        conn.commit()
        log.info(
            f"Stored: {total_meetings} meeting(s), "
            f"{total_races} race(s), "
            f"{total_runners} runner(s)."
        )

    except mysql.connector.Error as err:
        conn.rollback()
        log.error(f"Insert failed: {err}")
        raise

    finally:
        cursor.close()
        conn.close()
