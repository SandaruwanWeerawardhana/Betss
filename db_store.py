"""db_store.py
--------------
MySQL schema setup and storage.

This file implements the table design you provided:
- meetings
- runners
- races
- race_runners
- prices
- results

The API payload is expected to look like:
meetings[] -> races[] -> raceRunners[] with nested runner/latestPrice objects.
"""

import logging
import os
from datetime import date, datetime

import mysql.connector
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "your_password"),
}

DATABASE_NAME = os.getenv("DB_NAME", "scraper_db")


# ─────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────

CREATE_DB_SQL = f"""
CREATE DATABASE IF NOT EXISTS `{DATABASE_NAME}`
CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""


CREATE_MEETINGS_TABLE = """
CREATE TABLE IF NOT EXISTS meetings (
    id            INT             NOT NULL,
    meeting_name  VARCHAR(100)    NOT NULL,
    code          VARCHAR(20)     NULL,
    country_code  CHAR(3)         NULL,
    coverage_code VARCHAR(10)     NULL,
    sport_code    VARCHAR(20)     NULL,
    category      VARCHAR(10)     NOT NULL DEFAULT 'HR',
    sub_code      VARCHAR(10)     NULL,
    date          DATE            NULL,
    going         VARCHAR(20)     NULL,
    is_evening_meeting TINYINT(1) NOT NULL DEFAULT 0,
    is_deleted    TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


CREATE_RUNNERS_TABLE = """
CREATE TABLE IF NOT EXISTS runners (
    id            INT             NOT NULL,
    runner_id     VARCHAR(50)     NULL,
    runner_name   VARCHAR(100)    NOT NULL,
    jockey        VARCHAR(100)    NULL,
    trainer       VARCHAR(100)    NULL,
    silk          VARCHAR(255)    NULL,
    silk_file     VARCHAR(255)    NULL,
    runner_status CHAR(2)         NULL,
    age           TINYINT         NULL,
    last_runs     VARCHAR(30)     NULL,
    claiming      VARCHAR(10)     NULL,
    owner         VARCHAR(100)    NULL,
    horse_farm    VARCHAR(100)    NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


CREATE_RACES_TABLE = """
CREATE TABLE IF NOT EXISTS races (
    id                INT             NOT NULL,
    meeting_id        INT             NOT NULL,
    race_id           VARCHAR(50)     NULL,
    race_name         VARCHAR(150)    NULL,
    race_number       TINYINT         NULL,
    start_time        DATETIME        NULL,
    start_time_utc    DATETIME        NULL,
    course_type       CHAR(2)         NULL,
    distance          VARCHAR(30)     NULL,
    no_of_runners     TINYINT         NULL,
    eachway_places    TINYINT         NULL,
    expected_places   TINYINT         NULL,
    is_handicap       TINYINT(1)      NOT NULL DEFAULT 0,
    off_time          DATETIME        NULL,
    off_time_utc      DATETIME        NULL,
    status            CHAR(2)         NULL,
    surface           CHAR(2)         NULL,
    progress_code     VARCHAR(10)     NULL,
    progress_message  VARCHAR(100)    NULL,
    place_config_id   INT             NULL,
    source            TINYINT         NULL,
    channel           TINYINT         NULL,
    broadcast_channel VARCHAR(50)     NULL,
    is_quinella       TINYINT(1)      NOT NULL DEFAULT 0,
    is_exacta         TINYINT(1)      NOT NULL DEFAULT 0,
    is_trifecta       TINYINT(1)      NOT NULL DEFAULT 0,
    is_first_four     TINYINT(1)      NOT NULL DEFAULT 0,
    is_forecast       TINYINT(1)      NOT NULL DEFAULT 0,
    is_tricast        TINYINT(1)      NOT NULL DEFAULT 0,
    is_trio           TINYINT(1)      NOT NULL DEFAULT 0,
    is_settled        TINYINT(1)      NOT NULL DEFAULT 0,
    is_deleted        TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    CONSTRAINT fk_race_meeting FOREIGN KEY (meeting_id) REFERENCES meetings (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


CREATE_RACE_RUNNERS_TABLE = """
CREATE TABLE IF NOT EXISTS race_runners (
    id              INT             NOT NULL,
    race_id         INT             NOT NULL,
    runner_id       INT             NOT NULL,
    number          TINYINT         NULL,
    drawn           TINYINT         NULL,
    is_non_runner   TINYINT(1)      NOT NULL DEFAULT 0,
    is_withdrawn    TINYINT(1)      NOT NULL DEFAULT 0,
    weight_pounds   DECIMAL(5,2)    NULL,
    weight_stones   DECIMAL(5,2)    NULL,
    open_decimal    DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    fluc1           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    fluc2           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    race_source     TINYINT         NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_rr_race   FOREIGN KEY (race_id)   REFERENCES races   (id),
    CONSTRAINT fk_rr_runner FOREIGN KEY (runner_id) REFERENCES runners (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


CREATE_PRICES_TABLE = """
CREATE TABLE IF NOT EXISTS prices (
    id              INT             NOT NULL AUTO_INCREMENT,
    race_runner_id  INT             NOT NULL,
    price_id        VARCHAR(50)     NULL,
    decimal_value   DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    win_value       DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    place_value     DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    win_pool        DECIMAL(14,2)   NOT NULL DEFAULT 0.00,
    place_pool      DECIMAL(14,2)   NOT NULL DEFAULT 0.00,
    fraction_value  VARCHAR(20)     NULL,
    win_frac_value  VARCHAR(20)     NULL,
    place_frac_value VARCHAR(20)    NULL,
    market_id       INT             NOT NULL DEFAULT 0,
    time_field      DATETIME        NULL,
    timestamp_field BIGINT          NOT NULL DEFAULT 0,
    is_accurate     TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    UNIQUE KEY uq_price_rr_price_id (race_runner_id, price_id),
    CONSTRAINT fk_price_rr FOREIGN KEY (race_runner_id) REFERENCES race_runners (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


CREATE_RESULTS_TABLE = """
CREATE TABLE IF NOT EXISTS results (
    id              INT             NOT NULL,
    result_id       VARCHAR(50)     NULL,
    race_id         INT             NOT NULL,
    runner_id       INT             NOT NULL,
    race_runner_id  INT             NOT NULL,
    position        TINYINT         NOT NULL,
    win             DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    place           DECIMAL(10,2)   NOT NULL DEFAULT 0.00,
    odd             VARCHAR(20)     NULL,
    runner_number   TINYINT         NULL,
    jockey          VARCHAR(100)    NULL,
    race_source     TINYINT         NULL,
    is_deleted      TINYINT(1)      NOT NULL DEFAULT 0,
    PRIMARY KEY (id),
    CONSTRAINT fk_result_race   FOREIGN KEY (race_id)       REFERENCES races        (id),
    CONSTRAINT fk_result_runner FOREIGN KEY (runner_id)     REFERENCES runners      (id),
    CONSTRAINT fk_result_rr     FOREIGN KEY (race_runner_id) REFERENCES race_runners (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ─────────────────────────────────────────────
# INSERT/UPSERT
# ─────────────────────────────────────────────

INSERT_MEETING_SQL = """
INSERT INTO meetings (
    id, meeting_name, code, country_code, coverage_code,
    sport_code, category, sub_code, date, going,
    is_evening_meeting, is_deleted
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s
)
ON DUPLICATE KEY UPDATE
    meeting_name=VALUES(meeting_name),
    code=VALUES(code),
    country_code=VALUES(country_code),
    coverage_code=VALUES(coverage_code),
    sport_code=VALUES(sport_code),
    category=VALUES(category),
    sub_code=VALUES(sub_code),
    date=VALUES(date),
    going=VALUES(going),
    is_evening_meeting=VALUES(is_evening_meeting),
    is_deleted=VALUES(is_deleted);
"""


INSERT_RUNNER_SQL = """
INSERT INTO runners (
    id, runner_id, runner_name, jockey, trainer,
    silk, silk_file, runner_status, age, last_runs,
    claiming, owner, horse_farm
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    runner_id=VALUES(runner_id),
    runner_name=VALUES(runner_name),
    jockey=VALUES(jockey),
    trainer=VALUES(trainer),
    silk=VALUES(silk),
    silk_file=VALUES(silk_file),
    runner_status=VALUES(runner_status),
    age=VALUES(age),
    last_runs=VALUES(last_runs),
    claiming=VALUES(claiming),
    owner=VALUES(owner),
    horse_farm=VALUES(horse_farm);
"""


INSERT_RACE_SQL = """
INSERT INTO races (
    id, meeting_id, race_id, race_name, race_number,
    start_time, start_time_utc, course_type, distance,
    no_of_runners, eachway_places, expected_places, is_handicap,
    off_time, off_time_utc, status, surface,
    progress_code, progress_message, place_config_id,
    source, channel, broadcast_channel,
    is_quinella, is_exacta, is_trifecta, is_first_four,
    is_forecast, is_tricast, is_trio,
    is_settled, is_deleted
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s
)
ON DUPLICATE KEY UPDATE
    meeting_id=VALUES(meeting_id),
    race_id=VALUES(race_id),
    race_name=VALUES(race_name),
    race_number=VALUES(race_number),
    start_time=VALUES(start_time),
    start_time_utc=VALUES(start_time_utc),
    course_type=VALUES(course_type),
    distance=VALUES(distance),
    no_of_runners=VALUES(no_of_runners),
    eachway_places=VALUES(eachway_places),
    expected_places=VALUES(expected_places),
    is_handicap=VALUES(is_handicap),
    off_time=VALUES(off_time),
    off_time_utc=VALUES(off_time_utc),
    status=VALUES(status),
    surface=VALUES(surface),
    progress_code=VALUES(progress_code),
    progress_message=VALUES(progress_message),
    place_config_id=VALUES(place_config_id),
    source=VALUES(source),
    channel=VALUES(channel),
    broadcast_channel=VALUES(broadcast_channel),
    is_quinella=VALUES(is_quinella),
    is_exacta=VALUES(is_exacta),
    is_trifecta=VALUES(is_trifecta),
    is_first_four=VALUES(is_first_four),
    is_forecast=VALUES(is_forecast),
    is_tricast=VALUES(is_tricast),
    is_trio=VALUES(is_trio),
    is_settled=VALUES(is_settled),
    is_deleted=VALUES(is_deleted);
"""


INSERT_RACE_RUNNER_SQL = """
INSERT INTO race_runners (
    id, race_id, runner_id, number, drawn,
    is_non_runner, is_withdrawn,
    weight_pounds, weight_stones,
    open_decimal, fluc1, fluc2,
    race_source
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s,
    %s, %s, %s,
    %s
)
ON DUPLICATE KEY UPDATE
    race_id=VALUES(race_id),
    runner_id=VALUES(runner_id),
    number=VALUES(number),
    drawn=VALUES(drawn),
    is_non_runner=VALUES(is_non_runner),
    is_withdrawn=VALUES(is_withdrawn),
    weight_pounds=VALUES(weight_pounds),
    weight_stones=VALUES(weight_stones),
    open_decimal=VALUES(open_decimal),
    fluc1=VALUES(fluc1),
    fluc2=VALUES(fluc2),
    race_source=VALUES(race_source);
"""


INSERT_PRICE_SQL = """
INSERT INTO prices (
    race_runner_id, price_id, decimal_value, win_value, place_value,
    win_pool, place_pool,
    fraction_value, win_frac_value, place_frac_value,
    market_id, time_field, timestamp_field, is_accurate
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s,
    %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    decimal_value=VALUES(decimal_value),
    win_value=VALUES(win_value),
    place_value=VALUES(place_value),
    win_pool=VALUES(win_pool),
    place_pool=VALUES(place_pool),
    fraction_value=VALUES(fraction_value),
    win_frac_value=VALUES(win_frac_value),
    place_frac_value=VALUES(place_frac_value),
    market_id=VALUES(market_id),
    time_field=VALUES(time_field),
    timestamp_field=VALUES(timestamp_field),
    is_accurate=VALUES(is_accurate);
"""


INSERT_RESULT_SQL = """
INSERT INTO results (
    id, result_id, race_id, runner_id, race_runner_id,
    position, win, place, odd,
    runner_number, jockey, race_source, is_deleted
) VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s
)
ON DUPLICATE KEY UPDATE
    result_id=VALUES(result_id),
    race_id=VALUES(race_id),
    runner_id=VALUES(runner_id),
    race_runner_id=VALUES(race_runner_id),
    position=VALUES(position),
    win=VALUES(win),
    place=VALUES(place),
    odd=VALUES(odd),
    runner_number=VALUES(runner_number),
    jockey=VALUES(jockey),
    race_source=VALUES(race_source),
    is_deleted=VALUES(is_deleted);
"""


# ─────────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────────


def get_connection(with_db: bool = True):
    config = DB_CONFIG.copy()
    if with_db:
        config["database"] = DATABASE_NAME
    return mysql.connector.connect(**config)


# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────


def ensure_database_and_table():
    """Creates database + tables if they do not exist."""
    try:
        conn = get_connection(with_db=False)
        cursor = conn.cursor()

        cursor.execute(CREATE_DB_SQL)
        cursor.execute(f"USE `{DATABASE_NAME}`;")

        cursor.execute(CREATE_MEETINGS_TABLE)
        cursor.execute(CREATE_RUNNERS_TABLE)
        cursor.execute(CREATE_RACES_TABLE)
        cursor.execute(CREATE_RACE_RUNNERS_TABLE)
        cursor.execute(CREATE_PRICES_TABLE)
        cursor.execute(CREATE_RESULTS_TABLE)

        # Ensure the unique key used for price upserts exists.
        # Note: if you already have duplicates in `prices`, adding this key will fail.
        try:
            cursor.execute(
                "ALTER TABLE prices ADD UNIQUE KEY uq_price_rr_price_id (race_runner_id, price_id);"
            )
        except mysql.connector.Error as err:
            # 1061: duplicate key name, 1068/others: already exists or cannot add.
            log.debug(f"Skipping prices unique key creation: {err}")

        conn.commit()
        cursor.close()
        conn.close()

        log.info(f"Database `{DATABASE_NAME}` and tables are ready.")

    except mysql.connector.Error as err:
        log.error(f"Database setup failed: {err}")
        raise


# ─────────────────────────────────────────────
# PARSING
# ─────────────────────────────────────────────


def _parse_dt(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        if value.startswith("0001"):
            return None
        raw = value.strip()
        raw = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(raw)
        except ValueError:
            try:
                return datetime.fromisoformat(raw[:19])
            except ValueError:
                return None

    return None


def _parse_date(value):
    if not value:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    dt = _parse_dt(value)
    return dt.date() if dt else None


def _to_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bool_int(value, default: int = 0) -> int:
    if value is None:
        return default
    return 1 if bool(value) else 0


# ─────────────────────────────────────────────
# STORE
# ─────────────────────────────────────────────


def store_records(records):
    """Stores meetings/races/runners from the featured races API payload."""
    if not records:
        log.warning("No records received. Nothing stored.")
        return

    conn = get_connection()
    cursor = conn.cursor()

    total_meetings = total_races = total_runners = 0
    total_race_runners = total_prices = total_results = 0

    try:
        for meeting in records:
            meeting_id = _to_int(meeting.get("id"))
            if meeting_id is None:
                log.warning("Skipping meeting without id")
                continue

            cursor.execute(
                INSERT_MEETING_SQL,
                (
                    meeting_id,
                    (meeting.get("meetingName") or meeting.get("meeting_name") or "").strip() or "Unknown",
                    meeting.get("code"),
                    meeting.get("countryCode") or meeting.get("country_code"),
                    meeting.get("coverageCode") or meeting.get("coverage_code"),
                    meeting.get("sportCode") or meeting.get("sport_code"),
                    meeting.get("category") or "HR",
                    meeting.get("subCode") or meeting.get("sub_code"),
                    _parse_date(meeting.get("date")),
                    meeting.get("going"),
                    _bool_int(meeting.get("isEveningMeeting") or meeting.get("isEveningMeeting", False)),
                    _bool_int(meeting.get("isDeleted"), 0),
                ),
            )
            total_meetings += 1

            for race in meeting.get("races", []) or []:
                race_id = _to_int(race.get("id"))
                if race_id is None:
                    continue

                cursor.execute(
                    INSERT_RACE_SQL,
                    (
                        race_id,
                        meeting_id,
                        race.get("raceId"),
                        race.get("raceName"),
                        _to_int(race.get("raceNumber")),
                        _parse_dt(race.get("startTime")),
                        _parse_dt(race.get("startTimeUtc")),
                        race.get("courseType"),
                        race.get("distance"),
                        _to_int(race.get("noOfRunners")),
                        _to_int(race.get("eachwayPlaces")),
                        _to_int(race.get("expectedPlaces")),
                        _bool_int(race.get("isHandiCap") if "isHandiCap" in race else race.get("isHandicap"), 0),
                        _parse_dt(race.get("offTime")),
                        _parse_dt(race.get("offTimeUtc")),
                        race.get("status"),
                        race.get("surface"),
                        race.get("progressCode"),
                        race.get("progressMessage"),
                        _to_int(race.get("placeConfigId")),
                        _to_int(race.get("source")),
                        _to_int(race.get("channel")),
                        race.get("broadcastChannel"),
                        _bool_int(race.get("isQuinella"), 0),
                        _bool_int(race.get("isExacta"), 0),
                        _bool_int(race.get("isTrifecta"), 0),
                        _bool_int(race.get("isFirstFour"), 0),
                        _bool_int(race.get("isForecast"), 0),
                        _bool_int(race.get("isTricast"), 0),
                        _bool_int(race.get("isTrio"), 0),
                        _bool_int(race.get("isSettled"), 0),
                        _bool_int(race.get("isDeleted"), 0),
                    ),
                )
                total_races += 1

                for rr in race.get("raceRunners", []) or []:
                    rr_id = _to_int(rr.get("id"))
                    runner_pk = _to_int(rr.get("runnerId"))
                    if rr_id is None or runner_pk is None:
                        continue

                    runner = rr.get("runner") or {}
                    latest_price = rr.get("latestPrice") or {}

                    runner_name = runner.get("runnerName") or runner.get("name")
                    if not runner_name:
                        runner_name = f"Runner-{runner_pk}"

                    cursor.execute(
                        INSERT_RUNNER_SQL,
                        (
                            runner_pk,
                            runner.get("runnerId") or str(runner_pk),
                            runner_name,
                            runner.get("jockey"),
                            runner.get("trainer"),
                            runner.get("silk"),
                            runner.get("silkFile"),
                            (runner.get("runnerStatus") or rr.get("runnerStatus")),
                            _to_int(runner.get("age")),
                            runner.get("lastRuns"),
                            runner.get("claiming"),
                            runner.get("owner"),
                            runner.get("horseFarm"),
                        ),
                    )
                    total_runners += 1

                    cursor.execute(
                        INSERT_RACE_RUNNER_SQL,
                        (
                            rr_id,
                            race_id,
                            runner_pk,
                            _to_int(rr.get("number")),
                            _to_int(rr.get("drawn")),
                            _bool_int(rr.get("isNonRunner"), 0),
                            _bool_int(rr.get("isWithdrawn"), 0),
                            rr.get("weightPounds"),
                            rr.get("weightStones"),
                            rr.get("openDecimal") or 0.0,
                            rr.get("fluc1") or 0.0,
                            rr.get("fluc2") or 0.0,
                            _to_int(rr.get("raceSource")),
                        ),
                    )
                    total_race_runners += 1

                    if latest_price:
                        cursor.execute(
                            INSERT_PRICE_SQL,
                            (
                                rr_id,
                                rr.get("latestPriceId") or latest_price.get("priceId") or latest_price.get("id"),
                                latest_price.get("decimalValue") or 0.0,
                                latest_price.get("winValue") or 0.0,
                                latest_price.get("placeValue") or 0.0,
                                latest_price.get("winPool") or 0.0,
                                latest_price.get("placePool") or 0.0,
                                latest_price.get("fractionValue"),
                                latest_price.get("winFracValue"),
                                latest_price.get("placeFracValue"),
                                _to_int(latest_price.get("marketId") or 0),
                                _parse_dt(latest_price.get("timeField") or latest_price.get("time")),
                                _to_int(latest_price.get("timestampField") or latest_price.get("timestamp")) or 0,
                                _bool_int(latest_price.get("isAccurate"), 0),
                            ),
                        )
                        total_prices += 1

                    # Results are optional; store only when present.
                    rr_result = rr.get("result")
                    if isinstance(rr_result, dict):
                        res_id = _to_int(rr_result.get("id"))
                        pos = _to_int(rr_result.get("position"))
                        if res_id is not None and pos is not None:
                            cursor.execute(
                                INSERT_RESULT_SQL,
                                (
                                    res_id,
                                    rr_result.get("resultId"),
                                    race_id,
                                    runner_pk,
                                    rr_id,
                                    pos,
                                    rr_result.get("win") or 0.0,
                                    rr_result.get("place") or 0.0,
                                    rr_result.get("odd"),
                                    _to_int(rr_result.get("runnerNumber")),
                                    rr_result.get("jockey") or runner.get("jockey"),
                                    _to_int(rr_result.get("raceSource")),
                                    _bool_int(rr_result.get("isDeleted"), 0),
                                ),
                            )
                            total_results += 1

        conn.commit()
        log.info(
            "Stored: %s meeting(s), %s race(s), %s runner(s), %s race_runner(s), %s price row(s), %s result row(s).",
            total_meetings,
            total_races,
            total_runners,
            total_race_runners,
            total_prices,
            total_results,
        )

    except mysql.connector.Error as err:
        conn.rollback()
        log.error(f"Insert failed: {err}")
        raise

    finally:
        cursor.close()
        conn.close()


# Keep main.py working: treat API-2/3/4 as the same payload shape.
def store_api2_records(records):
    store_records(records)


def store_api3_records(records):
    store_records(records)


def store_api4_records(records):
    store_records(records)
