"""
UrbanPulse — NYC 311 Ingestion Script
======================================
Pulls NYC 311 service requests from the Socrata API and lands them
directly into Snowflake RAW schema.

Two modes:
  - historical: pulls last 90 days on first run (backfill)
  - incremental: pulls only last 3 days (daily runs)

Run locally:
    python ingestion/nyc_311.py --mode historical
    python ingestion/nyc_311.py --mode incremental

Author: UrbanPulse Pipeline
"""

import os
import time
import logging
import argparse
from datetime import datetime, timezone, timedelta

import requests
import snowflake.connector
from dotenv import load_dotenv

# SETUP
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# CONFIGURATION
NYC_311_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
APP_TOKEN        = os.getenv("NYC_APP_TOKEN")

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "role":      os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": "URBANPULSE_LOADING_WH",
    "database":  "URBANPULSE",
    "schema":    "RAW",
}

PAGE_SIZE = 50_000

SELECTED_COLUMNS = ",".join([
    "unique_key",
    "created_date",
    "closed_date",
    "complaint_type",
    "descriptor",
    "incident_zip",
    "incident_address",
    "city",
    "borough",
    "latitude",
    "longitude",
    "status",
    "resolution_description",
    "agency",
    "agency_name",
])

# Retry configuration
MAX_RETRIES    = 3
RETRY_DELAY_S  = 30   # Wait 30 seconds between retries on 503


# SNOWFLAKE: CREATE RAW TABLE
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS URBANPULSE.RAW.nyc_311_raw (
    unique_key              VARCHAR(50),
    created_date            VARCHAR(50),
    closed_date             VARCHAR(50),
    complaint_type          VARCHAR(200),
    descriptor              VARCHAR(500),
    incident_zip            VARCHAR(20),
    incident_address        VARCHAR(500),
    city                    VARCHAR(100),
    borough                 VARCHAR(100),
    latitude                VARCHAR(50),
    longitude               VARCHAR(50),
    status                  VARCHAR(100),
    resolution_description  VARCHAR(2000),
    agency                  VARCHAR(50),
    agency_name             VARCHAR(200),
    _ingested_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ingestion_mode         VARCHAR(20),
    _source                 VARCHAR(100) DEFAULT 'nyc_open_data_311'
)
COMMENT = 'Raw NYC 311 service requests. Loaded by UrbanPulse ingestion pipeline.';
"""


# FETCH FROM SOCRATA API (with retry logic)
def fetch_311_page(offset: int, since_date: str) -> list[dict]:
    """
    Fetch one page of 311 records with automatic retry on transient errors.
    503 = server temporarily unavailable — always worth retrying.
    """
    headers = {
        "X-App-Token": APP_TOKEN,
        "Accept":      "application/json",
    }
    params = {
        "$select":  SELECTED_COLUMNS,
        "$where":   f"created_date >= '{since_date}'",
        "$order":   "created_date ASC",
        "$limit":   PAGE_SIZE,
        "$offset":  offset,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                NYC_311_ENDPOINT,
                headers=headers,
                params=params,
                timeout=60,
            )

            if response.status_code == 200:
                records = response.json()
                log.info(f"  Fetched {len(records):,} records at offset {offset:,}")
                return records

            elif response.status_code == 503:
                # Server temporarily unavailable — wait and retry
                log.warning(
                    f"  503 Service Unavailable (attempt {attempt}/{MAX_RETRIES}). "
                    f"NYC Open Data servers are temporarily down. "
                    f"Waiting {RETRY_DELAY_S}s before retry..."
                )
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY_S)
                else:
                    raise ValueError(
                        f"NYC Open Data returned 503 after {MAX_RETRIES} attempts. "
                        f"Their servers may be undergoing maintenance. Try again later."
                    )

            elif response.status_code == 429:
                # Rate limited — wait longer
                log.warning(f"  429 Rate Limited. Waiting 60s...")
                time.sleep(60)

            else:
                raise ValueError(
                    f"API returned {response.status_code}: {response.text[:300]}"
                )

        except requests.exceptions.Timeout:
            log.warning(f"  Request timed out (attempt {attempt}/{MAX_RETRIES})")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_S)
            else:
                raise

        except requests.exceptions.ConnectionError as e:
            log.warning(f"  Connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_S)
            else:
                raise


def fetch_all_311(since_date: str) -> list[dict]:
    """Paginate through all 311 records since since_date."""
    all_records = []
    offset = 0

    log.info(f"Fetching 311 records since {since_date}...")

    while True:
        page = fetch_311_page(offset, since_date)
        all_records.extend(page)

        if len(page) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        log.info(f"  Total so far: {len(all_records):,} records")

    log.info(f"Total records fetched: {len(all_records):,}")
    return all_records


# LOAD INTO SNOWFLAKE
def get_snowflake_connection():
    missing = [k for k, v in SNOWFLAKE_CONFIG.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing Snowflake config: {missing}. Check your .env file."
        )
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_table_exists(cursor):
    log.info("Ensuring RAW table exists...")
    cursor.execute(CREATE_TABLE_SQL)
    log.info("Table ready: URBANPULSE.RAW.nyc_311_raw")


def load_to_snowflake(records: list[dict], mode: str, cursor) -> int:
    if not records:
        log.info("No records to load.")
        return 0

    # Use timezone-aware UTC datetime (fixes deprecation warning)
    ingested_at = datetime.now(timezone.utc).isoformat()
    batch_size  = 5_000
    total_loaded = 0

    log.info(f"Loading {len(records):,} records into Snowflake...")

    for i in range(0, len(records), batch_size):
        batch = records[i: i + batch_size]

        rows = [
            (
                r.get("unique_key", ""),
                r.get("created_date", ""),
                r.get("closed_date", ""),
                r.get("complaint_type", ""),
                r.get("descriptor", ""),
                r.get("incident_zip", ""),
                r.get("incident_address", ""),
                r.get("city", ""),
                r.get("borough", ""),
                r.get("latitude", ""),
                r.get("longitude", ""),
                r.get("status", ""),
                r.get("resolution_description", ""),
                r.get("agency", ""),
                r.get("agency_name", ""),
                ingested_at,
                mode,
                "nyc_open_data_311",
            )
            for r in batch
        ]

        cursor.executemany(
            """
            INSERT INTO URBANPULSE.RAW.nyc_311_raw (
                unique_key, created_date, closed_date, complaint_type,
                descriptor, incident_zip, incident_address, city, borough,
                latitude, longitude, status, resolution_description,
                agency, agency_name, _ingested_at, _ingestion_mode, _source
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            rows,
        )
        total_loaded += len(batch)
        log.info(f"  Inserted batch {i // batch_size + 1} — {total_loaded:,} rows total")

    return total_loaded


# MAIN
def run(mode: str):
    log.info(f"=== UrbanPulse 311 Ingestion | Mode: {mode.upper()} ===")

    # Use timezone-aware UTC datetime (fixes deprecation warning)
    now = datetime.now(timezone.utc)

    if mode == "historical":
        since_date = (now - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
        log.info(f"Historical mode: pulling 90 days back to {since_date}")
    elif mode == "incremental":
        since_date = (now - timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
        log.info(f"Incremental mode: 3-day lookback from {since_date}")
    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'historical' or 'incremental'.")

    records = fetch_all_311(since_date)

    if not records:
        log.warning("No records returned from API. Exiting.")
        return

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        ensure_table_exists(cursor)
        rows_loaded = load_to_snowflake(records, mode, cursor)
        conn.commit()
        log.info(f"=== SUCCESS: {rows_loaded:,} rows loaded into Snowflake ===")

    except Exception as e:
        conn.rollback()
        log.error(f"Pipeline failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UrbanPulse 311 Ingestion")
    parser.add_argument(
        "--mode",
        choices=["historical", "incremental"],
        default="incremental",
        help="historical = 90 days backfill | incremental = 3-day lookback",
    )
    args = parser.parse_args()
    run(args.mode)
