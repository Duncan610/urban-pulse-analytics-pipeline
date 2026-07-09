"""
UrbanPulse — NYC 311 Ingestion Script
======================================
Pulls NYC 311 service requests from the Socrata API and lands them
directly into Snowflake RAW schema.

Two modes:
  - Historical load: pulls last 90 days on first run (backfill)
  - Incremental load: pulls only records since last run date

Run locally:
    python ingest_nyc_311.py --mode historical
    python ingest_nyc_311.py --mode incremental

Author: UrbanPulse Pipeline
"""

import os
import json
import logging
import argparse
from datetime import datetime, timedelta

import requests
import snowflake.connector
from dotenv import load_dotenv

# =============================================================================
# SETUP
# =============================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================
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

# How many rows to pull per API page
# 50,000 is the Socrata maximum with an app token
PAGE_SIZE = 50_000

# Columns we care about — keeps payload lean
# We select exactly what we need rather than pulling all 40+ columns
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


# =============================================================================
# SNOWFLAKE: CREATE RAW TABLE
# =============================================================================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS URBANPULSE.RAW.nyc_311_raw (
    unique_key              VARCHAR(50),
    created_date            VARCHAR(50),    -- Raw string; dbt will cast to TIMESTAMP
    closed_date             VARCHAR(50),
    complaint_type          VARCHAR(200),
    descriptor              VARCHAR(500),
    incident_zip            VARCHAR(20),
    incident_address        VARCHAR(500),
    city                    VARCHAR(100),
    borough                 VARCHAR(100),
    latitude                VARCHAR(50),    -- Raw string; dbt will cast to FLOAT
    longitude               VARCHAR(50),
    status                  VARCHAR(100),
    resolution_description  VARCHAR(2000),
    agency                  VARCHAR(50),
    agency_name             VARCHAR(200),
    -- Pipeline metadata columns
    -- These are critical for incremental loading and debugging
    _ingested_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _ingestion_mode         VARCHAR(20),    -- 'historical' or 'incremental'
    _source                 VARCHAR(100) DEFAULT 'nyc_open_data_311'
)
COMMENT = 'Raw NYC 311 service requests. Loaded by UrbanPulse ingestion pipeline. Do not modify directly.';
"""

# =============================================================================
# SNOWFLAKE: UPSERT STATEMENT
# We use MERGE instead of INSERT to handle duplicate runs gracefully.
# If the pipeline runs twice, we update existing records rather than duplicate.
# This is what "idempotent pipeline" means — safe to run multiple times.
# =============================================================================
MERGE_SQL = """
MERGE INTO URBANPULSE.RAW.nyc_311_raw AS target
USING (SELECT * FROM VALUES {placeholders}) AS source (
    unique_key, created_date, closed_date, complaint_type, descriptor,
    incident_zip, incident_address, city, borough, latitude, longitude,
    status, resolution_description, agency, agency_name,
    _ingested_at, _ingestion_mode, _source
)
ON target.unique_key = source.unique_key
WHEN MATCHED THEN UPDATE SET
    closed_date            = source.closed_date,
    status                 = source.status,
    resolution_description = source.resolution_description,
    _ingested_at           = source._ingested_at
WHEN NOT MATCHED THEN INSERT (
    unique_key, created_date, closed_date, complaint_type, descriptor,
    incident_zip, incident_address, city, borough, latitude, longitude,
    status, resolution_description, agency, agency_name,
    _ingested_at, _ingestion_mode, _source
) VALUES (
    source.unique_key, source.created_date, source.closed_date,
    source.complaint_type, source.descriptor, source.incident_zip,
    source.incident_address, source.city, source.borough,
    source.latitude, source.longitude, source.status,
    source.resolution_description, source.agency, source.agency_name,
    source._ingested_at, source._ingestion_mode, source._source
);
"""


# =============================================================================
# FETCH FROM SOCRATA API
# =============================================================================
def fetch_311_page(offset: int, since_date: str) -> list[dict]:
    """
    Fetch one page of 311 records from the Socrata API.

    Args:
        offset:     Pagination offset (0, 50000, 100000, ...)
        since_date: Only fetch records created after this date (ISO format)

    Returns:
        List of record dicts from the API
    """
    headers = {
        "X-App-Token": APP_TOKEN,
        "Accept": "application/json",
    }

    params = {
        "$select":  SELECTED_COLUMNS,
        "$where":   f"created_date >= '{since_date}'",
        "$order":   "created_date ASC",
        "$limit":   PAGE_SIZE,
        "$offset":  offset,
    }

    response = requests.get(
        NYC_311_ENDPOINT,
        headers=headers,
        params=params,
        timeout=60,
    )

    if response.status_code != 200:
        raise ValueError(
            f"API returned {response.status_code}: {response.text[:500]}"
        )

    records = response.json()
    log.info(f"  Fetched {len(records):,} records at offset {offset:,}")
    return records


def fetch_all_311(since_date: str) -> list[dict]:
    """
    Paginate through all 311 records since since_date.
    Stops when a page returns fewer rows than PAGE_SIZE (last page).
    """
    all_records = []
    offset = 0

    log.info(f"Fetching 311 records since {since_date}...")

    while True:
        page = fetch_311_page(offset, since_date)
        all_records.extend(page)

        if len(page) < PAGE_SIZE:
            # Last page — we've got everything
            break

        offset += PAGE_SIZE
        log.info(f"  Total so far: {len(all_records):,} records")

    log.info(f"Total records fetched: {len(all_records):,}")
    return all_records


# =============================================================================
# LOAD INTO SNOWFLAKE
# =============================================================================
def get_snowflake_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_table_exists(cursor):
    """Create the raw table if it doesn't already exist."""
    log.info("Ensuring RAW table exists...")
    cursor.execute(CREATE_TABLE_SQL)
    log.info("Table ready: URBANPULSE.RAW.nyc_311_raw")


def load_to_snowflake(records: list[dict], mode: str, cursor) -> int:
    """
    Load records into Snowflake using batch INSERT.
    Returns number of rows loaded.
    """
    if not records:
        log.info("No records to load.")
        return 0

    ingested_at = datetime.utcnow().isoformat()
    batch_size  = 5_000   # Insert 5k rows per statement to stay within limits
    total_loaded = 0

    log.info(f"Loading {len(records):,} records into Snowflake...")

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]

        rows = []
        for r in batch:
            rows.append((
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
            ))

        # executemany is faster than looping individual inserts
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
        log.info(f"  Inserted batch {i//batch_size + 1} — {total_loaded:,} rows total")

    return total_loaded


# =============================================================================
# MAIN ENTRYPOINT
# =============================================================================
def run(mode: str):
    """
    Main pipeline function.

    Args:
        mode: 'historical' → pull last 90 days
              'incremental' → pull last 3 days (catches late arrivals)
    """
    log.info(f"=== UrbanPulse 311 Ingestion | Mode: {mode.upper()} ===")

    # Determine date range
    if mode == "historical":
        since_date = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%dT00:00:00")
        log.info(f"Historical mode: pulling 90 days back to {since_date}")
    elif mode == "incremental":
        # 3-day lookback catches late-arriving records
        # This is important: records sometimes appear in the API 24-48 hours late
        since_date = (datetime.utcnow() - timedelta(days=3)).strftime("%Y-%m-%dT00:00:00")
        log.info(f"Incremental mode: pulling 3-day lookback from {since_date}")
    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'historical' or 'incremental'.")

    # Fetch from API
    records = fetch_all_311(since_date)

    if not records:
        log.warning("No records returned from API. Exiting.")
        return

    # Load to Snowflake
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


# =============================================================================
# CLI
# =============================================================================
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