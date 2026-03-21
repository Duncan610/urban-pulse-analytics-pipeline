"""
UrbanPulse — US Census Bureau Ingestion Script
===============================================
Pulls American Community Survey (ACS) 5-year estimates for NYC
demographics at the borough level and lands them in Snowflake RAW.

Data includes:
  - Total population
  - Median household income
  - Poverty rate
  - Population density proxy

This data changes annually — it becomes our SCD Type 2 dimension in dbt.
Each year we run this, we track how demographics shifted over time.

Run locally:
    python ingestion/census.py

Author: UrbanPulse Pipeline
"""

import os
import logging
from datetime import datetime, timezone

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
CENSUS_API_KEY   = os.getenv("CENSUS_API_KEY")
CENSUS_ENDPOINT  = "https://api.census.gov/data/2022/acs/acs5"

# NYC County FIPS codes — each borough maps to a county
# Manhattan=061, Bronx=005, Brooklyn=047, Queens=081, Staten Island=085
# State FIPS for New York = 36
NYC_COUNTIES = {
    "061": "Manhattan",
    "005": "Bronx",
    "047": "Brooklyn",
    "081": "Queens",
    "085": "Staten Island",
}

# Census variables we want
CENSUS_VARIABLES = {
    "B01003_001E": "total_population",
    "B19013_001E": "median_household_income",
    "B17001_002E": "population_below_poverty",
    "B25001_001E": "total_housing_units",
    "B08006_001E": "total_workers_commuting",
}

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "role":      os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": "URBANPULSE_LOADING_WH",
    "database":  "URBANPULSE",
    "schema":    "RAW",
}


# =============================================================================
# SNOWFLAKE: CREATE RAW TABLE
# =============================================================================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS URBANPULSE.RAW.census_raw (
    borough                     VARCHAR(50),
    county_fips                 VARCHAR(10),
    state_fips                  VARCHAR(10),
    survey_year                 INTEGER,
    total_population            FLOAT,
    median_household_income     FLOAT,
    population_below_poverty    FLOAT,
    total_housing_units         FLOAT,
    total_workers_commuting     FLOAT,
    poverty_rate_pct            FLOAT,
    _ingested_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                     VARCHAR(100) DEFAULT 'us_census_bureau_acs5'
)
COMMENT = 'Raw US Census ACS 5-year estimates for NYC boroughs. Source for SCD Type 2 neighborhood dimension.';
"""


# =============================================================================
# FETCH FROM CENSUS API
# =============================================================================
def fetch_census_data() -> list[dict]:
    """
    Fetch ACS 5-year estimates for all NYC counties.

    Returns:
        List of borough-level demographic records
    """
    variables = "NAME," + ",".join(CENSUS_VARIABLES.keys())

    params = {
        "get": variables,
        "for": "county:061,005,047,081,085",
        "in":  "state:36",
        "key": CENSUS_API_KEY,
    }

    log.info("Fetching Census ACS 5-year estimates for NYC boroughs...")

    response = requests.get(CENSUS_ENDPOINT, params=params, timeout=30)

    if response.status_code != 200:
        raise ValueError(
            f"Census API error: {response.status_code} — {response.text[:300]}"
        )

    raw     = response.json()
    headers = raw[0]
    rows    = raw[1:]

    log.info(f"Census API returned {len(rows)} borough records")

    records = []
    for row in rows:
        data        = dict(zip(headers, row))
        county_fips = data.get("county", "")
        borough     = NYC_COUNTIES.get(county_fips, "Unknown")

        def safe_float(val):
            try:
                f = float(val)
                return None if f < 0 else f
            except (TypeError, ValueError):
                return None

        total_pop     = safe_float(data.get("B01003_001E"))
        below_poverty = safe_float(data.get("B17001_002E"))
        median_income = safe_float(data.get("B19013_001E"))

        if total_pop and total_pop > 0 and below_poverty is not None:
            poverty_rate = round((below_poverty / total_pop) * 100, 2)
        else:
            poverty_rate = None

        record = {
            "borough":                  borough,
            "county_fips":              county_fips,
            "state_fips":               data.get("state", "36"),
            "survey_year":              2022,
            "total_population":         total_pop,
            "median_household_income":  median_income,
            "population_below_poverty": below_poverty,
            "total_housing_units":      safe_float(data.get("B25001_001E")),
            "total_workers_commuting":  safe_float(data.get("B08006_001E")),
            "poverty_rate_pct":         poverty_rate,
            "_ingested_at":             datetime.now(timezone.utc).isoformat(),
            "_source":                  "us_census_bureau_acs5",
        }

        log.info(
            f"  {borough}: pop={total_pop:,.0f}, "
            f"median_income=${median_income:,.0f}, "
            f"poverty={poverty_rate}%"
            if total_pop and median_income and poverty_rate
            else f"  {borough}: data fetched"
        )

        records.append(record)

    return records


# =============================================================================
# LOAD INTO SNOWFLAKE
# =============================================================================
def get_snowflake_connection():
    missing = [k for k, v in SNOWFLAKE_CONFIG.items() if not v]
    if missing:
        raise EnvironmentError(
            f"Missing Snowflake config: {missing}. Check your .env file."
        )
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_table_exists(cursor):
    log.info("Ensuring census RAW table exists...")
    cursor.execute(CREATE_TABLE_SQL)
    log.info("Table ready: URBANPULSE.RAW.census_raw")


def load_to_snowflake(records: list[dict], cursor) -> int:
    """
    Load census records using MERGE for idempotency.
    Same borough + year = update not duplicate.
    """
    rows = [
        (
            r["borough"],
            r["county_fips"],
            r["state_fips"],
            r["survey_year"],
            r["total_population"],
            r["median_household_income"],
            r["population_below_poverty"],
            r["total_housing_units"],
            r["total_workers_commuting"],
            r["poverty_rate_pct"],
            r["_ingested_at"],
            r["_source"],
        )
        for r in records
    ]

    cursor.execute("""
        CREATE TEMPORARY TABLE census_staging (
            borough                     VARCHAR(50),
            county_fips                 VARCHAR(10),
            state_fips                  VARCHAR(10),
            survey_year                 INTEGER,
            total_population            FLOAT,
            median_household_income     FLOAT,
            population_below_poverty    FLOAT,
            total_housing_units         FLOAT,
            total_workers_commuting     FLOAT,
            poverty_rate_pct            FLOAT,
            _ingested_at                TIMESTAMP_NTZ,
            _source                     VARCHAR(100)
        )
    """)

    cursor.executemany(
        "INSERT INTO census_staging VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows,
    )

    cursor.execute("""
        MERGE INTO URBANPULSE.RAW.census_raw AS target
        USING census_staging AS source
        ON  target.borough     = source.borough
        AND target.survey_year = source.survey_year
        WHEN MATCHED THEN UPDATE SET
            total_population            = source.total_population,
            median_household_income     = source.median_household_income,
            population_below_poverty    = source.population_below_poverty,
            total_housing_units         = source.total_housing_units,
            total_workers_commuting     = source.total_workers_commuting,
            poverty_rate_pct            = source.poverty_rate_pct,
            _ingested_at                = source._ingested_at
        WHEN NOT MATCHED THEN INSERT (
            borough, county_fips, state_fips, survey_year,
            total_population, median_household_income,
            population_below_poverty, total_housing_units,
            total_workers_commuting, poverty_rate_pct,
            _ingested_at, _source
        ) VALUES (
            source.borough, source.county_fips, source.state_fips,
            source.survey_year, source.total_population,
            source.median_household_income, source.population_below_poverty,
            source.total_housing_units, source.total_workers_commuting,
            source.poverty_rate_pct, source._ingested_at, source._source
        )
    """)

    return len(rows)


# =============================================================================
# MAIN
# =============================================================================
def run():
    log.info("=== UrbanPulse Census Ingestion ===")

    records = fetch_census_data()

    if not records:
        log.warning("No Census records returned. Check API key and parameters.")
        return

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        ensure_table_exists(cursor)
        rows_loaded = load_to_snowflake(records, cursor)
        conn.commit()
        log.info(f"=== SUCCESS: {rows_loaded} borough census records loaded ===")

    except Exception as e:
        conn.rollback()
        log.error(f"Census ingestion failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
