"""
UrbanPulse — OpenWeather Ingestion Script
==========================================
Pulls current weather conditions for NYC from the OpenWeather API
and lands them into Snowflake RAW schema.

This runs on a schedule (daily via Airflow later) and accumulates
weather observations over time — building the dataset we need to
correlate weather with 311 complaint volumes.

Run locally:
    python ingest_weather.py

Author: UrbanPulse Pipeline
"""

import os
import logging
from datetime import datetime

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
# NYC boroughs — we pull weather for each borough separately
# This gives us borough-level weather granularity to match 311 data

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
OPENWEATHER_ENDPOINT = "https://api.openweathermap.org/data/2.5/weather"

# Pull weather for all 5 NYC boroughs
# Lat/lon coords for each borough center
NYC_BOROUGHS = [
    {"name": "Manhattan",     "lat": 40.7831, "lon": -73.9712},
    {"name": "Brooklyn",      "lat": 40.6782, "lon": -73.9442},
    {"name": "Queens",        "lat": 40.7282, "lon": -73.7949},
    {"name": "Bronx",         "lat": 40.8448, "lon": -73.8648},
    {"name": "Staten Island", "lat": 40.5795, "lon": -74.1502},
]

SNOWFLAKE_CONFIG = {
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "role":      os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": "URBANPULSE_LOADING_WH",
    "database":  "URBANPULSE",
    "schema":    "RAW",
}


# SNOWFLAKE: CREATE RAW TABLE
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS URBANPULSE.RAW.weather_raw (
    -- Location
    borough                 VARCHAR(50),
    city_name               VARCHAR(100),
    lat                     FLOAT,
    lon                     FLOAT,
    country                 VARCHAR(10),

    -- Weather conditions
    weather_main            VARCHAR(100),   -- e.g. Rain, Clear, Clouds
    weather_description     VARCHAR(200),   -- e.g. 'light rain', 'broken clouds'
    weather_icon            VARCHAR(20),

    -- Temperature (Celsius)
    temp_celsius            FLOAT,
    feels_like_celsius      FLOAT,
    temp_min_celsius        FLOAT,
    temp_max_celsius        FLOAT,

    -- Atmospheric
    pressure_hpa            FLOAT,
    humidity_pct            FLOAT,
    visibility_meters       FLOAT,
    wind_speed_ms           FLOAT,          -- metres per second
    wind_direction_deg      FLOAT,
    cloudiness_pct          FLOAT,

    -- Precipitation (last hour, mm) — NULL if no precipitation
    rain_1h_mm              FLOAT,
    snow_1h_mm              FLOAT,

    -- Timestamps
    observation_timestamp   TIMESTAMP_NTZ,  -- When OpenWeather recorded this
    sunrise_timestamp       TIMESTAMP_NTZ,
    sunset_timestamp        TIMESTAMP_NTZ,

    -- Pipeline metadata
    _ingested_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _source                 VARCHAR(100) DEFAULT 'openweather_api'
)
COMMENT = 'Raw weather observations per NYC borough. Loaded daily by UrbanPulse pipeline.';
"""


# FETCH FROM OPENWEATHER API
def fetch_weather(borough: dict) -> dict:
    """
    Fetch current weather for a single borough.

    Args:
        borough: dict with name, lat, lon

    Returns:
        Parsed weather record dict ready for Snowflake
    """
    params = {
        "lat":   borough["lat"],
        "lon":   borough["lon"],
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",   # Celsius, m/s
    }

    response = requests.get(OPENWEATHER_ENDPOINT, params=params, timeout=30)

    if response.status_code != 200:
        raise ValueError(
            f"OpenWeather API error for {borough['name']}: "
            f"{response.status_code} — {response.text[:300]}"
        )

    data = response.json()

    # Parse the nested JSON into a flat record
    # The API returns deeply nested data — we flatten it here
    record = {
        "borough":               borough["name"],
        "city_name":             data.get("name", ""),
        "lat":                   data["coord"]["lat"],
        "lon":                   data["coord"]["lon"],
        "country":               data["sys"].get("country", ""),
        "weather_main":          data["weather"][0]["main"],
        "weather_description":   data["weather"][0]["description"],
        "weather_icon":          data["weather"][0]["icon"],
        "temp_celsius":          data["main"]["temp"],
        "feels_like_celsius":    data["main"]["feels_like"],
        "temp_min_celsius":      data["main"]["temp_min"],
        "temp_max_celsius":      data["main"]["temp_max"],
        "pressure_hpa":          data["main"]["pressure"],
        "humidity_pct":          data["main"]["humidity"],
        "visibility_meters":     data.get("visibility"),
        "wind_speed_ms":         data["wind"].get("speed"),
        "wind_direction_deg":    data["wind"].get("deg"),
        "cloudiness_pct":        data["clouds"].get("all"),
        # Precipitation — only present if it rained/snowed
        "rain_1h_mm":            data.get("rain", {}).get("1h"),
        "snow_1h_mm":            data.get("snow", {}).get("1h"),
        # Convert Unix timestamps to ISO strings for Snowflake
        "observation_timestamp": datetime.utcfromtimestamp(data["dt"]).isoformat(),
        "sunrise_timestamp":     datetime.utcfromtimestamp(data["sys"]["sunrise"]).isoformat(),
        "sunset_timestamp":      datetime.utcfromtimestamp(data["sys"]["sunset"]).isoformat(),
        "_ingested_at":          datetime.utcnow().isoformat(),
        "_source":               "openweather_api",
    }

    log.info(
        f"  {borough['name']}: {record['weather_main']} "
        f"{record['temp_celsius']}°C, "
        f"humidity {record['humidity_pct']}%"
    )
    return record


def fetch_all_boroughs() -> list[dict]:
    """Fetch weather for all 5 NYC boroughs."""
    records = []
    for borough in NYC_BOROUGHS:
        record = fetch_weather(borough)
        records.append(record)
    return records


# LOAD INTO SNOWFLAKE
def get_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


def ensure_table_exists(cursor):
    log.info("Ensuring weather RAW table exists...")
    cursor.execute(CREATE_TABLE_SQL)
    log.info("Table ready: URBANPULSE.RAW.weather_raw")


def load_to_snowflake(records: list[dict], cursor) -> int:
    """Insert weather records into Snowflake."""
    rows = [
        (
            r["borough"], r["city_name"], r["lat"], r["lon"], r["country"],
            r["weather_main"], r["weather_description"], r["weather_icon"],
            r["temp_celsius"], r["feels_like_celsius"],
            r["temp_min_celsius"], r["temp_max_celsius"],
            r["pressure_hpa"], r["humidity_pct"], r["visibility_meters"],
            r["wind_speed_ms"], r["wind_direction_deg"], r["cloudiness_pct"],
            r["rain_1h_mm"], r["snow_1h_mm"],
            r["observation_timestamp"], r["sunrise_timestamp"], r["sunset_timestamp"],
            r["_ingested_at"], r["_source"],
        )
        for r in records
    ]

    cursor.executemany(
        """
        INSERT INTO URBANPULSE.RAW.weather_raw (
            borough, city_name, lat, lon, country,
            weather_main, weather_description, weather_icon,
            temp_celsius, feels_like_celsius, temp_min_celsius, temp_max_celsius,
            pressure_hpa, humidity_pct, visibility_meters,
            wind_speed_ms, wind_direction_deg, cloudiness_pct,
            rain_1h_mm, snow_1h_mm,
            observation_timestamp, sunrise_timestamp, sunset_timestamp,
            _ingested_at, _source
        ) VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
        """,
        rows,
    )
    return len(rows)


# MAIN
def run():
    log.info("=== UrbanPulse Weather Ingestion ===")

    records = fetch_all_boroughs()

    conn   = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        ensure_table_exists(cursor)
        rows_loaded = load_to_snowflake(records, cursor)
        conn.commit()
        log.info(f"=== SUCCESS: {rows_loaded} borough weather records loaded ===")

    except Exception as e:
        conn.rollback()
        log.error(f"Weather ingestion failed: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    run()
