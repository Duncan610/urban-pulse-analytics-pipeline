# 🏙️ UrbanPulse — NYC City Intelligence Platform

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://urban-pulse-analytics-pipeline.streamlit.app/)
![dbt CI](https://github.com/Duncan610/urban-pulse-analytics-pipeline/actions/workflows/dbt_ci.yml/badge.svg)

New York City fields 22,000+ service requests every three days. But not every
neighborhood gets the same response.

The Bronx, with a median household income of $47,036 and a 26.3% poverty rate,
files more complaints per capita than Manhattan. Yet the data shows measurable
differences in how quickly those complaints get resolved.

UrbanPulse builds the analytics infrastructure to make this visible. It ingests
live data from three public APIs, joins them across a medallion architecture in
Snowflake, and surfaces the findings through a live dashboard that anyone can access.

**The central finding:** Borough income correlates with city service response
patterns. The $52,000 income gap between Manhattan and the Bronx is not just
an economic statistic — it shows up in the data.

---

## 📊 Live Dashboard

![Dashboard Metrics and Title](images/streamlitdashmetricsandtitle.png)

![Borough Analysis](images/streamlitdashboardboroughanalysis.png)

![Income vs Response Time](images/streamlitdashincomevsresponsetime.png)

![Top 15 Complaint Types](images/streamlitdashtop15complaints.png)

![Daily Complaint Volume](images/streamlitdashdailycomplaintvolume.png)

---

## 🔍 Key Findings

- **$52,000 income gap** between Manhattan and the Bronx — two boroughs in the same city
- **The Bronx** (26.3% poverty rate, LOW income bracket) leads all boroughs in complaint volume
- **Response times vary by borough income** — visualised in the income vs response time scatter plot
- **135 unique complaint types** processed through the pipeline with response time tracking
- **54% resolution rate** across all complaints in the dataset

---

### 🏗️ Architecture
```mermaid
flowchart TD
    A[NYC 311 API\nSocrata REST] -->|Python ingestion| D
    B[OpenWeather API\nCurrent weather] -->|Python ingestion| D
    C[US Census Bureau\nACS5 demographics] -->|Python ingestion| D

    D[(Snowflake\nRAW schema)] -->|dbt Bronze| E

    E[stg_nyc_311\nstg_weather\nstg_census] -->|dbt Silver| F

    F[int_complaints_weather\nint_complaints_demographics\nint_complaints_response_time] -->|dbt Gold| G

    G[fct_service_requests\nfct_daily_borough_summary\ndim_boroughs · dim_date] -->|Streamlit| H

    H[Live Dashboard\nurban-pulse-analytics-pipeline.streamlit.app]

    I[GitHub Actions CI/CD\ndbt test on every push] -.->|validates| G
```

---

## 🛠️ Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python (requests, snowflake-connector) | Full control over API pagination, retry logic, and error handling. NYC 311 and Census have no native Airbyte connectors — Python is the right tool. |
| Warehouse | Snowflake | Separates compute from storage. Two warehouses (loading + transform) with 60s auto-suspend keep costs near zero. |
| Transformation | dbt Core | Industry standard for analytics engineering. SQL-first, version-controlled, self-documenting, built-in testing. |
| Orchestration | Apache Airflow | DAG-based scheduling with retry logic and task dependencies. Weather DAG runs 1 hour before 311 to ensure join data availability. |
| Dashboard | Streamlit | Python-native. Connects directly to Snowflake. Fast to build and deploy. |
| CI/CD | GitHub Actions | dbt tests run on every pull request. Bad data never reaches production. |

---

## 📂 Data Sources

| Source | Records | Ingestion Mode |
|---|---|---|
| NYC 311 Service Requests | 22,000+ | Daily incremental (3-day lookback) |
| OpenWeather API | 5 rows/day (one per borough) | Daily full refresh |
| US Census Bureau ACS5 | 5 rows (one per borough) | Annual |

---

## ❄️ Snowflake Setup

![Creating Role and Warehouse](images/snowflakecreatingroleandwarehouse.png)

![Create Schema and Grant Usage](images/snowflakecreateschemaandgrantusageonschemas.png)

![Snowflake Schemas](images/snowflakeschemas.png)

![Snowflake Warehouses](images/snowflakewarehouses.png)

**Production-grade Snowflake environment:**
- Dedicated `urbanpulse_role` following least-privilege principles
- Two separate compute warehouses — `urbanpulse_loading_wh` for ingestion, `urbanpulse_transform_wh` for dbt
- Both warehouses with 60-second auto-suspend to control costs
- Four-schema medallion architecture: RAW → STAGING → INTERMEDIATE → MARTS
- Dedicated service user `urbanpulse_svc` for pipeline connections
- Pipelines never run as ACCOUNTADMIN

---

## 🐍 Ingestion Layer

![Loading Weather Data via Terminal](images/loadingweatherdataonsnowflakeviaterminal.png)

![Weather Data on Snowflake](images/loadedweatherpyonsnowflake.png)

![Loading Census Data via Terminal](images/loadingcensusdataviaterminalrun.png)

![Census Data on Snowflake](images/loadcensusdataonsnowflake.png)

![Loading NYC 311 Data via Terminal](images/loadednyc311datafromterminaltosnowflake.png)

![NYC 311 Data on Snowflake](images/loadednyc311dataonsnowflake.png)

**Key engineering decisions:**

**MERGE not INSERT** — Pipelines are idempotent. Running twice produces the same result — no duplicates.

**3-day lookback window** — NYC Open Data records sometimes appear 24-48 hours after the service request was created. A 3-day lookback catches late-arriving records on every incremental run.

**Retry logic** — All scripts retry 3 times on transient errors (503s, timeouts) before failing. API outages do not break the pipeline.

**Raw data stored as strings** — Type casting happens in dbt staging, not ingestion. This preserves the original source data and makes debugging easier when something breaks downstream.

---

## 🔄 dbt Transformation Layer

### dbt Debug — All Checks Passed

![dbt Debug All Checks Passed](images/dbtdebugallcheckspassed.png)

### Bronze Layer (Staging)

![dbt Staging Run Passed](images/stagingrunupdated.png)

![dbt Staging Tests Green](images/stagingtestupdated.png)

**Three staging models — one per source:**
- `stg_nyc_311` — casts dates to timestamps, standardises borough names to uppercase, filters broken records
- `stg_weather` — coalesces NULL precipitation to 0, adds boolean flags: `is_rainy`, `is_cold`, `is_hot`
- `stg_census` — adds `income_bracket` (LOW/MEDIUM/HIGH) and `population_tier` classifications

### Silver Layer (Intermediate)

![Intermediate Files](images/intermediatefiles.png)

![dbt Intermediate Run](images/intermediaterunupdated.png)

![dbt Intermediate Tests](images/intermediatetestupdate.png)

**Three intermediate models — where the data sources meet:**
- `int_complaints_weather` — joins 311 complaints with weather by borough and date
- `int_complaints_demographics` — attaches Census demographics to every complaint
- `int_complaints_response_time` — calculates response time in hours, adds speed bucket classification

### Gold Layer (Marts)

![Marts Run Successful](images/dbtrunmartsupdatedupdate.png)

![Marts Test Successful](images/dbttestmartsupdated.png)


![Tables in Snowflake](images/tablesinsnowflake.png)

**Four mart models — business-facing final tables:**
- `dim_date` — calendar dimension 2020-2026 (2,557 rows)
- `dim_boroughs` — borough demographics with surrogate keys and income rankings
- `fct_service_requests` — main fact table, one row per complaint fully enriched (incremental)
- `fct_daily_borough_summary` — daily borough summary aggregation powering time series charts

### dbt Lineage Graph

![dbt Lineage Graph](images/dbtdocsupdated.png)

![All Models List](images/dbtprojectlist.png)

![fct_service_requests Model Description](images/factservicerequests.png)

![stg_nyc_311 Model Description](images/stgnyc.png)

### Data Quality — 57/57 Tests Passing

```
Bronze (Staging):      24/24 ✅
Silver (Intermediate): 19/19 ✅
Gold (Marts):          14/14 ✅
─────────────────────────────
Total:                 57/57 ✅
```

**Tests implemented:**
- `unique` + `not_null` on every primary key
- `not_null` on every column used in downstream joins
- `accepted_values` on borough — only the 5 valid NYC boroughs pass through
- `accepted_values` on `income_bracket` — only LOW, MEDIUM, HIGH allowed
- `accepted_values` on `response_speed_bucket` — validates all response categories
- `accepted_values` on `season` in dim_date — Winter, Spring, Summer, Fall only

---

## 🔐 GitHub Actions CI/CD

![GitHub Action Secrets](images/githubactionsecrets.png)

Every pull request to `main` automatically:
1. Installs dbt and dependencies
2. Writes `profiles.yml` from GitHub Secrets — credentials never in code
3. Runs `dbt compile` — catches SQL syntax errors
4. Runs `dbt test` — all 57 tests must pass before merging

---

## 🌊 Airflow Orchestration

Three DAGs orchestrate the full pipeline:

| DAG | Schedule | What it does |
|---|---|---|
| `urbanpulse_weather_daily` | Daily 5AM UTC | Ingests weather for all 5 boroughs |
| `urbanpulse_nyc_311_daily` | Daily 6AM UTC | Ingests 311 + runs full dbt pipeline |
| `urbanpulse_census_annual` | Jan 1 annually | Updates demographic dimension |

**Why weather runs before 311:** The 311 DAG joins complaints with weather by borough and date. Running weather one hour earlier guarantees today's weather exists when the join runs.

**`catchup=False`:** If Airflow goes offline for 3 days and restarts, it does not replay missed runs. The incremental ingestion lookback handles historical gaps.

---

## 🚀 Running Locally

### Prerequisites
- Python 3.11+
- Snowflake account (free trial works)
- API keys: OpenWeather, NYC Open Data, Census Bureau

### Setup

```bash
git clone https://github.com/Duncan610/urban-pulse-analytics-pipeline.git
cd urban-pulse-analytics-pipeline
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your credentials
```

### Run with Docker

```bash
docker compose up weather     # ingest weather data
docker compose up nyc311      # ingest 311 data (incremental)
docker compose up census      # ingest census data
```

### Run Manually

```bash
python ingestion/weather.py
python ingestion/census.py
python ingestion/nyc_311.py --mode historical
python ingestion/nyc_311.py --mode incremental
```

### Run dbt

```bash
cd urbanpulse
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### Run Dashboard

```bash
streamlit run streamlit/app.py
```

---

## 🗂️ Repository Structure

```
urban-pulse-analytics-pipeline/
├── ingestion/
│   ├── nyc_311.py
│   ├── weather.py
│   ├── census.py
│   └── utils.py
├── urbanpulse/
│   ├── models/
│   │   ├── staging/        ← stg_nyc_311, stg_weather, stg_census
│   │   ├── intermediate/   ← int_complaints_weather, int_complaints_demographics,
│   │   │                      int_complaints_response_time
│   │   └── marts/          ← fct_service_requests, fct_daily_borough_summary,
│   │                          dim_boroughs, dim_date
│   ├── macros/             ← generate_schema_name
│   └── dbt_project.yml
├── airflow/dags/           ← dag_weather, dag_nyc_311, dag_census
├── streamlit/app.py
├── docker-compose.yml
├── .github/workflows/
├── .env.example
└── README.md
```

---

## 💡 Key Engineering Decisions

**Why Python ingestion over Airbyte?**
NYC Open Data (Socrata) and the US Census Bureau API don't have native Airbyte connectors. Building custom HTTP connectors in Airbyte's UI means writing the same ingestion logic but buried inside a tool that's harder to debug and version control. Python gives full observability, testability, and control.

**Why a custom `generate_schema_name` macro?**
dbt's default behaviour concatenates the profile's default schema with the custom schema name — producing `STAGING_intermediate` instead of `INTERMEDIATE`. A custom macro overrides this to enforce clean medallion layer separation.

**Why incremental models in the Gold layer?**
`fct_service_requests` processes only new records on each run using a 3-day lookback window. On a dataset growing by 7,000+ rows daily, this reduces transformation time significantly.

**Why LEFT JOINs in the Silver layer?**
Complaint records should never be lost because weather data is missing for a specific date. LEFT JOINs in `int_complaints_weather` keep all 311 records intact — missing weather data produces NULLs, not missing rows.

---

## 👤 Author

**Duncan Otieno**
[GitHub](https://github.com/Duncan610) · [LinkedIn](https://www.linkedin.com/in/duncan-otieno)

*Built as a portfolio project demonstrating production-grade analytics engineering.*

---

*Stack: Python · Snowflake · dbt Core · Apache Airflow · Streamlit · GitHub Actions*
