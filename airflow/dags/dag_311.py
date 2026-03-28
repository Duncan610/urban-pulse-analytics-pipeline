"""
UrbanPulse — NYC 311 Ingestion DAG
Runs daily at 6:00 AM UTC (9:00 AM EAT).
Pulls last 3 days of 311 complaints (incremental mode).
Then triggers dbt to transform the new data.

Schedule: Daily
Retries: 3 (with 5 minute delays)
Owner: UrbanPulse Pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# DEFAULT ARGUMENTS
default_args = {
    "owner": "urbanpulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# DAG DEFINITION
with DAG(
    dag_id="urbanpulse_nyc_311_daily",
    default_args=default_args,
    description="Daily ingestion of NYC 311 service requests + dbt transform",
    schedule="0 6 * * *",          # 6:00 AM UTC = 9:00 AM EAT
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ingestion", "311"],
) as dag:

    # 
    # TASK 1: Ingest 311 data (incremental — last 3 days)
    ingest_311 = BashOperator(
        task_id="ingest_nyc_311_incremental",
        bash_command="cd /home/otieno/urbanpulse && python ingestion/nyc_311.py --mode incremental",
        doc_md="""
        Pulls NYC 311 service requests from the Socrata API.
        Uses incremental mode — only fetches records from last 3 days.
        MERGE logic in the script ensures no duplicates in the database.
        """,
    )

    # TASK 2: Check source freshness
    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command="cd /home/otieno/urbanpulse && dbt source freshness --target prod",
        doc_md="Checks that raw source tables have been updated recently.",
    )

    # TASK 3: Run dbt staging models
    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select staging --target prod",
        doc_md="Runs Bronze layer dbt models to clean and type raw 311 data.",
    )

    # TASK 4: Run dbt intermediate models
    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select intermediate --target prod",
        doc_md="Runs Silver layer dbt models — joins 311 with weather and demographics.",
    )

    # TASK 5: Run dbt marts models
    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select marts --target prod",
        doc_md="Runs Gold layer dbt models — final fact and dimension tables.",
    )

    # TASK 6: Run dbt tests
   
    dbt_test = BashOperator(
        task_id="dbt_test_all",
        bash_command="cd /home/otieno/urbanpulse && dbt test --target prod",
        doc_md="""
        Runs all dbt tests across Bronze, Silver, and Gold layers.
        This is the data quality gate — if tests fail, the DAG fails.
        """,
    )

    
    # TASK SEQUENCE

    (
        ingest_311
        >> dbt_freshness
        >> dbt_staging
        >> dbt_intermediate
        >> dbt_marts
        >> dbt_test
    )