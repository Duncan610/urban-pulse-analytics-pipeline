"""
UrbanPulse — Weather Ingestion DAG
Runs daily at 5:00 AM UTC (8:00 AM EAT).
Pulls current weather for all NYC boroughs before 311 DAG runs.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "urbanpulse",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="urbanpulse_weather_daily",
    default_args=default_args,
    description="Daily ingestion of NYC weather data per borough",
    schedule="0 5 * * *",  # 5:00 AM UTC = 8:00 AM EAT
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ingestion", "weather"],
) as dag:

    ingest_weather = BashOperator(
        task_id="ingest_weather_all_boroughs",
        bash_command="cd /home/otieno/urbanpulse && python ingestion/weather.py",
        doc_md="Pulls current weather from OpenWeather API for all NYC boroughs.",
    )

    dbt_staging_weather = BashOperator(
        task_id="dbt_run_stg_weather",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select stg_weather --target prod",
        doc_md="Refreshes the stg_weather Bronze view with latest observations.",
    )

    # Task sequence
    ingest_weather >> dbt_staging_weather