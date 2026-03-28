"""
UrbanPulse — Census Ingestion DAG
Runs annually on January 1st.
Pulls ACS 5-year estimates and updates demographic dimensions.
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
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="urbanpulse_census_annual",
    default_args=default_args,
    description="Annual ingestion of US Census demographics for NYC boroughs",
    schedule="0 4 1 1 *",  # 4:00 AM UTC on January 1st
    catchup=False,
    max_active_runs=1,
    tags=["urbanpulse", "ingestion", "census"],
) as dag:

    ingest_census = BashOperator(
        task_id="ingest_census_demographics",
        bash_command="cd /home/otieno/urbanpulse && python ingestion/census.py",
        doc_md="Pulls ACS 5-year estimates for all NYC boroughs.",
    )

    dbt_staging_census = BashOperator(
        task_id="dbt_run_stg_census",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select stg_census --target prod",
        doc_md="Refreshes stg_census Bronze view.",
    )

    dbt_dim_neighborhood = BashOperator(
        task_id="dbt_run_dim_neighborhood",
        bash_command="cd /home/otieno/urbanpulse && dbt run --select dim_neighborhood --target prod",
        doc_md="Rebuilds dim_neighborhood with updated demographics.",
    )

    dbt_test_census = BashOperator(
        task_id="dbt_test_census_models",
        bash_command="cd /home/otieno/urbanpulse && dbt test --select stg_census dim_neighborhood --target prod",
        doc_md="Validates Census data quality after annual refresh.",
    )

    # Task sequence
    ingest_census >> dbt_staging_census >> dbt_dim_neighborhood >> dbt_test_census