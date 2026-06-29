"""
Scheduled API ingestion (A5): Open Food Facts -> S3 -> Snowflake -> dbt.

    extract (incremental)  ->  load (COPY into VARIANT)  ->  dbt build (OFF models)

Runs daily; each run pulls only products modified since the last watermark, so
re-runs are cheap and idempotent. This is the API counterpart to the wave-based
instacart_pipeline.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PROJECT = "/usr/local/airflow"
DBT_DIR = f"{PROJECT}/dbt/instacart"
DBT_BIN = f"{PROJECT}/dbt_venv/bin/dbt"


@dag(
    dag_id="off_api_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart", "api", "open-food-facts"],
)
def off_api_pipeline():
    extract = BashOperator(
        task_id="extract_api",
        bash_command=f"cd {PROJECT} && python src/extract_api.py",
    )
    load = BashOperator(
        task_id="load_api",
        bash_command=f"cd {PROJECT} && python src/load_api_to_snowflake.py",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --select stg_off_products+ --profiles-dir . --project-dir .",
    )

    extract >> load >> dbt_build


off_api_pipeline()
