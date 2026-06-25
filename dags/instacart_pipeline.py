"""
End-to-end pipeline for one order_number wave:

    upload wave to S3  ->  COPY wave into raw  ->  dbt build (transform + test)

Trigger with a run config to pick the wave, e.g. {"wave": 2}. Each task is the
same src/ script you ran by hand in Steps 2-5; a failing dbt test fails the DAG.
Run `instacart_setup` once before the first wave.
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PROJECT = "/usr/local/airflow"
DBT_DIR = f"{PROJECT}/dbt/instacart"
DBT_BIN = f"{PROJECT}/dbt_venv/bin/dbt"


@dag(
    dag_id="instacart_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"wave": 1},
    tags=["instacart", "pipeline"],
)
def instacart_pipeline():
    upload_wave = BashOperator(
        task_id="upload_wave",
        bash_command="cd " + PROJECT + " && python src/upload_to_s3.py --wave {{ params.wave }}",
    )
    load_wave = BashOperator(
        task_id="load_wave",
        bash_command="cd " + PROJECT + " && python src/load_to_snowflake.py --wave {{ params.wave }}",
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --profiles-dir . --project-dir .",
    )

    upload_wave >> load_wave >> dbt_build


instacart_pipeline()
