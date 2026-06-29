"""
One-time warehouse setup (run before the first pipeline run):

    create raw tables  ->  upload reference CSVs to S3  ->  COPY them into raw

Invokes the src/ scripts as BashOperator tasks. Credentials come from the
injected .env (Snowflake) and the mounted ~/.aws SSO session (AWS).
"""

from __future__ import annotations

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PROJECT = "/usr/local/airflow"


@dag(
    dag_id="instacart_setup",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart", "setup"],
)
def instacart_setup():
    create_raw_tables = BashOperator(
        task_id="create_raw_tables",
        bash_command=f"cd {PROJECT} && python src/load_to_snowflake.py --setup",
    )
    upload_reference = BashOperator(
        task_id="upload_reference",
        bash_command=f"cd {PROJECT} && python src/upload_to_s3.py --full",
    )
    load_reference = BashOperator(
        task_id="load_reference",
        bash_command=f"cd {PROJECT} && python src/load_to_snowflake.py --full",
    )

    create_raw_tables >> upload_reference >> load_reference


instacart_setup()
