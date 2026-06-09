"""
End-to-end pipeline for one order_number wave:

    load wave into raw  ->  dbt build (transform + test)

Trigger with a run config to choose the wave, e.g. {"wave": 3}.
Reference dimensions and the raw tables are created once by the separate
`setup_warehouse` DAG, which must run before the first pipeline run.
"""

from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, get_current_context, task

from include.extract.loader import load_config, load_wave

WAREHOUSE_CONN_ID = "warehouse_postgres"
DBT_DIR = "/usr/local/airflow/include/dbt/instacart"
DBT_BIN = "/usr/local/airflow/dbt_venv/bin/dbt"


@dag(
    dag_id="instacart_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"wave": 3},
    tags=["instacart", "pipeline"],
)
def instacart_pipeline():
    @task
    def load_raw():
        wave = int(get_current_context()["params"]["wave"])
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        config = load_config()
        for table in config["tables"]:
            if table["load_mode"] == "wave":
                load_wave(hook, config, table, wave)

    # `dbt build` runs models AND tests in one shot; a failing test fails the task.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} build --profiles-dir .",
    )

    load_raw() >> dbt_build


instacart_pipeline()