from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, get_current_context, task

from include.extract.loader import load_config, load_wave

WAREHOUSE_CONN_ID = "warehouse_postgres"


@dag(
    dag_id="load_raw_wave",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={"wave": 1},
    tags=["instacart", "el"],
)
def load_raw_wave():
    @task
    def load():
        wave = int(get_current_context()["params"]["wave"])
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        config = load_config()
        for table in config["tables"]:
            if table["load_mode"] == "wave":
                load_wave(hook, config, table, wave)

    load()


load_raw_wave()