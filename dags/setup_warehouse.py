from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

from include.extract.loader import create_raw_tables, load_config, load_full

WAREHOUSE_CONN_ID = "warehouse_postgres"


@dag(
    dag_id="setup_warehouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart", "el", "setup"],
)
def setup_warehouse():
    @task
    def build():
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        config = load_config()
        create_raw_tables(hook, config)
        for table in config["tables"]:
            if table["load_mode"] == "full":
                load_full(hook, config, table)

    build()


setup_warehouse()