from __future__ import annotations

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

WAREHOUSE_CONN_ID = "warehouse_postgres"


@dag(
    dag_id="ping_warehouse",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["instacart", "setup"],
)
def ping_warehouse():
    @task
    def check_connection():
        hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
        version = hook.get_first("SELECT version();")[0]
        schemas = [r[0] for r in hook.get_records(
            "SELECT schema_name FROM information_schema.schemata ORDER BY 1;"
        )]
        print(f"Connected. Server: {version}")
        print(f"Schemas: {schemas}")
        assert "raw" in schemas, "raw schema missing!"

    check_connection()


ping_warehouse()