# Enterprise-Retail-Data-Warehouse-Pipeline

A production-like end-to-end data engineering project simulating a retail data platform using Instacart dataset. Includes Dockerized Airflow + Postgres stack, dbt-driven ELT pipeline, incremental dimensional modelling (Kimball star schema), and batch-simulated ingestion for realistic warehouse behaviour.

Built environment
A dedicated warehouse Postgres runs alongside Airflow, the raw schema is created automatically on first boot, and an Airflow connection + smoke-test DAG confirm end-to-end connectivity.

The project was initialized using Astro CLI to scaffold a local Airflow development environment.
-- astro dev init

-- docker-compose.override.yml adds a warehouse Postgres service next to the Airflow components. It publishes host port 5433 (Airflow's own metadata Postgres keeps 5432) and persists data in a named volume.

-- sql/00_init_schemas.sql is mounted into /docker-entrypoint-initdb.d, so the raw schema is created the first time the warehouse volume initialises.

-- airflow_settings.yaml defines the warehouse_postgres connection. Airflow reaches the warehouse via host.docker.internal:5433 (the two run on separate Docker networks, so the host loopback is used rather than the service name).

-- dags/ping_warehouse.py is a smoke-test DAG that queries the warehouse and asserts the raw schema exists.
