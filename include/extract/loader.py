"""
Config-driven Extract & Load into the `raw` schema.

  - full : TRUNCATE + COPY the whole source file (reference data)
  - wave : DELETE the wave's rows + COPY that wave's file (transactional data)

Both modes are idempotent: re-running replaces, never duplicates.
"""

from __future__ import annotations

import os

import yaml
from airflow.providers.postgres.hooks.postgres import PostgresHook

INCLUDE_DIR = "/usr/local/airflow/include"
DATA_DIR = os.path.join(INCLUDE_DIR, "data")
CONFIG_PATH = os.path.join(INCLUDE_DIR, "extract", "extract_tables.yml")


def load_config(path: str = CONFIG_PATH) -> dict:
    with open(path) as fh:
        return yaml.safe_load(fh)


def _q(schema: str, table: str) -> str:
    return f"{schema}.{table}"


def _copy_sql(schema: str, table: dict) -> str:
    cols = ", ".join(c["name"] for c in table["columns"])
    return f"COPY {_q(schema, table['name'])} ({cols}) FROM STDIN WITH (FORMAT csv, HEADER true)"


def create_raw_tables(hook: PostgresHook, config: dict) -> None:
    schema = config["target_schema"]
    hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    for t in config["tables"]:
        cols = ",\n  ".join(f"{c['name']} {c['type']}" for c in t["columns"])
        hook.run(
            f"CREATE TABLE IF NOT EXISTS {_q(schema, t['name'])} (\n"
            f"  {cols},\n"
            f"  _loaded_at timestamptz NOT NULL DEFAULT now()\n"
            f");"
        )
    print(f"Ensured {len(config['tables'])} raw tables in schema '{schema}'.")


def load_full(hook: PostgresHook, config: dict, table: dict) -> None:
    schema = config["target_schema"]
    path = os.path.join(DATA_DIR, table["source_file"])
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Source file not found: {path}")
    hook.run(f"TRUNCATE {_q(schema, table['name'])};")
    hook.copy_expert(_copy_sql(schema, table), path)
    print(f"Loaded full table '{table['name']}' from {table['source_file']}.")


def load_wave(hook: PostgresHook, config: dict, table: dict, wave: int) -> None:
    schema = config["target_schema"]
    wave_col = table["wave_column"]
    path = os.path.join(DATA_DIR, table["wave_dir"], f"{table['name']}_{wave:03d}.csv")

    # Idempotency: clear this wave's rows first.
    hook.run(
        f"DELETE FROM {_q(schema, table['name'])} WHERE {wave_col} = %(wave)s;",
        parameters={"wave": wave},
    )

    # Some waves legitimately have no file (e.g. no train orders at low order_number).
    if not os.path.isfile(path):
        print(f"Wave {wave:03d}: no file for '{table['name']}', skipping.")
        return

    hook.copy_expert(_copy_sql(schema, table), path)
    print(f"Wave {wave:03d}: loaded '{table['name']}' from {os.path.basename(path)}.")