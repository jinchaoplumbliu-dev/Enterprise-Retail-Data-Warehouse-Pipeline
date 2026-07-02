"""
COPY the landed Open Food Facts JSON into a Snowflake VARIANT table.

The raw product object stays in a VARIANT column; only last_modified_t is
pulled out at load time for the incremental watermark, dbt flattens the rest.
The table is append-only and the COPY relies on Snowflake's load history
(FORCE defaults to false), so re-running never re-loads an S3 file that is
already in. Dedup by product code happens in dbt.

Usage:
    python src/load_api_to_snowflake.py --setup    # create the VARIANT table
    python src/load_api_to_snowflake.py            # COPY new JSON files in
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import snowflake.connector
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "api_sources.yml"
SCHEMA = "raw"
STAGE = "s3_stage"  # existing stage at s3://<bucket>/raw/


def load_config() -> dict:
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)["sources"]["off_products"]


def connect():
    load_dotenv(ROOT / ".env")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"], role=os.environ.get("SNOWFLAKE_ROLE", "INSTACART_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INSTACART_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "INSTACART"), schema=SCHEMA,
    )


def create_table(cur, table: str) -> None:
    cur.execute(
        f"create table if not exists {SCHEMA}.{table} (\n"
        f"  payload          variant,\n"
        f"  last_modified_t  number,\n"
        f"  _loaded_at       timestamp_tz default current_timestamp()\n"
        f")"
    )
    print(f"  ensured table {SCHEMA}.{table}")


def copy_in(cur, cfg: dict) -> None:
    table = cfg["raw_table"]
    # the stage root is already raw/, so drop that from s3_prefix
    subpath = cfg["s3_prefix"].split("/", 1)[1]
    cur.execute(
        f"copy into {SCHEMA}.{table} (payload, last_modified_t)\n"
        f"from (select $1, $1:last_modified_t::number from @{STAGE}/{subpath}/)\n"
        f"file_format = (type = 'JSON' strip_outer_array = true)\n"
        f"on_error = abort_statement"
    )
    loaded = cur.fetchall()
    print(f"  COPY into {SCHEMA}.{table}: {len(loaded)} file(s) processed")


def main() -> None:
    ap = argparse.ArgumentParser(description="Load OFF JSON into Snowflake VARIANT.")
    ap.add_argument("--setup", action="store_true", help="create the VARIANT table")
    args = ap.parse_args()

    cfg = load_config()
    conn = connect()
    cur = conn.cursor()
    try:
        # create table if not exists, so the DAG needs no separate setup step
        create_table(cur, cfg["raw_table"])
        print("Loading API JSON:")
        copy_in(cur, cfg)
    finally:
        cur.close()
        conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
