"""
API ingestion (A3) - COPY the landed JSON into a Snowflake VARIANT table.

Schema-on-read: we keep the raw product object in a VARIANT column and only
pull out last_modified_t (for the incremental watermark) at load time. dbt does
the real flattening downstream.

Append-only + Snowflake's load history (FORCE=false) means re-running never
re-loads the same S3 file; dbt deduplicates by product code keeping the latest.

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
STAGE = "s3_stage"            # existing stage at s3://<bucket>/raw/


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
    # s3_prefix is 'raw/off_products_api'; the stage root is 'raw/', so the
    # path under the stage is everything after 'raw/'.
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
        # Idempotent (create table if not exists), so the DAG never needs a
        # separate setup step. The --setup flag is kept for parity/clarity.
        create_table(cur, cfg["raw_table"])
        print("Loading API JSON:")
        copy_in(cur, cfg)
    finally:
        cur.close()
        conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
