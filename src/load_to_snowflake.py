"""
Load S3 files into Snowflake `raw` tables via COPY INTO.

Idempotent loads:
  - full : TRUNCATE + COPY      (reference data)
  - wave : DELETE wave + COPY   (transactional data)

COPY uses FORCE = TRUE so that re-reading a file after a TRUNCATE/DELETE actually
re-loads it (otherwise Snowflake's load history would skip an "already loaded"
file, breaking idempotency).

Credentials come from .env. Run AFTER the stage exists (snowflake/03_stage.sql).

Usage:
    python src/load_to_snowflake.py --setup        # create raw tables (once)
    python src/load_to_snowflake.py --full         # load reference tables
    python src/load_to_snowflake.py --wave 1       # load wave 001 of wave tables
    python src/load_to_snowflake.py --setup --full --wave 1   # all three, in order
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import snowflake.connector
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "tables.yml"
SCHEMA = "raw"
STAGE = "s3_stage"
FILE_FORMAT = "csv_ff"


def load_config() -> dict:
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)


def connect():
    load_dotenv(ROOT / ".env")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "INSTACART_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INSTACART_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "INSTACART"),
        schema=SCHEMA,
    )


def create_raw_tables(cur, config) -> None:
    for t in config["tables"]:
        cols = ",\n  ".join(f"{c['name']} {c['type']}" for c in t["columns"])
        cur.execute(
            f"create table if not exists {SCHEMA}.{t['name']} (\n"
            f"  {cols},\n"
            f"  _loaded_at timestamp_tz default current_timestamp()\n"
            f")"
        )
        print(f"  ensured table {SCHEMA}.{t['name']}")


def _copy_into(cur, table, stage_path) -> None:
    # Explicit column list + a $1..$N projection from the stage, so the table's
    # extra _loaded_at column keeps its default instead of expecting a CSV value.
    cols = [c["name"] for c in table["columns"]]
    col_list = ", ".join(cols)
    select_list = ", ".join(f"${i + 1}" for i in range(len(cols)))
    cur.execute(
        f"copy into {SCHEMA}.{table['name']} ({col_list})\n"
        f"from (select {select_list} from @{STAGE}/{stage_path})\n"
        f"file_format = (format_name = {FILE_FORMAT})\n"
        f"force = true\n"
        f"on_error = abort_statement"
    )


def load_full(cur, table) -> None:
    cur.execute(f"truncate table {SCHEMA}.{table['name']}")
    _copy_into(cur, table, f"{table['name']}/{table['source_file']}")
    print(f"  loaded full  {table['name']}")


def load_wave(cur, table, wave) -> None:
    wave_col = table["wave_column"]
    cur.execute(
        f"delete from {SCHEMA}.{table['name']} where {wave_col} = %s", (wave,)
    )
    path = f"{table['name']}/{table['name']}_{wave:03d}.csv"
    # Some waves legitimately have no file (e.g. no train orders that low); a
    # COPY of a missing path errors, so guard by LISTing it first.
    cur.execute(f"list @{STAGE}/{path}")
    if not cur.fetchall():
        print(f"  wave {wave:03d}: no file for {table['name']}, skip")
        return
    _copy_into(cur, table, path)
    print(f"  loaded wave {wave:03d}  {table['name']}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Load S3 files into Snowflake raw.")
    ap.add_argument("--setup", action="store_true", help="create raw tables")
    ap.add_argument("--full", action="store_true", help="load reference tables")
    ap.add_argument("--wave", type=int, help="load this wave of the wave tables")
    args = ap.parse_args()

    if not (args.setup or args.full or args.wave is not None):
        ap.error("nothing to do: pass --setup and/or --full and/or --wave N")

    config = load_config()
    conn = connect()
    cur = conn.cursor()
    try:
        if args.setup:
            print("Creating raw tables:")
            create_raw_tables(cur, config)
        if args.full:
            print("Loading reference (full) tables:")
            for t in config["tables"]:
                if t["load_mode"] == "full":
                    load_full(cur, t)
        if args.wave is not None:
            print(f"Loading wave {args.wave:03d}:")
            for t in config["tables"]:
                if t["load_mode"] == "wave":
                    load_wave(cur, t, args.wave)
    finally:
        cur.close()
        conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
