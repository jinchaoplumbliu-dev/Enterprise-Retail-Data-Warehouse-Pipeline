"""
Upload the source CSVs to the S3 landing zone.

Files go to s3://<bucket>/<s3_prefix>/<table>/<file>.csv, one prefix per table
so the Snowflake stage can COPY each table from its own folder.

Needs an active SSO session for the AWS_PROFILE named in .env
(`aws sso login --profile <name>` first).

Usage:
    python src/upload_to_s3.py            # reference tables + all waves
    python src/upload_to_s3.py --full     # only the 3 reference tables
    python src/upload_to_s3.py --wave 1   # only wave 001 of the wave tables
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import boto3
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
CONFIG_PATH = ROOT / "config" / "tables.yml"


def load_config() -> dict:
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)


def make_s3_client() -> tuple:
    load_dotenv(ROOT / ".env")
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
    return session.client("s3"), os.environ["S3_BUCKET"]


def upload(client, bucket: str, local: Path, key: str) -> None:
    size_mb = local.stat().st_size / 1e6
    print(f"  -> s3://{bucket}/{key}  ({size_mb:,.1f} MB)")
    client.upload_file(str(local), bucket, key)  # handles multipart for big files


def upload_full(client, bucket, prefix, table) -> None:
    local = DATA_DIR / table["source_file"]
    if not local.is_file():
        raise FileNotFoundError(f"Source file not found: {local}")
    upload(client, bucket, local, f"{prefix}/{table['name']}/{table['source_file']}")


def upload_one_wave(client, bucket, prefix, table, wave: int) -> None:
    fname = f"{table['name']}_{wave:03d}.csv"
    local = DATA_DIR / table["wave_dir"] / fname
    if not local.is_file():
        # some waves have no file, e.g. no train orders with a low order_number
        print(f"  (wave {wave:03d}: no file for {table['name']}, skip)")
        return
    upload(client, bucket, local, f"{prefix}/{table['name']}/{fname}")


def upload_all_waves(client, bucket, prefix, table) -> None:
    wdir = DATA_DIR / table["wave_dir"]
    files = sorted(wdir.glob(f"{table['name']}_*.csv"))
    if not files:
        print(f"  (no wave files found in {wdir})")
        return
    for local in files:
        upload(client, bucket, local, f"{prefix}/{table['name']}/{local.name}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Upload Instacart CSVs to S3.")
    ap.add_argument("--full", action="store_true", help="only reference (full) tables")
    ap.add_argument("--wave", type=int, help="only this wave number of the wave tables")
    args = ap.parse_args()

    config = load_config()
    prefix = config["s3_prefix"]
    client, bucket = make_s3_client()

    full_tables = [t for t in config["tables"] if t["load_mode"] == "full"]
    wave_tables = [t for t in config["tables"] if t["load_mode"] == "wave"]

    do_full = args.full or args.wave is None
    do_waves = not args.full

    if do_full:
        print("Reference (full) tables:")
        for t in full_tables:
            upload_full(client, bucket, prefix, t)

    if do_waves:
        for t in wave_tables:
            print(f"{t['name']}:")
            if args.wave is not None:
                upload_one_wave(client, bucket, prefix, t, args.wave)
            else:
                upload_all_waves(client, bucket, prefix, t)

    print("Done.")


if __name__ == "__main__":
    main()
