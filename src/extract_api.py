"""
Incrementally extract products from Open Food Facts to S3.

  - paginated reads against the live API
  - polite rate limiting with retry/backoff on 5xx
  - incremental by a high-water mark: products are returned newest-modified
    first (sort_by=last_modified_t DESC), so we page until we cross the watermark
    and stop. The watermark is max(last_modified_t) already in Snowflake, so no
    separate state store is needed.
  - raw JSON landed to S3, partitioned by extract date (parsed schema-on-read).

Usage:
    python src/extract_api.py                 # incremental (watermark from Snowflake)
    python src/extract_api.py --full          # ignore watermark (backfill up to max_pages)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import time
import urllib.parse
import urllib.request
from pathlib import Path

import boto3
import snowflake.connector
import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config" / "api_sources.yml"
USER_AGENT = "instacart-dwh-learning/1.0 (jinchaoplumbliu@gmail.com)"


def load_config() -> dict:
    with open(CONFIG_PATH) as fh:
        return yaml.safe_load(fh)["sources"]["off_products"]


def get_watermark(raw_table: str) -> int:
    """max(last_modified_t) already loaded, or 0 if the table is empty/missing."""
    load_dotenv(ROOT / ".env")
    try:
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"], user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"], role=os.environ.get("SNOWFLAKE_ROLE", "INSTACART_ROLE"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INSTACART_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "INSTACART"), schema="raw",
        )
        try:
            cur = conn.cursor()
            cur.execute(f"select coalesce(max(last_modified_t), 0) from raw.{raw_table}")
            return int(cur.fetchone()[0])
        finally:
            conn.close()
    except Exception as exc:  # table not created yet -> first run
        print(f"  (no watermark yet: {exc.__class__.__name__}); starting from 0")
        return 0


def fetch_page(cfg: dict, page: int, max_retries: int = 8) -> list[dict]:
    params = {
        "fields": ",".join(cfg["fields"]),
        "page_size": cfg["page_size"],
        "page": page,
        "sort_by": cfg["sort_by"],
    }
    url = cfg["base_url"] + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    # The free OFF service throttles with intermittent 5xx; retry with backoff.
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp).get("products", [])
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            status = getattr(exc, "code", None)
            if attempt == max_retries or (status is not None and status < 500):
                raise
            backoff = min(2 ** attempt, 30)
            print(f"    page {page}: {exc} - retry {attempt}/{max_retries - 1} in {backoff}s")
            time.sleep(backoff)
    return []


def extract(cfg: dict, watermark: int) -> list[dict]:
    """Page newest-first, keeping rows newer than the watermark; stop once crossed."""
    new_rows: list[dict] = []
    for page in range(1, cfg["max_pages"] + 1):
        try:
            products = fetch_page(cfg, page)
        except (urllib.error.HTTPError, urllib.error.URLError) as exc:
            # Persistent failure: keep what we have, resume on the next run.
            print(f"  page {page}: giving up ({exc}); stopping with {len(new_rows)} rows so far")
            break
        if not products:
            break
        fresh = [p for p in products if int(p.get("last_modified_t") or 0) > watermark]
        new_rows.extend(fresh)
        print(f"  page {page}: {len(products)} fetched, {len(fresh)} newer than watermark")
        if len(fresh) < len(products):
            break  # crossed the watermark - everything older is already loaded
        time.sleep(cfg["rate_limit_seconds"])
    return new_rows


def s3_client():
    load_dotenv(ROOT / ".env")
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"),
        region_name=os.environ.get("AWS_DEFAULT_REGION"),
    )
    return session.client("s3"), os.environ["S3_BUCKET"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Incrementally extract OFF products to S3.")
    ap.add_argument("--full", action="store_true", help="ignore watermark (backfill)")
    args = ap.parse_args()

    cfg = load_config()
    watermark = 0 if args.full else get_watermark(cfg["raw_table"])
    print(f"Watermark (last_modified_t > {watermark}); pulling up to {cfg['max_pages']} pages.")

    rows = extract(cfg, watermark)
    if not rows:
        print("No new products since last run. Done.")
        return

    client, bucket = s3_client()
    day = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%H%M%S")
    key = f"{cfg['s3_prefix']}/dt={day}/products_{stamp}.json"
    body = json.dumps(rows).encode("utf-8")           # JSON array -> STRIP_OUTER_ARRAY in Snowflake
    client.put_object(Bucket=bucket, Key=key, Body=body)
    print(f"Wrote {len(rows):,} products -> s3://{bucket}/{key} ({len(body) / 1e6:.2f} MB)")


if __name__ == "__main__":
    main()
