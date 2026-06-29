# Retail Data Warehouse on AWS + Snowflake

An end-to-end ELT pipeline on the Instacart Online Grocery dataset, rebuilt on a
cloud stack:

```
batch:  CSV ──(boto3)──────────────> S3 ─┐
api:    Open Food Facts ──(incremental)─>┘──(External Stage + COPY INTO)──> Snowflake.raw ──(dbt)──> staging → marts
```

Two ingestion paths into the same S3 landing zone:
- **Batch** — Instacart CSVs uploaded with `boto3`, loaded in `order_number` waves.
- **API** — Open Food Facts pulled incrementally (pagination, rate-limit/retry,
  `last_modified_t` watermark), landed as JSON, read schema-on-read via a
  Snowflake `VARIANT` column.

- **AWS S3** — landing zone / data lake.
- **Snowflake** — the warehouse (separated compute + storage; `VARIANT` for JSON).
- **dbt** — transforms raw into a Kimball dimensional model + a nutrition dimension.
- **Airflow (Astro)** — orchestrates both pipelines.

The dimensional model (a fact constellation: `fact_order_items`, `fact_orders`
over conformed `dim_product` / `dim_user` / `dim_time`) is carried over from the
original Postgres build — dbt models are warehouse-agnostic.

## Build roadmap

Built inside-out: each step is independently verifiable before the next.

| Step | Goal | Status |
|---|---|---|
| 0 | Project skeleton + tooling | ✅ done |
| 1 | AWS foundation: S3 bucket + SSO (Identity Center) | ✅ done |
| 2 | Python → S3 (`boto3` uploader) | ✅ done |
| 3 | Snowflake foundation: warehouse / db / role | ✅ done |
| 4 | S3 → Snowflake: Storage Integration + Stage + `COPY INTO` | ✅ done |
| 5 | dbt on Snowflake: staging → marts | ✅ done |
| 6 | Local Airflow orchestration (Astro) | ✅ done |
| 7 | API ingestion: Open Food Facts → VARIANT → `dim_food_product` | ✅ done |
| — | (optional) Deploy to AWS MWAA | ☐ |

## Project structure

```
.
├── config/
│   ├── tables.yml              # batch EL contract: tables, columns, load mode
│   └── api_sources.yml         # API ingestion contract (Open Food Facts)
├── data/                       # source CSVs + generated waves (gitignored)
├── src/
│   ├── prep/split_waves.py     # one-time wave split by order_number
│   ├── upload_to_s3.py         # batch: boto3 → S3
│   ├── load_to_snowflake.py    # batch: COPY INTO from stage
│   ├── extract_api.py          # api: incremental pull → S3 (watermark)
│   └── load_api_to_snowflake.py # api: COPY JSON into VARIANT
├── snowflake/
│   ├── 01_setup.sql            # warehouse / database / role / grants
│   ├── 02_storage_integration.sql  # S3 <-> Snowflake trust
│   └── 03_stage.sql            # external stage + file format
├── dbt/instacart/              # dbt project (staging / intermediate / marts)
├── dags/                       # Airflow: instacart_pipeline + off_api_pipeline
├── Dockerfile                  # Astro image (+ isolated dbt venv)
├── docker-compose.override.yml # mounts src/config/dbt/data + ~/.aws into Astro
├── requirements.txt            # Airflow image deps (dbt is separate)
└── .env.example                # credential template (copy to .env)
```

## Setup

```bash
python -m venv .venv && source .venv/Scripts/activate   # Windows Git Bash
pip install -r requirements.txt
cp .env.example .env            # then fill in AWS + Snowflake credentials
```

Get the data: download the 6 Instacart CSVs into `data/` (see
[Kaggle](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset)).
