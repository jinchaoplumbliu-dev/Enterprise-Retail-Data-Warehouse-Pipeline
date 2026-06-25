# Retail Data Warehouse on AWS + Snowflake

An end-to-end ELT pipeline on the Instacart Online Grocery dataset, rebuilt on a
cloud stack:

```
CSV ──(boto3)──> S3 ──(External Stage + COPY INTO)──> Snowflake.raw ──(dbt)──> staging → marts
```

- **AWS S3** — landing zone / data lake for the raw CSVs.
- **Snowflake** — the warehouse (separated compute + storage).
- **dbt** — transforms raw into a Kimball dimensional model.
- **Airflow → MWAA** — orchestration (added in the last steps).

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
| 7 | Deploy to AWS MWAA | ☐ |

## Project structure

```
.
├── config/tables.yml          # EL contract: tables, columns, load mode
├── data/                       # source CSVs + generated waves (gitignored)
├── src/
│   ├── prep/split_waves.py     # one-time wave split by order_number
│   ├── upload_to_s3.py         # Step 2 — boto3 → S3
│   └── load_to_snowflake.py    # Step 4 — COPY INTO from stage
├── snowflake/
│   ├── 01_setup.sql            # warehouse / database / role / grants
│   ├── 02_storage_integration.sql  # S3 <-> Snowflake trust (Step 4)
│   └── 03_stage_and_raw.sql    # external stage + raw tables (Step 4)
├── dbt/instacart/              # dbt project (staging / intermediate / marts)
├── requirements.txt
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
