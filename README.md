# Enterprise Retail Data Warehouse Pipeline

A containerised, end-to-end data engineering pipeline on the Instacart Online
Grocery dataset (~3.4M orders, ~33M order line items). Raw CSVs are ingested in
`order_number` **waves**, transformed with **dbt** through a layered warehouse,
and modelled into a **Kimball dimensional model** (a fact constellation), all
orchestrated by **Apache Airflow** and tested at build time.

`Data Engineer` · `Analytics Engineer` — dimensional modelling, incremental ingestion, orchestration, data quality.

## What this project demonstrates

- **Dimensional modelling (Kimball):** a fact constellation — two facts at two
  grains sharing conformed dimensions, with degenerate dimensions and
  deterministic surrogate keys.
- **Scale:** an incrementally-materialised line-item fact (~33M rows) loaded via
  `COPY` rather than row-by-row.
- **Simulated incremental ingestion:** a static dataset is replayed as
  `order_number` waves to give orchestration and incremental logic real meaning.
- **Idempotency end to end:** re-running any wave never duplicates data, at both
  the raw and the transformation layer.
- **Data quality as a gate:** dbt tests run inside the pipeline; a failing test
  fails the DAG.

## Architecture

```mermaid
flowchart LR
    A[6 source CSVs] --> B[Wave split<br/>by order_number]
    B --> C[(raw schema)]
    C --> D[dbt staging<br/>views]
    D --> E[dbt intermediate]
    E --> F[dbt marts<br/>Kimball constellation]
    F --> G[Data quality tests]
```

Airflow orchestrates each wave as `load_raw → dbt build` (build runs models **and**
tests). Everything runs in Docker via the Astro CLI; the analytics warehouse is a
PostgreSQL instance separate from Airflow's metadata database.

### Layers

| Layer | Schema | Materialisation | Purpose |
|---|---|---|---|
| Landing | `raw` | tables | faithful copy of source CSVs (+ `_loaded_at` lineage) |
| Staging | `staging` | views | clean / rename / type; `prior ∪ train`; drop `test` |
| Intermediate | `intermediate` | table / view | order- and user-grain derived metrics |
| Marts | `marts` | tables (+ incremental) | the dimensional model |

## Data model (fact constellation)

| Model | Grain | Materialisation |
|---|---|---|
| `fact_order_items` | one product in one order (~33M) | **incremental** |
| `fact_orders` | one order | table |
| `dim_product` | product (aisle + department flattened in) | table |
| `dim_user` | user + behavioural attributes | table |
| `dim_time` | day-of-week × hour-of-day (168 rows) | table |

`order_id`, `order_number`, and `add_to_cart_order` are kept as degenerate
dimensions on the facts. Surrogate keys (`product_key`, `user_key`, `time_key`)
are deterministic hashes of the natural keys.

## Key design decisions

- **Hash surrogate keys, not auto-increment.** `dbt_utils.generate_surrogate_key`
  produces a deterministic key from the natural key, so it is stable across
  rebuilds and identical regardless of insert order — which suits a pipeline
  whose models are rebuilt repeatedly. Because the key is a pure function of the
  natural key, the facts **recompute** it instead of joining to dimensions,
  avoiding a 33M-row lookup join. (Auto-increment keys are smaller and join
  faster, but are stateful and order-dependent.)
- **`order_number` waves + partitioned landing (one-time split).** The large
  files are pre-split once into per-wave files, so each wave load is a small,
  fast `COPY` rather than a re-scan of the full file. Line items only carry
  `order_id`, so the split builds an `order_id → order_number` map from `orders`
  and appends `order_number` to each line-item row.
- **Two-layer incrementality, both idempotent.** Raw loads `DELETE` the wave then
  `COPY` it; the line-item fact is a dbt `incremental` model
  (`delete+insert`, `unique_key=[order_id, product_id]`) that advances on an
  `order_number` watermark (`where order_number > max(order_number)`).
- **No Cosmos.** dbt's model graph is already a DAG; Airflow runs a single
  `dbt build` task rather than exploding each model into an Airflow task — simpler
  and lower-maintenance at this scale.
- **`prior ∪ train`, `test` excluded.** Both `prior` and `train` are real
  completed orders and are unioned into the facts (with a `source` lineage flag);
  `test` orders have no basket (withheld for the original ML competition) so they
  are dropped. The ML split itself is irrelevant to a transactional warehouse.
- **Cyclical time, no synthetic calendar.** The dataset has no real dates, only
  `order_dow` + `order_hour_of_day` and relative `days_since_prior_order`.
  `dim_time` captures the cyclical time honestly; the day-of-week → day-name
  mapping is flagged in-model as an unverified community assumption.

## Tech stack

- **Orchestration:** Apache Airflow 3.2 (Astro Runtime `3.2-5`)
- **Runtime:** Docker, via the Astro CLI
- **Warehouse:** PostgreSQL 16
- **Extract & Load:** config-driven Python + `PostgresHook.copy_expert` (`COPY`)
- **Transformation / tests:** dbt-core 1.9 + dbt-postgres 1.9.0 (isolated venv), dbt_utils 1.3.3
- **Modelling:** Kimball star / constellation schema

## Project structure

```
.
├── Dockerfile                          # Astro Runtime 3.2-5; installs dbt into a venv
├── docker-compose.override.yml         # adds the `warehouse` Postgres service
├── airflow_settings.yaml               # warehouse_postgres connection
├── sql/00_init_schemas.sql             # creates the `raw` schema on first boot
├── dags/
│   ├── ping_warehouse.py               # connectivity smoke test
│   ├── setup_warehouse.py              # create raw tables + load reference dims (run once)
│   ├── load_raw_wave.py                # EL only: load one wave into raw
│   └── instacart_pipeline.py           # end-to-end: load_raw → dbt build (per wave)
└── include/
    ├── data/                           # source CSVs + waves/ (gitignored)
    ├── prep/split_waves.py             # one-time wave split
    ├── extract/
    │   ├── extract_tables.yml          # EL contract (single source of truth)
    │   └── loader.py                   # config-driven full / wave loader
    └── dbt/instacart/
        ├── dbt_project.yml
        ├── packages.yml                # dbt_utils
        ├── profiles.yml
        ├── macros/generate_schema_name.sql
        └── models/
            ├── staging/                # stg_* + _sources.yml
            ├── intermediate/           # int_order_metrics, int_user_metrics
            └── marts/                  # dims/, facts/, _marts.yml (tests)
```

## Running it

Prerequisites: Docker Desktop and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli/).

1. **Start the environment** (builds the image, including the dbt venv):

   ```bash
   astro dev start
   ```

2. **Get the data** — download the 6 CSVs into `include/data/` (see the dataset on
   [Kaggle](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset)),
   then split them into waves:

   ```bash
   docker exec -it <...-scheduler-1> python /usr/local/airflow/include/prep/split_waves.py
   ```

3. **Set up the warehouse** (once) — trigger the `setup_warehouse` DAG to create
   the raw tables and load the reference dimensions.

4. **Run the pipeline per wave** — trigger `instacart_pipeline` with a run config
   such as `{"wave": 1}`, `{"wave": 2}`, … Each run loads that wave into `raw` and
   rebuilds + tests the warehouse.

### Verify

```sql
-- facts grow as waves are loaded
SELECT order_number, count(*) FROM marts.fact_order_items GROUP BY 1 ORDER BY 1;

-- referential integrity (expect 0)
SELECT count(*) FROM marts.fact_order_items f
LEFT JOIN marts.dim_product d ON f.product_key = d.product_key
WHERE d.product_key IS NULL;
```

> Local-dev credentials only: `instacart` / `instacart` / `instacart_warehouse`.

## Data quality

`dbt build` runs the test suite on every pipeline run:

- `unique` / `not_null` on every surrogate key
- `relationships` from each fact key to its dimension (referential integrity)
- `accepted_values` on `reordered` (`0` / `1`)
- `dbt_utils.unique_combination_of_columns` on `(order_id, product_id)` — the
  guard against incremental-load duplicates

## Possible extensions

- **Scheduling:** a master DAG that advances through waves automatically, or a
  schedule that replays "one wave per day".
- **Performance:** index / declaratively partition `fact_order_items` on
  `order_number`.
- **SCD Type 2:** model a slowly-changing attribute (e.g. product re-categorisation)
  to demonstrate history tracking.

## Credits

Built on the [Instacart Online Grocery Basket Analysis dataset](https://www.kaggle.com/datasets/yasserh/instacart-online-grocery-basket-analysis-dataset).
