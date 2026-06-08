FROM astrocrpublic.azurecr.io/runtime:3.2-5
# The base image bundles dbt Fusion (dbt-core 2.x) in dbt_venv, which only
# supports snowflake/bigquery/databricks/redshift. dbt-postgres declares an
# unpinned dbt-core dependency, so pip otherwise resolves it to the Fusion
# 2.0 core. Wipe the bundled venv and rebuild on the classic dbt-core 1.x
# engine so the postgres adapter is available.
RUN rm -rf dbt_venv && \
    python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir "dbt-core<2.0" "dbt-postgres==1.9.0"
