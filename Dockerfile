FROM astrocrpublic.azurecr.io/runtime:3.2-5

# Install dbt into an isolated venv so its pins don't collide with Airflow's.
# The DAG's dbt task calls /usr/local/airflow/dbt_venv/bin/dbt directly.
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN rm -rf dbt_venv && \
    python -m venv dbt_venv && \
    dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
