-- Executes automatically during container initialization (triggered by docker-entrypoint-initdb.d)
CREATE SCHEMA IF NOT EXISTS raw;
COMMENT ON SCHEMA raw IS 'Landing zone for raw CSV extracts before dbt transformation.';