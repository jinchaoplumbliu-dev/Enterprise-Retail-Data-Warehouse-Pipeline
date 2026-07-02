import load_api_to_snowflake as loader


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append(sql)

    def fetchall(self):
        return []


CFG = {
    "raw_table": "off_products_raw",
    "s3_prefix": "raw/off_products_api",
}


def test_create_table_has_variant_and_watermark_columns():
    cur = FakeCursor()
    loader.create_table(cur, CFG["raw_table"])

    sql = cur.executed[0]
    assert "create table if not exists raw.off_products_raw" in sql
    assert "payload          variant" in sql
    assert "last_modified_t  number" in sql


def test_copy_in_strips_stage_root_from_prefix():
    cur = FakeCursor()
    loader.copy_in(cur, CFG)

    sql = cur.executed[0]
    # the stage already points at raw/, so the path must not repeat it
    assert "@s3_stage/off_products_api/" in sql
    assert "@s3_stage/raw/" not in sql


def test_copy_in_parses_json_array_and_extracts_watermark():
    cur = FakeCursor()
    loader.copy_in(cur, CFG)

    sql = cur.executed[0]
    assert "copy into raw.off_products_raw (payload, last_modified_t)" in sql
    assert "$1:last_modified_t::number" in sql
    assert "strip_outer_array = true" in sql
    # relies on load history for idempotency, so no FORCE here
    assert "force" not in sql.lower()
