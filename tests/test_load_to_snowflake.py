import load_to_snowflake as loader


class FakeCursor:
    """Records executed SQL; `list @...` returns whatever list_result is set to."""

    def __init__(self, list_result=None):
        self.executed = []          # list of (sql, params)
        self.list_result = list_result or []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.list_result

    def sql(self):
        return [s for s, _ in self.executed]


ORDERS = {
    "name": "orders",
    "load_mode": "wave",
    "wave_column": "order_number",
    "columns": [
        {"name": "order_id", "type": "bigint"},
        {"name": "user_id", "type": "bigint"},
        {"name": "order_number", "type": "integer"},
    ],
}

AISLES = {
    "name": "aisles",
    "load_mode": "full",
    "source_file": "aisles.csv",
    "columns": [
        {"name": "aisle_id", "type": "integer"},
        {"name": "aisle", "type": "text"},
    ],
}


def test_create_raw_tables_adds_loaded_at_default():
    cur = FakeCursor()
    loader.create_raw_tables(cur, {"tables": [AISLES]})

    sql = cur.sql()[0]
    assert "create table if not exists raw.aisles" in sql
    assert "aisle_id integer" in sql
    assert "aisle text" in sql
    assert "_loaded_at timestamp_tz default current_timestamp()" in sql


def test_copy_into_projects_stage_columns_explicitly():
    cur = FakeCursor()
    loader._copy_into(cur, ORDERS, "orders/orders_001.csv")

    sql = cur.sql()[0]
    # explicit column list + $N projection so _loaded_at keeps its default
    assert "copy into raw.orders (order_id, user_id, order_number)" in sql
    assert "select $1, $2, $3 from @s3_stage/orders/orders_001.csv" in sql
    assert "force = true" in sql
    assert "on_error = abort_statement" in sql


def test_load_full_truncates_then_copies():
    cur = FakeCursor()
    loader.load_full(cur, AISLES)

    sql = cur.sql()
    assert sql[0] == "truncate table raw.aisles"
    assert "copy into raw.aisles" in sql[1]
    assert "@s3_stage/aisles/aisles.csv" in sql[1]


def test_load_wave_deletes_wave_then_copies_wave_file():
    cur = FakeCursor(list_result=[("orders/orders_003.csv",)])
    loader.load_wave(cur, ORDERS, 3)

    sql = cur.sql()
    assert sql[0] == "delete from raw.orders where order_number = %s"
    assert cur.executed[0][1] == (3,)
    assert sql[1] == "list @s3_stage/orders/orders_003.csv"
    assert "@s3_stage/orders/orders_003.csv" in sql[2]


def test_load_wave_skips_copy_when_file_missing():
    cur = FakeCursor(list_result=[])
    loader.load_wave(cur, ORDERS, 3)

    # delete + list, but no COPY for a wave with no file
    assert len(cur.sql()) == 2
    assert not any("copy into" in s for s in cur.sql())
