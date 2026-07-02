import pytest

import upload_to_s3


class FakeClient:
    def __init__(self):
        self.uploads = []  # (local_path, bucket, key)

    def upload_file(self, filename, bucket, key):
        self.uploads.append((filename, bucket, key))


ORDERS = {"name": "orders", "load_mode": "wave", "wave_dir": "waves/orders"}
AISLES = {"name": "aisles", "load_mode": "full", "source_file": "aisles.csv"}


@pytest.fixture
def data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(upload_to_s3, "DATA_DIR", tmp_path)
    return tmp_path


def test_upload_full_puts_file_under_table_prefix(data_dir):
    (data_dir / "aisles.csv").write_text("aisle_id,aisle\n1,bakery\n")
    client = FakeClient()

    upload_to_s3.upload_full(client, "bkt", "raw", AISLES)

    assert client.uploads == [(str(data_dir / "aisles.csv"), "bkt", "raw/aisles/aisles.csv")]


def test_upload_full_missing_source_raises(data_dir):
    with pytest.raises(FileNotFoundError):
        upload_to_s3.upload_full(FakeClient(), "bkt", "raw", AISLES)


def test_upload_one_wave_uses_zero_padded_key(data_dir):
    wave_dir = data_dir / "waves" / "orders"
    wave_dir.mkdir(parents=True)
    (wave_dir / "orders_007.csv").write_text("order_id\n1\n")
    client = FakeClient()

    upload_to_s3.upload_one_wave(client, "bkt", "raw", ORDERS, 7)

    assert client.uploads == [
        (str(wave_dir / "orders_007.csv"), "bkt", "raw/orders/orders_007.csv")
    ]


def test_upload_one_wave_skips_missing_file(data_dir):
    client = FakeClient()

    upload_to_s3.upload_one_wave(client, "bkt", "raw", ORDERS, 42)

    assert client.uploads == []


def test_upload_all_waves_uploads_in_order(data_dir):
    wave_dir = data_dir / "waves" / "orders"
    wave_dir.mkdir(parents=True)
    for n in (2, 1, 10):
        (wave_dir / f"orders_{n:03d}.csv").write_text("order_id\n1\n")
    client = FakeClient()

    upload_to_s3.upload_all_waves(client, "bkt", "raw", ORDERS)

    keys = [key for _, _, key in client.uploads]
    assert keys == ["raw/orders/orders_001.csv", "raw/orders/orders_002.csv", "raw/orders/orders_010.csv"]
