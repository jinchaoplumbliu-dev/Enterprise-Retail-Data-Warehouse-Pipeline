"""
Splits the large transactional CSVs into per-wave files keyed by order_number.
order_products rows only carry order_id, so we first build an
order_id -> order_number map from orders.csv, then append order_number to each
line-item row as it is routed to its wave file.

Run once, inside the scheduler container:
    python /usr/local/airflow/include/prep/split_waves.py
"""

from __future__ import annotations

import csv
import os

DATA_DIR = "/usr/local/airflow/include/data"
WAVES_DIR = os.path.join(DATA_DIR, "waves")


def _wave_path(table: str, wave: int) -> str:
    folder = os.path.join(WAVES_DIR, table)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, f"{table}_{wave:03d}.csv")


class WaveWriter:
    """Lazily opens one CSV writer per wave, writing a header on first use."""

    def __init__(self, table: str, header: list[str]):
        self.table = table
        self.header = header
        self._writers: dict[int, "csv._writer"] = {}
        self._handles: dict[int, object] = {}

    def write(self, wave: int, row: list) -> None:
        if wave not in self._writers:
            fh = open(_wave_path(self.table, wave), "w", newline="")
            self._handles[wave] = fh
            w = csv.writer(fh)
            w.writerow(self.header)
            self._writers[wave] = w
        self._writers[wave].writerow(row)

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()


def split_orders() -> dict[int, int]:
    """Split orders.csv by order_number; return an order_id -> order_number map."""
    order_to_wave: dict[int, int] = {}
    with open(os.path.join(DATA_DIR, "orders.csv"), newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        oid_idx = header.index("order_id")
        onum_idx = header.index("order_number")

        writer = WaveWriter("orders", header)
        for row in reader:
            wave = int(row[onum_idx])
            order_to_wave[int(row[oid_idx])] = wave
            writer.write(wave, row)
        writer.close()

    print(f"orders: {len(order_to_wave):,} orders mapped and split into waves")
    return order_to_wave


def split_order_products(filename: str, table: str, order_to_wave: dict[int, int]) -> None:
    """Split an order_products file by its order's order_number (appended as a column)."""
    with open(os.path.join(DATA_DIR, filename), newline="") as fh:
        reader = csv.reader(fh)
        src_header = next(reader)
        oid_idx = src_header.index("order_id")

        writer = WaveWriter(table, src_header + ["order_number"])
        n = 0
        for row in reader:
            wave = order_to_wave[int(row[oid_idx])]
            writer.write(wave, row + [wave])
            n += 1
        writer.close()

    print(f"{table}: split {n:,} line items into waves")


def main() -> None:
    order_to_wave = split_orders()
    split_order_products("order_products__prior.csv", "order_products_prior", order_to_wave)
    split_order_products("order_products__train.csv", "order_products_train", order_to_wave)
    print("Wave split complete.")


if __name__ == "__main__":
    main()