"""
Splits the large transactional CSVs into per-wave files keyed by order_number.

order_products rows only carry order_id, so we first build an
order_id -> order_number map from orders.csv, then append order_number to each
line-item row as it is routed to its wave file.

Run once before the first pipeline run:
    python src/prep/split_waves.py                  # uses <repo>/data
    python src/prep/split_waves.py --data-dir /path/to/data
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

# repo root locally, /usr/local/airflow in the container (src/ is mounted there)
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = ROOT / "data"


class WaveWriter:
    """Lazily opens one CSV writer per wave, writing a header on first use."""

    def __init__(self, waves_dir: Path, table: str, header: list[str]):
        self.folder = waves_dir / table
        self.table = table
        self.header = header
        self._writers: dict[int, object] = {}
        self._handles: dict[int, object] = {}

    def write(self, wave: int, row: list) -> None:
        if wave not in self._writers:
            self.folder.mkdir(parents=True, exist_ok=True)
            fh = open(self.folder / f"{self.table}_{wave:03d}.csv", "w", newline="")
            self._handles[wave] = fh
            w = csv.writer(fh)
            w.writerow(self.header)
            self._writers[wave] = w
        self._writers[wave].writerow(row)

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()


def split_orders(data_dir: Path) -> dict[int, int]:
    """Split orders.csv by order_number; return an order_id -> order_number map."""
    order_to_wave: dict[int, int] = {}
    with open(data_dir / "orders.csv", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        oid_idx = header.index("order_id")
        onum_idx = header.index("order_number")

        writer = WaveWriter(data_dir / "waves", "orders", header)
        for row in reader:
            wave = int(row[onum_idx])
            order_to_wave[int(row[oid_idx])] = wave
            writer.write(wave, row)
        writer.close()

    print(f"orders: {len(order_to_wave):,} orders mapped and split into waves")
    return order_to_wave


def split_order_products(
    data_dir: Path, filename: str, table: str, order_to_wave: dict[int, int]
) -> None:
    """Split an order_products file by its order's order_number (appended as a column)."""
    with open(data_dir / filename, newline="") as fh:
        reader = csv.reader(fh)
        src_header = next(reader)
        oid_idx = src_header.index("order_id")

        writer = WaveWriter(data_dir / "waves", table, src_header + ["order_number"])
        n = 0
        for row in reader:
            wave = order_to_wave[int(row[oid_idx])]
            writer.write(wave, row + [wave])
            n += 1
        writer.close()

    print(f"{table}: split {n:,} line items into waves")


def main() -> None:
    ap = argparse.ArgumentParser(description="Split the transactional CSVs into waves.")
    ap.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = ap.parse_args()

    order_to_wave = split_orders(args.data_dir)
    split_order_products(args.data_dir, "order_products__prior.csv", "order_products_prior", order_to_wave)
    split_order_products(args.data_dir, "order_products__train.csv", "order_products_train", order_to_wave)
    print("Wave split complete.")


if __name__ == "__main__":
    main()
