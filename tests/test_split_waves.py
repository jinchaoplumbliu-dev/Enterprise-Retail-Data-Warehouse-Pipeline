import csv

import split_waves


def write_csv(path, header, rows):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def read_csv(path):
    with open(path, newline="") as fh:
        return list(csv.reader(fh))


ORDERS_HEADER = [
    "order_id", "user_id", "eval_set", "order_number",
    "order_dow", "order_hour_of_day", "days_since_prior_order",
]
LINES_HEADER = ["order_id", "product_id", "add_to_cart_order", "reordered"]


def make_data_dir(tmp_path):
    # user 7 has orders 101 (1st) and 102 (2nd); user 8 has order 201 (1st)
    write_csv(tmp_path / "orders.csv", ORDERS_HEADER, [
        ["101", "7", "prior", "1", "2", "9", ""],
        ["102", "7", "train", "2", "3", "14", "8.0"],
        ["201", "8", "prior", "1", "6", "20", ""],
    ])
    write_csv(tmp_path / "order_products__prior.csv", LINES_HEADER, [
        ["101", "555", "1", "0"],
        ["101", "556", "2", "0"],
        ["201", "555", "1", "0"],
    ])
    write_csv(tmp_path / "order_products__train.csv", LINES_HEADER, [
        ["102", "555", "1", "1"],
    ])
    return tmp_path


def test_split_orders_builds_map_and_wave_files(tmp_path):
    data = make_data_dir(tmp_path)

    order_to_wave = split_waves.split_orders(data)

    assert order_to_wave == {101: 1, 102: 2, 201: 1}
    wave1 = read_csv(data / "waves" / "orders" / "orders_001.csv")
    wave2 = read_csv(data / "waves" / "orders" / "orders_002.csv")
    assert wave1 == [
        ORDERS_HEADER,
        ["101", "7", "prior", "1", "2", "9", ""],
        ["201", "8", "prior", "1", "6", "20", ""],
    ]
    assert wave2 == [
        ORDERS_HEADER,
        ["102", "7", "train", "2", "3", "14", "8.0"],
    ]


def test_split_order_products_appends_wave_column(tmp_path):
    data = make_data_dir(tmp_path)
    order_to_wave = split_waves.split_orders(data)

    split_waves.split_order_products(
        data, "order_products__prior.csv", "order_products_prior", order_to_wave
    )

    wave1 = read_csv(data / "waves" / "order_products_prior" / "order_products_prior_001.csv")
    assert wave1 == [
        LINES_HEADER + ["order_number"],
        ["101", "555", "1", "0", "1"],
        ["101", "556", "2", "0", "1"],
        ["201", "555", "1", "0", "1"],
    ]
    # all prior line items belong to first orders -> no wave-2 file at all
    assert not (data / "waves" / "order_products_prior" / "order_products_prior_002.csv").exists()


def test_split_is_deterministic_on_rerun(tmp_path):
    data = make_data_dir(tmp_path)

    order_to_wave = split_waves.split_orders(data)
    split_waves.split_order_products(
        data, "order_products__train.csv", "order_products_train", order_to_wave
    )
    first = read_csv(data / "waves" / "order_products_train" / "order_products_train_002.csv")

    order_to_wave = split_waves.split_orders(data)
    split_waves.split_order_products(
        data, "order_products__train.csv", "order_products_train", order_to_wave
    )
    second = read_csv(data / "waves" / "order_products_train" / "order_products_train_002.csv")

    assert first == second == [
        LINES_HEADER + ["order_number"],
        ["102", "555", "1", "1", "2"],
    ]
