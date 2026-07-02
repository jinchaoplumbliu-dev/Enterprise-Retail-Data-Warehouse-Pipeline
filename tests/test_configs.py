"""Sanity checks on the EL config contracts the loaders are driven by."""
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def load(name):
    with open(ROOT / "config" / name) as fh:
        return yaml.safe_load(fh)


def test_tables_config_shape():
    cfg = load("tables.yml")
    assert cfg["s3_prefix"]

    for t in cfg["tables"]:
        assert t["load_mode"] in ("full", "wave"), t["name"]
        assert t["columns"], t["name"]
        for c in t["columns"]:
            assert c["name"] and c["type"], t["name"]
        if t["load_mode"] == "full":
            assert t["source_file"].endswith(".csv"), t["name"]
        else:
            assert t["wave_dir"], t["name"]
            # the loader deletes by this column, so it must exist on the table
            assert t["wave_column"] in [c["name"] for c in t["columns"]], t["name"]


def test_api_config_shape():
    cfg = load("api_sources.yml")["sources"]["off_products"]

    assert cfg["base_url"].startswith("https://")
    assert 0 < cfg["page_size"] <= 100      # OFF caps page_size at 100
    assert cfg["max_pages"] >= 1
    assert cfg["rate_limit_seconds"] > 0
    # the watermark field must be part of the extract, and drive the sort
    assert cfg["watermark_field"] in cfg["fields"]
    assert cfg["sort_by"] == cfg["watermark_field"]
    # must land under the stage root (raw/) so the existing integration covers it
    assert cfg["s3_prefix"].startswith("raw/")
