import io
import json
import urllib.error

import pytest

import extract_api


CFG = {
    "base_url": "https://example.test/api/v2/search",
    "page_size": 3,
    "max_pages": 4,
    "rate_limit_seconds": 0,
    "sort_by": "last_modified_t",
    "fields": ["code", "last_modified_t"],
}


def product(code, t):
    return {"code": code, "last_modified_t": t}


class TestExtract:
    """Watermark logic: page newest-first, stop once we cross the watermark."""

    def test_stops_at_watermark_and_keeps_only_fresh(self, monkeypatch):
        pages = {
            1: [product("a", 500), product("b", 400), product("c", 300)],
            2: [product("d", 250), product("e", 100), product("f", 50)],
        }
        fetched = []
        monkeypatch.setattr(
            extract_api, "fetch_page",
            lambda cfg, page: fetched.append(page) or pages[page],
        )

        rows = extract_api.extract(CFG, watermark=200)

        # page 2 contains rows at/below the watermark, so paging stops there
        assert fetched == [1, 2]
        assert [r["code"] for r in rows] == ["a", "b", "c", "d"]

    def test_full_pages_of_fresh_rows_keep_paging_until_empty(self, monkeypatch):
        pages = {
            1: [product("a", 300), product("b", 200), product("c", 100)],
            2: [],
        }
        monkeypatch.setattr(extract_api, "fetch_page", lambda cfg, page: pages[page])

        rows = extract_api.extract(CFG, watermark=0)

        assert [r["code"] for r in rows] == ["a", "b", "c"]

    def test_respects_max_pages(self, monkeypatch):
        monkeypatch.setattr(
            extract_api, "fetch_page",
            lambda cfg, page: [product(f"p{page}-{i}", 10_000 - page) for i in range(3)],
        )

        rows = extract_api.extract(CFG, watermark=0)

        assert len(rows) == CFG["max_pages"] * 3

    def test_persistent_failure_keeps_partial_rows(self, monkeypatch):
        def fetch(cfg, page):
            if page == 2:
                raise urllib.error.URLError("boom")
            return [product("a", 300), product("b", 200), product("c", 100)]

        monkeypatch.setattr(extract_api, "fetch_page", fetch)

        rows = extract_api.extract(CFG, watermark=0)

        # page 1 survived; the next run resumes from the watermark
        assert [r["code"] for r in rows] == ["a", "b", "c"]

    def test_missing_last_modified_treated_as_stale(self, monkeypatch):
        pages = {1: [product("a", 300), {"code": "no-timestamp"}]}
        monkeypatch.setattr(extract_api, "fetch_page", lambda cfg, page: pages[page])

        rows = extract_api.extract(CFG, watermark=100)

        assert [r["code"] for r in rows] == ["a"]


class FakeResponse:
    def __init__(self, payload):
        self._body = io.BytesIO(json.dumps(payload).encode())

    def read(self, *args):
        return self._body.read(*args)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class TestFetchPage:
    """Retry/backoff: 5xx retries, 4xx raises straight away."""

    def test_retries_5xx_then_succeeds(self, monkeypatch):
        calls = {"n": 0}

        def fake_urlopen(req, timeout):
            calls["n"] += 1
            if calls["n"] < 3:
                raise urllib.error.HTTPError(req.full_url, 503, "unavailable", None, None)
            return FakeResponse({"products": [product("a", 1)]})

        sleeps = []
        monkeypatch.setattr(extract_api.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setattr(extract_api.time, "sleep", sleeps.append)

        products = extract_api.fetch_page(CFG, page=1)

        assert [p["code"] for p in products] == ["a"]
        assert calls["n"] == 3
        assert sleeps == [2, 4]  # exponential backoff between attempts

    def test_4xx_raises_without_retry(self, monkeypatch):
        calls = {"n": 0}

        def fake_urlopen(req, timeout):
            calls["n"] += 1
            raise urllib.error.HTTPError(req.full_url, 404, "not found", None, None)

        monkeypatch.setattr(extract_api.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setattr(extract_api.time, "sleep", lambda s: None)

        with pytest.raises(urllib.error.HTTPError):
            extract_api.fetch_page(CFG, page=1)

        assert calls["n"] == 1

    def test_gives_up_after_max_retries(self, monkeypatch):
        def fake_urlopen(req, timeout):
            raise urllib.error.HTTPError(req.full_url, 500, "server error", None, None)

        monkeypatch.setattr(extract_api.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setattr(extract_api.time, "sleep", lambda s: None)

        with pytest.raises(urllib.error.HTTPError):
            extract_api.fetch_page(CFG, page=1, max_retries=3)

    def test_backoff_is_capped(self, monkeypatch):
        def fake_urlopen(req, timeout):
            raise urllib.error.HTTPError(req.full_url, 500, "server error", None, None)

        sleeps = []
        monkeypatch.setattr(extract_api.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setattr(extract_api.time, "sleep", sleeps.append)

        with pytest.raises(urllib.error.HTTPError):
            extract_api.fetch_page(CFG, page=1, max_retries=8)

        assert max(sleeps) == 30
