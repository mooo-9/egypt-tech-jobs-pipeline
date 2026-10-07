import time

import pytest
import responses

from pipeline.extract import common
from pipeline.extract.common import clean_text, http_get_json, in_egypt

UA = "egypt-tech-jobs-pipeline (+https://github.com/mooo-9/egypt-tech-jobs-pipeline)"


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Tests never wait on retries or rate limiting."""
    common._last_call.clear()
    monkeypatch.setattr(common.time, "sleep", lambda s: None)


def test_in_egypt_matches_cities():
    assert in_egypt("New Cairo, Egypt")
    assert in_egypt("Giza")
    assert in_egypt("", "Analyst - Alexandria")
    assert not in_egypt("Dubai, UAE")


def test_clean_text_strips_html_keeps_arabic():
    out = clean_text("<li>SQL &amp; Python</li><p>مهندس بيانات</p>")
    assert "SQL & Python" in out
    assert "مهندس بيانات" in out
    assert "<" not in out
    assert clean_text(None) is None


@responses.activate
def test_http_get_json_retries_then_succeeds():
    url = "https://example.com/api"
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, status=503)
    responses.add(responses.GET, url, json={"ok": 1})
    assert http_get_json(url) == {"ok": 1}
    assert len(responses.calls) == 3


@responses.activate
def test_http_sends_user_agent():
    responses.add(responses.GET, "https://example.com/api", json={})
    responses.add(responses.POST, "https://example.com/api", json={})
    http_get_json("https://example.com/api")
    common.http_post_json("https://example.com/api", {"a": 1})
    assert [c.request.headers["User-Agent"] for c in responses.calls] == [UA, UA]


@responses.activate
def test_rate_limit_per_host(monkeypatch):
    now = [100.0]
    sleeps = []
    monkeypatch.setattr(common.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(common.time, "sleep", sleeps.append)
    for host in ("a.example.com", "b.example.com"):
        responses.add(responses.GET, f"https://{host}/x", json={})

    http_get_json("https://a.example.com/x")
    http_get_json("https://b.example.com/x")
    assert sleeps == []

    http_get_json("https://a.example.com/x")
    assert sleeps == [pytest.approx(1.0)]
