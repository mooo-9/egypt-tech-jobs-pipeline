"""Shared pieces for every extractor: the Posting record, polite HTTP, text cleanup."""
import re
import time
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

USER_AGENT = "egypt-tech-jobs-pipeline (+https://github.com/mooo-9/egypt-tech-jobs-pipeline)"
TIMEOUT = 20
RETRIES = 3
MIN_INTERVAL = 1.0  # seconds between requests to one host

_EGYPT = re.compile(r"egypt|cairo|giza|alexandria", re.IGNORECASE)
_session = requests.Session()
_session.headers["User-Agent"] = USER_AGENT
_last_call: dict[str, float] = {}


@dataclass(frozen=True)
class Posting:
    source_system: str
    company_key: str
    posting_id: str
    title: str
    location: str
    posted_raw: str
    url: str
    description: str | None
    collected_at: str  # ISO date


# Filled by the extractor modules as they are imported (see load_all).
EXTRACTORS: dict[str, Callable[[dict, str], list[Posting]]] = {}
DESCRIBERS: dict[str, Callable[[dict, Posting], str | None]] = {}


def in_egypt(location: str, title: str = "") -> bool:
    return bool(_EGYPT.search(location or title))


def clean_text(html: str | None) -> str | None:
    if html is None:
        return None
    return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)


def _wait_for_host(url: str) -> None:
    host = urlparse(url).netloc
    last = _last_call.get(host)
    if last is not None:
        wait = MIN_INTERVAL - (time.monotonic() - last)
        if wait > 0:
            time.sleep(wait)
    _last_call[host] = time.monotonic()


def _request_json(method: str, url: str, **kwargs) -> dict:
    for attempt in range(RETRIES + 1):
        _wait_for_host(url)
        try:
            resp = _session.request(method, url, timeout=TIMEOUT, **kwargs)
            if resp.status_code < 500 and resp.status_code != 429:
                resp.raise_for_status()
                return resp.json()
            error = requests.HTTPError(f"{resp.status_code} for {url}", response=resp)
        except (requests.ConnectionError, requests.Timeout) as exc:
            error = exc
        if attempt == RETRIES:
            raise error
        time.sleep(2 ** attempt)  # backoff: 1s, 2s, 4s


def http_get_json(url: str, params: dict | None = None) -> dict:
    return _request_json("GET", url, params=params)


def http_post_json(url: str, body: dict) -> dict:
    return _request_json("POST", url, json=body)


def configs(company: dict, system: str) -> list[dict]:
    """A company's config for one system: one dict, or a list when it has several sites."""
    config = company[system]
    return [config] if isinstance(config, dict) else config
