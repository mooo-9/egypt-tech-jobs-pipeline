"""Workday's public job search, the one behind a company's careers page."""
import re
from urllib.parse import urlparse

from pipeline.extract.common import (
    DESCRIBERS, EXTRACTORS, Posting, clean_text, http_get_json, http_post_json, in_egypt,
)

PAGE = 20  # Workday answers 20 postings at a time
MAX_POSTINGS = 100  # five pages is past any company's Egypt list
# A multi-location posting shows as "2 Locations"; the search was already for Egypt.
_MANY_LOCATIONS = re.compile(r"\d+ Locations", re.IGNORECASE)


def _sites(company: dict) -> list[dict]:
    config = company["workday"]
    return [config] if isinstance(config, dict) else config


def _collect(site: dict) -> list[dict]:
    """Every listed posting for one site. Workday gives the total only on the first page."""
    url = f"https://{site['host']}/wday/cxs/{site['tenant']}/{site['site']}/jobs"
    found, total = [], None
    while len(found) < MAX_POSTINGS:
        page = http_post_json(url, {"appliedFacets": {}, "limit": PAGE,
                                    "offset": len(found), "searchText": "Egypt"})
        batch = page.get("jobPostings", [])
        found += batch
        total = page.get("total", 0) if total is None else total
        if not batch or len(found) >= total:
            break
    return found


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for site in _sites(company):
        for p in _collect(site):
            where, title, path = p.get("locationsText", ""), p.get("title", ""), p.get("externalPath", "")
            if not (in_egypt(where, title) or _MANY_LOCATIONS.fullmatch(where)):
                continue
            postings.append(Posting(
                source_system="workday", company_key=company["key"],
                posting_id=f"{site['tenant']}:{path.rsplit('_', 1)[-1]}",  # requisition ids repeat across tenants
                title=title, location=where, posted_raw=p.get("postedOn", ""),
                url=f"https://{site['host']}/en-US/{site['site']}{path}",
                description=None, collected_at=collected_at))
    return postings


def describe(company: dict, posting: Posting) -> str | None:
    """The posting's text, read from the site its url points at."""
    parts = urlparse(posting.url)
    _, _, site_name, path = parts.path.split("/", 3)  # /en-US/{site}/job/...
    path = "/" + path
    site = next(s for s in _sites(company) if s["host"] == parts.netloc and s["site"] == site_name)
    info = http_get_json(f"https://{site['host']}/wday/cxs/{site['tenant']}/{site['site']}{path}")
    return clean_text(info["jobPostingInfo"].get("jobDescription"))


EXTRACTORS["workday"] = fetch
DESCRIBERS["workday"] = describe
