"""Eightfold public search (Ericsson careers site)."""
from datetime import datetime, timezone

from pipeline.extract.common import EXTRACTORS, Posting, configs, http_get_json, in_egypt


def _collect(site: dict) -> list[dict]:
    positions, start = [], 0
    while True:
        data = http_get_json(f"https://{site['host']}/api/pcsx/search",
                             {"domain": site["domain"], "query": "", "location": "Egypt", "start": start})["data"]
        found = data.get("positions", [])
        positions += found
        start += len(found)
        if not found or start >= data.get("count", 0):
            return positions


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for site in configs(company, "eightfold"):
        for p in _collect(site):
            # a posting open in several countries keeps only its Egypt locations
            where = "; ".join(l for l in p.get("locations", []) if in_egypt(l))
            if not in_egypt(where, p["name"]):
                continue
            ts = p.get("postedTs")
            postings.append(Posting(
                source_system="eightfold", company_key=company["key"],
                posting_id=f"{site['host']}:{p['id']}",  # ids are per Eightfold tenant
                title=p["name"], location=where,
                posted_raw=datetime.fromtimestamp(ts, timezone.utc).date().isoformat() if ts else "",
                url=f"https://{site['host']}{p['positionUrl']}", description=None, collected_at=collected_at))
    return postings


EXTRACTORS["eightfold"] = fetch
