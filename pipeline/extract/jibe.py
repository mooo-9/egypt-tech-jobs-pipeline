"""Jibe public job search (PepsiCo careers site). Its list carries each job's full text."""
from pipeline.extract.common import EXTRACTORS, TRUNCATED, Posting, clean_text, configs, http_get_json, in_egypt

MAX_PAGES = 20  # Jibe answers 10 at a time; PepsiCo lists about 100 in Egypt


def _collect(company: dict, site: dict) -> list[dict]:
    jobs, seen, page, total = [], 0, 1, 0
    while page <= MAX_PAGES:
        found = http_get_json(f"https://{site['host']}/api/jobs", {"location": "Egypt", "page": page})
        batch = [j["data"] for j in found.get("jobs", [])]
        jobs += batch
        seen += len(batch)
        total = found.get("totalCount") or 0
        if not batch or seen >= total:
            break
        page += 1
    if total > seen:
        TRUNCATED.add(company["key"])
    return jobs


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for site in configs(company, "jibe"):
        for d in _collect(company, site):
            where = d.get("full_location") or f"{d.get('city', '')}, {d.get('country', '')}"
            if not in_egypt(where, d["title"]):
                continue
            postings.append(Posting(
                source_system="jibe", company_key=company["key"],
                posting_id=f"{site['host']}:{d['slug']}",  # slugs are per Jibe site
                title=d["title"], location=where, posted_raw=(d.get("posted_date") or "")[:10],
                url=f"https://{site['host']}/main/jobs/{d['slug']}",
                description=clean_text(d.get("description", "")), collected_at=collected_at))
    return postings


EXTRACTORS["jibe"] = fetch
