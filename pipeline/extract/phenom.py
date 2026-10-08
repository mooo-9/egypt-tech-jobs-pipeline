"""A Phenom careers site's own search, the call its search page makes (BCG, Majid Al Futtaim)."""
from pipeline.extract.common import EXTRACTORS, TRUNCATED, Posting, configs, http_post_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for site in configs(company, "phenom"):
        # ponytail: one page of 100, no paging; add a "from" loop if a company lists more in Egypt (the run summary flags it)
        found = http_post_json(f"https://{site['host']}/widgets", {
            "ddoKey": "refineSearch", "refNum": site["ref"], "lang": "en_global",
            "siteType": "external", "from": 0, "size": 100, "jobs": True,
            "selected_fields": {"country": ["Egypt"]}})
        jobs = found["refineSearch"]["data"]["jobs"]
        if (found["refineSearch"].get("totalHits") or 0) > len(jobs):
            TRUNCATED.add(company["key"])
        for j in jobs:
            where = j.get("cityStateCountry") or j.get("location") or ""
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="phenom", company_key=company["key"],
                posting_id=f"{site['host']}:{j['jobId']}",  # job ids are per Phenom site
                title=j["title"], location=where, posted_raw=(j.get("postedDate") or "")[:10],
                url=f"https://{site['host']}/global/en/job/{j['jobId']}",
                description=None, collected_at=collected_at))
    return postings


EXTRACTORS["phenom"] = fetch
