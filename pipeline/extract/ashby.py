"""Ashby job boards. The list call carries each posting's description."""
from pipeline.extract.common import EXTRACTORS, Posting, clean_text, configs, http_get_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "ashby"):
        page = http_get_json(f"https://api.ashbyhq.com/posting-api/job-board/{config['board']}")
        for j in page.get("jobs", []):
            places = [j.get("location")] + [s.get("location") for s in j.get("secondaryLocations") or []]
            where = "; ".join(dict.fromkeys(p.strip() for p in places if p))
            country = (((j.get("address") or {}).get("postalAddress")) or {}).get("addressCountry") or ""
            if country == "Egypt" and not in_egypt(where):
                where += ", Egypt"  # "HQ Office" names no place the filter knows
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="ashby", company_key=company["key"],
                posting_id=j["id"],  # Ashby ids are UUIDs, so bare
                title=j["title"].strip(), location=where, posted_raw=j.get("publishedAt") or "",
                url=j["jobUrl"],
                description=(clean_text(j.get("descriptionHtml")) or j.get("descriptionPlain") or None),
                collected_at=collected_at))
    return postings


EXTRACTORS["ashby"] = fetch
