"""SmartRecruiters' public postings API."""
from pipeline.extract.common import (
    DESCRIBERS, EXTRACTORS, TRUNCATED, Posting, clean_text, http_get_json, in_egypt,
)

API = "https://api.smartrecruiters.com/v1/companies"


def _configs(company: dict) -> list[dict]:
    config = company["smartrecruiters"]
    return [config] if isinstance(config, dict) else config


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in _configs(company):
        # ponytail: one page of 100, no paging; add an offset loop if a company lists more in Egypt (the run summary flags it)
        page = http_get_json(f"{API}/{config['company']}/postings", {"country": "eg", "limit": 100})
        if (page.get("totalFound") or 0) > len(page.get("content") or []):
            TRUNCATED.add(company["key"])
        for p in page.get("content", []):
            where = (p.get("location") or {}).get("fullLocation") or ""
            if not in_egypt(where, p["name"]):
                continue
            postings.append(Posting(
                source_system="smartrecruiters", company_key=company["key"], posting_id=p["id"],
                title=p["name"], location=where, posted_raw=(p.get("releasedDate") or "")[:10],
                url=f"https://jobs.smartrecruiters.com/{config['company']}/{p['id']}",
                description=None, collected_at=collected_at))
    return postings


def describe(company: dict, posting: Posting) -> str | None:
    """The posting's sections joined into one text; the url names its company."""
    smart_company = posting.url.rsplit("/", 2)[1]
    ad = http_get_json(f"{API}/{smart_company}/postings/{posting.posting_id}")["jobAd"]
    return clean_text(" ".join(s.get("text", "") for s in ad["sections"].values()))


EXTRACTORS["smartrecruiters"] = fetch
DESCRIBERS["smartrecruiters"] = describe
