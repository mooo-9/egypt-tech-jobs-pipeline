"""Workable career pages via the public widget API. details=true adds each description."""
from pipeline.extract.common import EXTRACTORS, Posting, clean_text, configs, http_get_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "workable"):
        account = config["account"]
        page = http_get_json(f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true")
        for j in page.get("jobs", []):
            places = j.get("locations") or [j]  # older entries carry only the top-level city and country
            where = "; ".join(dict.fromkeys(
                ", ".join(x for x in (p.get("city"), p.get("country")) if x) for p in places))
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="workable", company_key=company["key"],
                posting_id=f"{account}:{j['shortcode']}",  # shortcodes are per account
                title=j["title"].strip(), location=where, posted_raw=j.get("published_on") or "",
                url=j["url"], description=clean_text(j.get("description")) or None, collected_at=collected_at))
    return postings


EXTRACTORS["workable"] = fetch
