"""Greenhouse job boards. The list call with content=true carries each job's description."""
from html import unescape

from pipeline.extract.common import EXTRACTORS, Posting, clean_text, configs, http_get_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "greenhouse"):
        board = config["board"]
        page = http_get_json(f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true")
        for j in page.get("jobs", []):
            where = ((j.get("location") or {}).get("name") or "").strip()
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="greenhouse", company_key=company["key"],
                posting_id=f"{board}:{j['id']}",  # ids are not proven unique across boards
                title=j["title"].strip(), location=where,
                posted_raw=j.get("first_published") or j.get("updated_at") or "",
                url=j["absolute_url"],
                # the content field is HTML escaped once, so unescape before stripping tags
                description=clean_text(unescape(j.get("content") or "")) or None, collected_at=collected_at))
    return postings


EXTRACTORS["greenhouse"] = fetch
