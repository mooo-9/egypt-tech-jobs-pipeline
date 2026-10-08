"""Lever job sites. The list call carries each posting's description and requirement lists."""
from datetime import datetime, timezone

from pipeline.extract.common import EXTRACTORS, Posting, clean_text, configs, http_get_json, in_egypt


def _description(j: dict) -> str | None:
    html = j.get("description") or ""
    for section in j.get("lists") or []:
        html += f"<div>{section.get('text') or ''}</div><ul>{section.get('content') or ''}</ul>"
    html += j.get("additional") or ""
    return clean_text(html) or None


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "lever"):
        for j in http_get_json(f"https://api.lever.co/v0/postings/{config['site']}?mode=json"):
            cats = j.get("categories") or {}
            # a posting can list many sites; keep it when any of them is Egyptian
            places = [cats.get("location")] + (cats.get("allLocations") or [])
            where = "; ".join(dict.fromkeys(p.strip() for p in places if p))
            if not in_egypt(where, j["text"]):
                continue
            created = j.get("createdAt")
            postings.append(Posting(
                source_system="lever", company_key=company["key"],
                posting_id=j["id"],  # Lever ids are UUIDs, so bare
                title=j["text"].strip(), location=where,
                posted_raw=datetime.fromtimestamp(created / 1000, timezone.utc).date().isoformat() if created else "",
                url=j["hostedUrl"], description=_description(j), collected_at=collected_at))
    return postings


EXTRACTORS["lever"] = fetch
