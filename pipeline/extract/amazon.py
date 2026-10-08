"""amazon.jobs own search, for one country. Its list carries each job's description."""
from pipeline.extract.common import EXTRACTORS, TRUNCATED, Posting, clean_text, configs, http_get_json, in_egypt

PAGE = 100
MAX_POSTINGS = 1000  # Egypt lists about 120; the cap only stops a runaway loop


def _collect(company: dict, country: str) -> list[dict]:
    """Every job the search lists, paged by offset until its reported hits are read."""
    jobs, hits = [], 0
    while len(jobs) < MAX_POSTINGS:
        page = http_get_json("https://www.amazon.jobs/en/search.json",
                             {"normalized_country_code[]": country, "result_limit": PAGE, "offset": len(jobs)})
        batch, hits = page.get("jobs") or [], page.get("hits") or 0
        jobs += batch
        if not batch or len(jobs) >= hits:
            break
    if hits > len(jobs):
        TRUNCATED.add(company["key"])
    return jobs


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "amazon"):
        for j in _collect(company, config["country"]):
            where = j.get("location") or ""
            if j.get("country_code") == "EGY" and not in_egypt(where):
                where += ", Egypt"  # "EG, Mansoura" names no Egyptian city the filter knows
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="amazon", company_key=company["key"],
                posting_id=j["id_icims"],  # Amazon job ids are global, so bare
                title=j["title"], location=where, posted_raw=j.get("posted_date") or "",
                url="https://www.amazon.jobs" + j["job_path"],
                description=clean_text(j.get("description")), collected_at=collected_at))
    return postings


EXTRACTORS["amazon"] = fetch
