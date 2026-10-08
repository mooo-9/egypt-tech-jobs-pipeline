"""amazon.jobs own search, for one country. Its list carries each job's description."""
from pipeline.extract.common import EXTRACTORS, Posting, clean_text, configs, http_get_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for config in configs(company, "amazon"):
        # ponytail: one page of 100, no paging; add an offset loop if Egypt lists more
        page = http_get_json("https://www.amazon.jobs/en/search.json",
                             {"normalized_country_code[]": config["country"], "result_limit": 100})
        for j in page.get("jobs", []):
            where = j.get("location", "")
            if j.get("country_code") == "EGY" and not in_egypt(where):
                where += ", Egypt"  # "EG, Mansoura" names no Egyptian city the filter knows
            if not in_egypt(where, j["title"]):
                continue
            postings.append(Posting(
                source_system="amazon", company_key=company["key"],
                posting_id=j["id_icims"],  # Amazon job ids are global, so bare
                title=j["title"], location=where, posted_raw=j.get("posted_date", ""),
                url="https://www.amazon.jobs" + j["job_path"],
                description=clean_text(j.get("description")), collected_at=collected_at))
    return postings


EXTRACTORS["amazon"] = fetch
