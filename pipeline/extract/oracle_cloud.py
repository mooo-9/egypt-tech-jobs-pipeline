"""Oracle Recruiting Cloud public search, as a company careers page asks it for Egypt."""
from pipeline.extract.common import EXTRACTORS, Posting, configs, http_get_json, in_egypt


def fetch(company: dict, collected_at: str) -> list[Posting]:
    postings = []
    for site in configs(company, "oracle_cloud"):
        url = (f"https://{site['host']}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
               "?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values"
               f"&finder=findReqs;siteNumber={site['site']},facetsList=LOCATIONS%3BFLEX_FIELDS,"
               "limit=100,location=Egypt,sortBy=POSTING_DATES_DESC")
        # ponytail: one page of 100, no paging; add an offset loop if a company lists more in Egypt
        for r in http_get_json(url)["items"][0]["requisitionList"]:
            where = r.get("PrimaryLocation", "")
            if not in_egypt(where, r["Title"]):
                continue
            postings.append(Posting(
                source_system="oracle_cloud", company_key=company["key"],
                posting_id=f"{site['host']}:{r['Id']}",  # requisition ids are per Oracle instance
                title=r["Title"], location=where, posted_raw=r.get("PostedDate", ""),
                url=site["job_url"] + str(r["Id"]), description=None, collected_at=collected_at))
    return postings


EXTRACTORS["oracle_cloud"] = fetch
