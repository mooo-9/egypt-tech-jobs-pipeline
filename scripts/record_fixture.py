"""Save live responses for one company into tests/fixtures/http/. The only code that
calls the network outside a real run.   Usage: record_fixture.py <system> <company_key>"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.extract.common import http_get_json, http_post_json  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "http"

# Known-good configs to record from; the real company list lives elsewhere.
CONFIGS = {
    ("workday", "mastercard"): {"host": "mastercard.wd1.myworkdayjobs.com", "tenant": "mastercard", "site": "CorporateCareers"},
    ("workday", "visa"): {"host": "visa.wd5.myworkdayjobs.com", "tenant": "visa", "site": "Visa"},
    ("workday", "pfizer"): {"host": "pfizer.wd1.myworkdayjobs.com", "tenant": "pfizer", "site": "PfizerCareers"},
    ("workday", "pwc"): {"host": "pwc.wd3.myworkdayjobs.com", "tenant": "pwc", "site": "global_experienced_careers"},
    ("smartrecruiters", "talabat"): {"company": "DeliveryHero"},
    ("oracle_cloud", "dell"): {"host": "enterpriseplatform.dell.com", "site": "CX_1001",
                               "job_url": "https://enterpriseplatform.dell.com/hcmUI/CandidateExperience/en/sites/careers/job/"},
    ("oracle_cloud", "oracle"): {"host": "eeho.fa.us2.oraclecloud.com", "site": "CX_45001",
                                 "job_url": "https://careers.oracle.com/en/sites/jobsearch/job/"},
    ("eightfold", "ericsson"): {"host": "jobs.ericsson.com", "domain": "ericsson.com"},
    ("jibe", "pepsico"): {"host": "www.pepsicojobs.com"},
    ("phenom", "bcg"): {"host": "careers.bcg.com", "ref": "BCG1US"},
    ("phenom", "maf"): {"host": "careers.majidalfuttaim.com", "ref": "MAFMAFGLOBAL"},
    ("amazon", "amazon"): {"country": "EGY"},
}


def save(name: str, data: dict) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print("saved", name)


def record_workday(key: str, c: dict) -> None:
    base = f"https://{c['host']}/wday/cxs/{c['tenant']}/{c['site']}"
    page = http_post_json(f"{base}/jobs", {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "Egypt"})
    save(f"workday_{key}_list.json", page)
    path = page["jobPostings"][0]["externalPath"]
    save(f"workday_{key}_detail.json", http_get_json(base + path))


def record_smartrecruiters(key: str, c: dict) -> None:
    base = f"https://api.smartrecruiters.com/v1/companies/{c['company']}/postings"
    page = http_get_json(base, {"country": "eg", "limit": 100})
    save(f"smartrecruiters_{key}_list.json", page)
    save(f"smartrecruiters_{key}_detail.json", http_get_json(f"{base}/{page['content'][0]['id']}"))


def record_oracle_cloud(key: str, c: dict) -> None:
    url = (f"https://{c['host']}/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
           "?onlyData=true&expand=requisitionList.secondaryLocations,flexFieldsFacet.values"
           f"&finder=findReqs;siteNumber={c['site']},facetsList=LOCATIONS%3BFLEX_FIELDS,"
           "limit=100,location=Egypt,sortBy=POSTING_DATES_DESC")
    save(f"oracle_cloud_{key}_list.json", http_get_json(url))


def record_eightfold(key: str, c: dict) -> None:
    save(f"eightfold_{key}_list.json", http_get_json(
        f"https://{c['host']}/api/pcsx/search", {"domain": c["domain"], "query": "", "location": "Egypt", "start": 0}))


def record_jibe(key: str, c: dict) -> None:
    save(f"jibe_{key}_list.json", http_get_json(f"https://{c['host']}/api/jobs", {"location": "Egypt", "page": 1}))


def record_phenom(key: str, c: dict) -> None:
    save(f"phenom_{key}_list.json", http_post_json(f"https://{c['host']}/widgets", {
        "ddoKey": "refineSearch", "refNum": c["ref"], "lang": "en_global", "siteType": "external",
        "from": 0, "size": 100, "jobs": True, "selected_fields": {"country": ["Egypt"]}}))


def record_amazon(key: str, c: dict) -> None:
    save(f"amazon_{key}_list.json", http_get_json(
        "https://www.amazon.jobs/en/search.json", {"normalized_country_code[]": c["country"], "result_limit": 100}))


RECORDERS = {"workday": record_workday, "smartrecruiters": record_smartrecruiters,
             "oracle_cloud": record_oracle_cloud, "eightfold": record_eightfold, "jibe": record_jibe,
             "phenom": record_phenom, "amazon": record_amazon}

if __name__ == "__main__":
    system, key = sys.argv[1:3]
    RECORDERS[system](key, CONFIGS[(system, key)])
