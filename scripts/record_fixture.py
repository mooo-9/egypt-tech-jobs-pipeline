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


if __name__ == "__main__":
    system, key = sys.argv[1:3]
    {"workday": record_workday, "smartrecruiters": record_smartrecruiters}[system](key, CONFIGS[(system, key)])
