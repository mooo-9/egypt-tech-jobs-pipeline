import json
from pathlib import Path

import responses

from pipeline.extract import workday
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, in_egypt

FIXTURES = Path(__file__).parent / "fixtures" / "http"
SITE = {"host": "mastercard.wd1.myworkdayjobs.com", "tenant": "mastercard", "site": "CorporateCareers"}
COMPANY = {"key": "mastercard", "name": "Mastercard", "workday": SITE}
JOBS_URL = "https://mastercard.wd1.myworkdayjobs.com/wday/cxs/mastercard/CorporateCareers/jobs"
TODAY = "2026-10-07"


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@responses.activate
def test_workday_keeps_only_egypt():
    responses.add(responses.POST, JOBS_URL, json=fixture("workday_mastercard_list.json"))
    postings = workday.fetch(COMPANY, TODAY)
    assert len(postings) == 4  # the Dubai posting is dropped, the "2 Locations" one kept
    for p in postings:
        assert in_egypt(p.location, p.title) or p.location == "2 Locations"
        assert p.source_system == "workday" and p.company_key == "mastercard"
        assert p.posting_id and p.description is None and p.collected_at == TODAY
    assert postings[0].url == (
        "https://mastercard.wd1.myworkdayjobs.com/en-US/CorporateCareers"
        "/job/Cairo-Egypt/Manager--Specialist-Sales--Digital-Payments_R-290246-1")
    assert postings[0].posted_raw == "Posted Yesterday"
    assert EXTRACTORS["workday"] is workday.fetch and DESCRIBERS["workday"] is workday.describe


@responses.activate
def test_workday_keeps_multi_location_postings():
    responses.add(responses.POST, JOBS_URL, json=fixture("workday_mastercard_list.json"))
    titles = {p.title: p.location for p in workday.fetch(COMPANY, TODAY)}
    assert titles["Data Analyst"] == "2 Locations"
    assert "Software Engineer II" not in titles  # Dubai


@responses.activate
def test_workday_paginates_until_total():
    template = fixture("workday_mastercard_list.json")["jobPostings"][0]

    def page(first, last):
        return [{**template, "externalPath": f"/job/Cairo-Egypt/Job_R-{i}"} for i in range(first, last)]

    responses.add(responses.POST, JOBS_URL, json={"total": 25, "jobPostings": page(0, 20)})
    responses.add(responses.POST, JOBS_URL, json={"jobPostings": page(20, 25)})  # total only on page 1
    postings = workday.fetch(COMPANY, TODAY)
    assert [p.posting_id for p in postings] == [f"mastercard:R-{i}" for i in range(25)]
    assert [json.loads(c.request.body)["offset"] for c in responses.calls] == [0, 20]


@responses.activate
def test_workday_posting_id_is_stable():
    reordered = fixture("workday_mastercard_list.json")
    reordered["jobPostings"].reverse()
    responses.add(responses.POST, JOBS_URL, json=fixture("workday_mastercard_list.json"))
    responses.add(responses.POST, JOBS_URL, json=reordered)
    first = {p.title: p.posting_id for p in workday.fetch(COMPANY, TODAY)}
    second = {p.title: p.posting_id for p in workday.fetch(COMPANY, TODAY)}
    assert first == second
    assert first["Manager, Specialist Sales, Digital Payments"] == "mastercard:R-290246-1"


@responses.activate
def test_workday_collects_every_site_and_describes_from_its_own():
    other = {"host": "mastercard.wd1.myworkdayjobs.com", "tenant": "mastercard", "site": "Other"}
    company = {**COMPANY, "workday": [SITE, other]}
    other_url = JOBS_URL.replace("CorporateCareers", "Other")
    responses.add(responses.POST, JOBS_URL, json=fixture("workday_mastercard_list.json"))
    responses.add(responses.POST, other_url, json=fixture("workday_mastercard_list.json"))
    postings = workday.fetch(company, TODAY)
    assert len(postings) == 8
    on_other = next(p for p in postings if "/en-US/Other/" in p.url)
    detail_url = other_url.removesuffix("/jobs") + on_other.url.split("/en-US/Other")[1]
    responses.add(responses.GET, detail_url, json=fixture("workday_mastercard_detail.json"))
    assert workday.describe(company, on_other)


@responses.activate
def test_workday_describe_returns_clean_text():
    responses.add(responses.POST, JOBS_URL, json=fixture("workday_mastercard_list.json"))
    posting = next(p for p in workday.fetch(COMPANY, TODAY) if p.posting_id == "mastercard:R-286532")
    detail_url = JOBS_URL.removesuffix("/jobs") + "/job/Cairo-Egypt/Director-Specialist-Sales---Egypt_R-286532"
    responses.add(responses.GET, detail_url, json=fixture("workday_mastercard_detail.json"))
    text = workday.describe(COMPANY, posting)
    assert text.startswith("Our Purpose")
    assert "<" not in text and ">" not in text
