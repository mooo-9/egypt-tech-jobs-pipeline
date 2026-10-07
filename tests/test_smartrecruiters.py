import json
from pathlib import Path

import responses

from pipeline.extract import smartrecruiters
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, clean_text, in_egypt

FIXTURES = Path(__file__).parent / "fixtures" / "http"
COMPANY = {"key": "talabat", "name": "Talabat", "smartrecruiters": {"company": "DeliveryHero"}}
API = "https://api.smartrecruiters.com/v1/companies/DeliveryHero/postings"
TODAY = "2026-10-07"


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@responses.activate
def test_smartrecruiters_fetch_and_describe():
    responses.add(responses.GET, API, json=fixture("smartrecruiters_talabat_list.json"))
    postings = smartrecruiters.fetch(COMPANY, TODAY)
    assert [p.title for p in postings] == ["Operations Admin - tMart", "Category Manager"]  # Berlin dropped
    first = postings[0]
    assert in_egypt(first.location, first.title)
    assert (first.source_system, first.company_key, first.posting_id) == ("smartrecruiters", "talabat", "744000154060728")
    assert first.url == "https://jobs.smartrecruiters.com/DeliveryHero/744000154060728"
    assert first.posted_raw == "2026-10-07" and first.description is None and first.collected_at == TODAY
    assert EXTRACTORS["smartrecruiters"] is smartrecruiters.fetch
    assert DESCRIBERS["smartrecruiters"] is smartrecruiters.describe

    detail = fixture("smartrecruiters_talabat_detail.json")
    responses.add(responses.GET, f"{API}/744000154060728", json=detail)
    text = smartrecruiters.describe(COMPANY, first)
    sections = detail["jobAd"]["sections"]
    assert "<" not in text
    for section in sections.values():  # every section with text is in the description
        assert clean_text(section["text"]) in text
    assert text.index(clean_text(sections["jobDescription"]["text"])) \
        < text.index(clean_text(sections["qualifications"]["text"]))
