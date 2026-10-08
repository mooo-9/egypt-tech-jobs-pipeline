import json
import re
from pathlib import Path

import pytest
import responses

from pipeline.extract import amazon, eightfold, jibe, oracle_cloud, phenom
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, in_egypt

FIXTURES = Path(__file__).parent / "fixtures" / "http"
TODAY = "2026-10-07"

# system -> (module, company, fixture, HTTP method, url pattern, titles the Egypt filter must drop,
#            posting_id of the first posting, whether the list carries descriptions)
SYSTEMS = {
    "oracle_cloud": (
        oracle_cloud,
        {"key": "oracle", "oracle_cloud": [{"host": "eeho.fa.us2.oraclecloud.com", "site": "CX_45001",
                                            "job_url": "https://careers.oracle.com/en/sites/jobsearch/job/"}]},
        "oracle_cloud_oracle_list.json", responses.GET, r"https://eeho\.fa\.us2\.oraclecloud\.com/hcmRestApi/.*",
        {"Hand-made Dubai Consultant"}, "eeho.fa.us2.oraclecloud.com:344587", False),
    "eightfold": (
        eightfold,
        {"key": "ericsson", "eightfold": [{"host": "jobs.ericsson.com", "domain": "ericsson.com"}]},
        "eightfold_ericsson_list.json", responses.GET, r"https://jobs\.ericsson\.com/api/pcsx/search.*",
        {"Hand-made Lahore Engineer"}, "jobs.ericsson.com:563121777323844", False),
    "jibe": (
        jibe,
        {"key": "pepsico", "jibe": [{"host": "www.pepsicojobs.com"}]},
        "jibe_pepsico_list.json", responses.GET, r"https://www\.pepsicojobs\.com/api/jobs.*",
        {"Hand-made Berlin Analyst"}, "www.pepsicojobs.com:468410", True),
    "phenom": (
        phenom,
        {"key": "maf", "phenom": [{"host": "careers.majidalfuttaim.com", "ref": "MAFMAFGLOBAL"}]},
        "phenom_maf_list.json", responses.POST, r"https://careers\.majidalfuttaim\.com/widgets",
        {"Hand-made Dubai Manager"}, "careers.majidalfuttaim.com:20193", False),
    "amazon": (
        amazon,
        {"key": "amazon", "amazon": [{"country": "EGY"}]},
        "amazon_amazon_list.json", responses.GET, r"https://www\.amazon\.jobs/en/search\.json.*",
        {"Hand-made Seattle SDE"}, "10566397", True),
}


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("system", SYSTEMS)
@responses.activate
def test_returns_egypt_postings_with_ids(system):
    module, company, fixture_name, method, url, dropped, first_id, has_description = SYSTEMS[system]
    responses.add(method, re.compile(url), json=fixture(fixture_name))
    postings = module.fetch(company, TODAY)

    assert postings
    assert all(in_egypt(p.location, p.title) for p in postings)
    assert not dropped & {p.title for p in postings}
    ids = [p.posting_id for p in postings]
    assert len(set(ids)) == len(ids) and ids[0] == first_id
    for p in postings:
        assert (p.source_system, p.company_key, p.collected_at) == (system, company["key"], TODAY)
        assert p.title and p.url.startswith("https://") and p.posted_raw
        assert (p.description is not None) == has_description
    assert EXTRACTORS[system] is module.fetch
    assert system not in DESCRIBERS


@responses.activate
def test_each_system_maps_its_own_fields():
    def first(system):
        module, company, fixture_name, method, url, *_ = SYSTEMS[system]
        responses.add(method, re.compile(url), json=fixture(fixture_name))
        return module.fetch(company, TODAY)[0]

    p = first("oracle_cloud")
    assert (p.location, p.posted_raw) == ("CAIRO, Egypt", "2026-09-07")
    assert p.url == "https://careers.oracle.com/en/sites/jobsearch/job/344587"
    p = first("eightfold")
    assert p.url == "https://jobs.ericsson.com/careers/job/563121777323844"
    assert p.location == "Cairo,Cairo,Egypt" and p.posted_raw == "2026-09-29"
    p = first("jibe")
    assert p.url == "https://www.pepsicojobs.com/main/jobs/468410" and p.location == "Assiut, Egypt"
    assert p.posted_raw == "2026-09-07" and "<" not in p.description and p.description.startswith("Overview")
    p = first("phenom")
    assert p.url == "https://careers.majidalfuttaim.com/global/en/job/20193"
    assert (p.location, p.posted_raw) == ("Cairo, Egypt", "2026-06-01")
    p = first("amazon")
    assert p.url == "https://www.amazon.jobs/en/jobs/10566397/rush-delivery-supervisor-eg-last-mile"
    assert p.posted_raw == "October  1, 2026" and p.description.startswith("We")


@responses.activate
def test_eightfold_keeps_only_a_posting_s_egypt_locations():
    _, company, fixture_name, method, url, *_ = SYSTEMS["eightfold"]
    responses.add(method, re.compile(url), json=fixture(fixture_name))
    multi = next(p for p in eightfold.fetch(company, TODAY) if p.title == "BSS Integration Engineer")
    assert multi.location == "Smart Village,Cairo,Egypt; Cairo,Cairo,Egypt"


@responses.activate
def test_amazon_keeps_egyptian_cities_the_filter_does_not_name():
    _, company, fixture_name, method, url, *_ = SYSTEMS["amazon"]
    responses.add(method, re.compile(url), json=fixture(fixture_name))
    mansoura = next(p for p in amazon.fetch(company, TODAY) if "Mansoura" in p.title)
    assert in_egypt(mansoura.location)


@responses.activate
def test_jibe_stops_at_max_pages():
    page = fixture("jibe_pepsico_list.json")
    page["totalCount"] = 10_000  # more pages on offer than we will ever read
    responses.add(responses.GET, re.compile(r"https://www\.pepsicojobs\.com/api/jobs.*"), json=page)
    jibe.fetch(SYSTEMS["jibe"][1], TODAY)
    assert len(responses.calls) == jibe.MAX_PAGES == 20
