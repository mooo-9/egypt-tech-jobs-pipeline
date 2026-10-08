import json
import re
from pathlib import Path

import pytest
import responses

from pipeline.extract import amazon, eightfold, jibe, oracle_cloud, phenom, smartrecruiters, workday
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, TRUNCATED, in_egypt

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


@responses.activate
def test_amazon_pages_with_offset_until_hits():
    _, company, fixture_name, method, url, *_ = SYSTEMS["amazon"]
    first, second = fixture(fixture_name), fixture(fixture_name)
    first["hits"] = second["hits"] = 6  # 4 jobs on page one, the last 2 on page two
    for i, j in enumerate(second["jobs"][:2]):
        j["id_icims"] = f"page2-{i}"
    second["jobs"] = second["jobs"][:2]
    responses.add(method, re.compile(url), json=first)
    responses.add(method, re.compile(url), json=second)
    ids = [p.posting_id for p in amazon.fetch(company, TODAY)]
    assert "page2-0" in ids and "page2-1" in ids
    offsets = [re.search(r"offset=(\d+)", c.request.url).group(1) for c in responses.calls]
    assert offsets == ["0", "4"]
    assert "amazon" not in TRUNCATED


# system -> (module, company, fixture, method, url, how the fixture reports more results than any cap reads)
CAPPED = {
    **{s: SYSTEMS[s][:5] for s in ("oracle_cloud", "jibe", "phenom", "amazon")},
    "workday": (workday, {"key": "mastercard", "workday": {"host": "mastercard.wd1.myworkdayjobs.com",
                                                           "tenant": "mastercard", "site": "CorporateCareers"}},
                "workday_mastercard_list.json", responses.POST, r"https://mastercard\.wd1\.myworkdayjobs\.com/.*"),
    "smartrecruiters": (smartrecruiters, {"key": "talabat", "smartrecruiters": {"company": "DeliveryHero"}},
                        "smartrecruiters_talabat_list.json", responses.GET, r"https://api\.smartrecruiters\.com/.*"),
}
INFLATE = {
    "oracle_cloud": lambda d: d["items"][0].update(TotalJobsCount=10_000),
    "jibe": lambda d: d.update(totalCount=10_000),
    "phenom": lambda d: d["refineSearch"].update(totalHits=10_000),
    "amazon": lambda d: d.update(hits=10_000),
    "workday": lambda d: d.update(total=10_000),
    "smartrecruiters": lambda d: d.update(totalFound=10_000),
}


@pytest.mark.parametrize("system", CAPPED)
@pytest.mark.parametrize("more", [False, True])
@responses.activate
def test_capped_extractor_reports_truncation(system, more):
    module, company, fixture_name, method, url = CAPPED[system]
    page = fixture(fixture_name)
    if more:
        INFLATE[system](page)
    responses.add(method, re.compile(url), json=page)
    assert module.fetch(company, TODAY)
    assert (company["key"] in TRUNCATED) == more


def _jobs(system, page):
    """The list of job dicts inside one fixture page."""
    return {"smartrecruiters": lambda: page["content"], "jibe": lambda: [j["data"] for j in page["jobs"]],
            "phenom": lambda: page["refineSearch"]["data"]["jobs"], "amazon": lambda: page["jobs"],
            "workday": lambda: page["jobPostings"], "eightfold": lambda: page["data"]["positions"]}[system]()


ALL = {**CAPPED, "eightfold": SYSTEMS["eightfold"][:5]}
POSTED = {"smartrecruiters": "releasedDate", "jibe": "posted_date", "phenom": "postedDate",
          "amazon": "posted_date", "workday": "postedOn"}
LOCATION = {"smartrecruiters": "location", "amazon": "location", "workday": "locationsText", "eightfold": "locations"}


@pytest.mark.parametrize("system", POSTED)
@responses.activate
def test_null_posted_date_keeps_the_posting(system):
    module, company, fixture_name, method, url = ALL[system]
    page = fixture(fixture_name)
    for j in _jobs(system, page):
        j[POSTED[system]] = None  # JSON null, not a missing key
    responses.add(method, re.compile(url), json=page)
    postings = module.fetch(company, TODAY)
    assert postings and all(p.posted_raw == "" for p in postings)


@pytest.mark.parametrize("system", LOCATION)
@responses.activate
def test_null_location_does_not_raise(system):
    module, company, fixture_name, method, url = ALL[system]
    page = fixture(fixture_name)
    for j in _jobs(system, page):
        j[LOCATION[system]] = None
    responses.add(method, re.compile(url), json=page)
    module.fetch(company, TODAY)
