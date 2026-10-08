import json
from pathlib import Path

import pytest
import responses

from pipeline.extract import ashby, greenhouse, lever, workable
from pipeline.extract.common import EXTRACTORS, in_egypt

FIXTURES = Path(__file__).parent / "fixtures" / "http"
TODAY = "2026-10-08"

# system -> (module, company, API url, fixture, expected Egypt posting_ids, first url)
CASES = {
    "greenhouse": (greenhouse, {"key": "tamara", "name": "Tamara", "greenhouse": {"board": "tamara"}},
                   "https://boards-api.greenhouse.io/v1/boards/tamara/jobs", "greenhouse_tamara_list.json",
                   ["tamara:4901328101", "tamara:4920917101"],
                   "https://job-boards.eu.greenhouse.io/tamara/jobs/4901328101"),
    "lever": (lever, {"key": "yassir", "name": "Yassir", "lever": {"site": "Yassir"}},
              "https://api.lever.co/v0/postings/Yassir", "lever_yassir_list.json",
              ["8ea76e6d-d4f7-43e0-9c99-ebc1329d0ff3", "37980bd7-7725-41a9-8b88-8f3f9513cd7b"],
              "https://jobs.lever.co/Yassir/8ea76e6d-d4f7-43e0-9c99-ebc1329d0ff3"),
    "ashby": (ashby, {"key": "thndr", "name": "Thndr", "ashby": {"board": "thndr"}},
              "https://api.ashbyhq.com/posting-api/job-board/thndr", "ashby_thndr_list.json",
              ["2cd6c3b5-fd44-450b-8c81-3ffaf10b2bf2", "26d159c0-f734-4d93-8243-0f1fe232eeb6"],
              "https://jobs.ashbyhq.com/thndr/2cd6c3b5-fd44-450b-8c81-3ffaf10b2bf2"),
    "workable": (workable, {"key": "tamatem", "name": "Tamatem", "workable": {"account": "tamatem"}},
                 "https://apply.workable.com/api/v1/widget/accounts/tamatem", "workable_tamatem_list.json",
                 ["tamatem:3A4B6846CB", "tamatem:D9140986BC"], "https://apply.workable.com/j/3A4B6846CB"),
}


@pytest.mark.parametrize("system", CASES)
@responses.activate
def test_board_returns_egypt_postings_with_clean_descriptions(system):
    module, company, api, fixture, ids, url = CASES[system]
    responses.add(responses.GET, api, json=json.loads((FIXTURES / fixture).read_text(encoding="utf-8")))
    postings = module.fetch(company, TODAY)  # the fixture holds 3 jobs, the last one outside Egypt
    assert [p.posting_id for p in postings] == ids
    assert EXTRACTORS[system] is module.fetch
    for p in postings:
        assert in_egypt(p.location, p.title)
        assert (p.source_system, p.company_key, p.collected_at) == (system, company["key"], TODAY)
        assert p.title.strip() and p.posted_raw
        assert p.description and "<" not in p.description and "&lt;" not in p.description
    assert postings[0].url == url


@responses.activate
def test_greenhouse_description_unescapes_then_strips_tags():
    job = {"id": 7, "title": "Dev", "location": {"name": "Cairo, Egypt"}, "absolute_url": "u",
           "first_published": "2026-10-01", "content": "&lt;p&gt;Build &amp;amp; ship&lt;/p&gt;"}
    responses.add(responses.GET, CASES["greenhouse"][2], json={"jobs": [job]})
    assert greenhouse.fetch(CASES["greenhouse"][1], TODAY)[0].description == "Build & ship"


@responses.activate
def test_lever_keeps_posting_with_cairo_among_many_locations_and_joins_list_sections():
    job = {"id": "abc", "text": "Dev", "hostedUrl": "u", "createdAt": 1735825140463,
           "categories": {"location": "Algiers", "allLocations": ["Algiers", "Cairo, Egypt"]},
           "description": "<div>Intro</div>", "lists": [{"text": "Skills:", "content": "<li>Python</li>"}],
           "additional": "<div>Perks</div>"}
    responses.add(responses.GET, CASES["lever"][2], json=[job])
    p = lever.fetch(CASES["lever"][1], TODAY)[0]
    assert "Cairo, Egypt" in p.location and p.posted_raw == "2025-01-02"
    assert p.description == "Intro Skills: Python Perks"


@responses.activate
def test_ashby_matches_on_address_country_when_location_names_no_city():
    job = {"id": "x", "title": "Dev", "location": "HQ Office", "secondaryLocations": [], "jobUrl": "u",
           "publishedAt": "2026-10-01", "address": {"postalAddress": {"addressCountry": "Egypt"}},
           "descriptionHtml": "<p>Hi</p>"}
    other = dict(job, id="y", address={"postalAddress": {"addressCountry": "Bulgaria"}})
    responses.add(responses.GET, CASES["ashby"][2], json={"jobs": [job, other]})
    postings = ashby.fetch(CASES["ashby"][1], TODAY)
    assert [p.posting_id for p in postings] == ["x"] and in_egypt(postings[0].location)


@responses.activate
def test_workable_location_without_city_and_missing_description_is_none():
    job = {"shortcode": "AB12", "title": "Dev", "url": "u", "published_on": "2026-10-01", "city": "",
           "country": "Egypt", "description": None}
    responses.add(responses.GET, CASES["workable"][2], json={"jobs": [job]})
    p = workable.fetch(CASES["workable"][1], TODAY)[0]
    assert p.location == "Egypt" and p.description is None


@responses.activate
def test_lever_missing_description_is_none():
    job = {"id": "abc", "text": "Dev", "hostedUrl": "u", "createdAt": None,
           "categories": {"location": "Cairo", "allLocations": None}}
    responses.add(responses.GET, CASES["lever"][2], json=[job])
    p = lever.fetch(CASES["lever"][1], TODAY)[0]
    assert p.description is None and p.posted_raw == ""
