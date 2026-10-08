import json
from dataclasses import fields

import pyarrow.parquet as pq
import pytest
import yaml

from pipeline import run as run_mod
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, Posting


def posting(pid, company="a", description=None, day="2026-01-02", system="fake"):
    return Posting(system, company, pid, f"t{pid}", "Cairo", "today", f"http://x/{pid}", description, day)


@pytest.fixture
def companies(tmp_path):
    def make(*keys):
        path = tmp_path / "companies.yml"
        path.write_text(yaml.safe_dump([{"key": k, "name": k, "industry": "x", "fake": {}} for k in keys]))
        return path
    return make


@pytest.fixture
def fake(monkeypatch):
    """Install a fake extractor: rows(company, day) -> list[Posting]."""
    def install(rows):
        monkeypatch.setitem(EXTRACTORS, "fake", rows)
    return install


def read(raw_dir, day):
    return pq.read_table(raw_dir / f"date={day}" / "postings.parquet")


def test_run_writes_parquet_with_schema(tmp_path, companies, fake):
    fake(lambda c, day: [posting("1", c["key"], day=day)])
    raw = tmp_path / "raw"
    summary = run_mod.run("2026-01-02", companies("a"), raw)
    path = raw / "date=2026-01-02" / "postings.parquet"
    assert pq.read_table(path).column_names == [f.name for f in fields(Posting)]
    assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"
    assert summary["rows"] == 1
    assert json.loads((raw / "date=2026-01-02" / "run_summary.json").read_text()) == summary


def test_run_continues_when_one_company_fails(tmp_path, companies, fake):
    def rows(c, day):
        if c["key"] == "bad":
            raise RuntimeError("boom")
        return [posting(c["key"], c["key"], day=day)]
    fake(rows)
    summary = run_mod.run("2026-01-02", companies("a", "bad", "c"), tmp_path / "raw")
    by_key = {c["key"]: c for c in summary["companies"]}
    assert by_key["bad"]["status"] == "error" and by_key["bad"]["error"] == "RuntimeError: boom"
    assert by_key["a"]["status"] == by_key["c"]["status"] == "ok"
    assert read(tmp_path / "raw", "2026-01-02").num_rows == 2


def test_run_fails_when_all_companies_empty(tmp_path, companies, fake):
    fake(lambda c, day: [])
    raw = tmp_path / "raw"
    code = run_mod.main(["--date", "2026-01-02", "--companies", str(companies("a")), "--raw-dir", str(raw)])
    assert code == 1
    assert json.loads((raw / "date=2026-01-02" / "run_summary.json").read_text())["companies"][0]["status"] == "empty"
    assert read(raw, "2026-01-02").num_rows == 0  # schema stays stable on an empty day


def test_duplicate_postings_written_once(tmp_path, companies, fake):
    fake(lambda c, day: [posting("1", day=day), posting("1", day=day)])
    summary = run_mod.run("2026-01-02", companies("a"), tmp_path / "raw")
    assert read(tmp_path / "raw", "2026-01-02").num_rows == 1
    assert summary["rows"] == 1


def test_description_carried_forward_not_refetched(tmp_path, companies, fake, monkeypatch):
    raw = tmp_path / "raw"
    fake(lambda c, day: [posting("X", day=day, description="old text")])
    run_mod.run("2026-01-01", companies("a"), raw)

    calls = []
    def describe(company, p):
        calls.append(p.posting_id)
        return "new text"
    monkeypatch.setitem(DESCRIBERS, "fake", describe)
    fake(lambda c, day: [posting("X", day=day), posting("Y", day=day)])
    run_mod.run("2026-01-02", companies("a"), raw)

    got = {r["posting_id"]: r["description"] for r in read(raw, "2026-01-02").to_pylist()}
    assert got == {"X": "old text", "Y": "new text"}
    assert calls == ["Y"]


def test_describer_failure_keeps_company_and_is_counted(tmp_path, companies, fake, monkeypatch):
    def describe(company, p):
        raise RuntimeError("nope")
    monkeypatch.setitem(DESCRIBERS, "fake", describe)
    fake(lambda c, day: [posting("1", day=day)])
    summary = run_mod.run("2026-01-02", companies("a"), tmp_path / "raw")
    assert summary["companies"][0]["status"] == "ok"
    assert summary["companies"][0]["describe_errors"] == 1
    assert read(tmp_path / "raw", "2026-01-02").to_pylist()[0]["description"] is None


def test_rerun_same_day_overwrites_only_that_day(tmp_path, companies, fake):
    raw = tmp_path / "raw"
    fake(lambda c, day: [posting("1", day=day)])
    run_mod.run("2026-01-01", companies("a"), raw)
    before = (raw / "date=2026-01-01" / "postings.parquet").read_bytes()
    fake(lambda c, day: [posting("1", day=day), posting("2", day=day)])
    run_mod.run("2026-01-02", companies("a"), raw)
    fake(lambda c, day: [posting("3", day=day)])
    run_mod.run("2026-01-02", companies("a"), raw)
    assert [r["posting_id"] for r in read(raw, "2026-01-02").to_pylist()] == ["3"]
    assert (raw / "date=2026-01-01" / "postings.parquet").read_bytes() == before
