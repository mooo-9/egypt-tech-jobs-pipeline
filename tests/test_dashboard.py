"""Dashboard build tests. The warehouse is built by a real `dbt build` over tests/fixtures/raw into a temp
duckdb file (DBT_DUCKDB_PATH), so the tests never touch dbt/warehouse.duckdb or data/raw/."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from dashboard.build import build

ROOT = Path(__file__).resolve().parent.parent
KEYS = {"generated_at", "coverage", "skills_this_week", "skill_trend", "roles", "seniority",
        "top_companies", "health"}


@pytest.fixture(scope="session")
def warehouse(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("warehouse")
    db = tmp / "warehouse.duckdb"
    env = {**os.environ, "DBT_DUCKDB_PATH": str(db),
           "RAW_GLOB": (ROOT / "tests/fixtures/raw/*/postings.parquet").as_posix()}
    subprocess.run([sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", ".", "--target", "duckdb",
                    "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")],
                   cwd=ROOT / "dbt", env=env, check=True, capture_output=True)
    return db


def _summary(raw: Path, day: str, companies: list[dict]) -> None:
    d = raw / f"date={day}"
    d.mkdir(parents=True)
    (d / "run_summary.json").write_text(json.dumps(
        {"date": day, "started_at": f"{day}T02:00:00+00:00", "duration_s": 10, "rows": 7,
         "companies": companies}), encoding="utf-8")


def _build(warehouse, tmp_path, companies):
    raw, out = tmp_path / "raw", tmp_path / "site"
    _summary(raw, "2026-10-02", companies)
    build(warehouse, raw, out)
    return json.loads((out / "data.json").read_text(encoding="utf-8")), out


OK = [{"key": "a", "status": "ok", "rows": 5, "error": None}]


def test_build_writes_expected_keys(warehouse, tmp_path):
    data, out = _build(warehouse, tmp_path, OK)
    assert KEYS <= data.keys()
    assert {"companies", "postings_open"} <= data["coverage"].keys()
    assert set(data["skills_this_week"]) == {"all", "data", "ai_ml", "tech"}
    assert {"weeks", "series"} <= data["skill_trend"].keys()
    assert {"last_run", "rows", "sources_ok", "sources_failed"} <= data["health"].keys()
    assert data["roles"] and data["seniority"] and data["top_companies"]
    assert "<canvas" in (out / "index.html").read_text(encoding="utf-8")


def test_top_skills_limited_to_15(warehouse, tmp_path):
    data, _ = _build(warehouse, tmp_path, OK)
    for rows in data["skills_this_week"].values():
        assert len(rows) <= 15
        counts = [r["open_postings"] for r in rows]
        assert counts == sorted(counts, reverse=True)
    assert data["skills_this_week"]["tech"]


def test_health_lists_failed_sources(warehouse, tmp_path):
    data, _ = _build(warehouse, tmp_path, OK + [{"key": "broken", "status": "error", "rows": 0, "error": "boom"}])
    assert data["health"]["sources_failed"] == ["broken"]
    assert data["health"]["sources_ok"] == 1


def _script(out):
    html = (out / "index.html").read_text(encoding="utf-8")
    return html[html.index("const D = JSON.parse"):]


def test_health_and_note_render_even_without_chart_js(warehouse, tmp_path):
    """If the Chart.js CDN fails, the first new Chart would throw; health and the source note must already be filled."""
    _, out = _build(warehouse, tmp_path, OK)
    script = _script(out)
    guard = script.index("if (window.Chart) {")
    assert script.index("$('health').innerHTML") < guard
    assert script.index("$('note').textContent") < guard
    assert script.count("new Chart") == script[guard:].count("new Chart") > 0  # every chart is behind the guard


def test_footer_counts_open_of_tracked_employers(warehouse, tmp_path):
    data, out = _build(warehouse, tmp_path, OK)
    tracked = len(yaml.safe_load((ROOT / "companies.yml").read_text(encoding="utf-8")))
    assert data["coverage"]["companies_tracked"] == tracked  # from the companies seed in the warehouse
    assert 0 < data["coverage"]["companies"] <= tracked
    assert "D.coverage.companies + ' of ' + D.coverage.companies_tracked + ' employers" in _script(out)
