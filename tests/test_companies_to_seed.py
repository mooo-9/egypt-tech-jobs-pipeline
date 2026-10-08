import csv
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import companies_to_seed  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def test_seed_matches_companies_yml():
    companies = yaml.safe_load((ROOT / "companies.yml").read_text(encoding="utf-8"))
    companies_to_seed.main()
    rows = list(csv.DictReader((ROOT / "dbt" / "seeds" / "companies.csv").read_text(encoding="utf-8").splitlines()))
    assert [r["company_key"] for r in rows] == [c["key"] for c in companies]
    assert all(r["name"] and r["industry"] and r["source_system"] for r in rows)
