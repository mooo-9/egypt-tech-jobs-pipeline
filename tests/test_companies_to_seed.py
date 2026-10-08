import csv
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import companies_to_seed  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def test_committed_seed_matches_companies_yml():
    """The committed seed is what companies.yml produces; regenerate it after editing companies.yml."""
    companies = yaml.safe_load((ROOT / "companies.yml").read_text(encoding="utf-8"))
    with open(ROOT / "dbt" / "seeds" / "companies.csv", newline="", encoding="utf-8") as f:
        committed = list(csv.reader(f))  # parsed rows, so CRLF vs LF does not matter
    assert committed == companies_to_seed.rows(companies)
    assert all(all(r) for r in committed)
