"""Turn companies.yml into the dbt seed dbt/seeds/companies.csv (company_key, name, industry, source_system)."""
import csv
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
META = {"key", "name", "industry"}


def rows(companies: list[dict]) -> list[list[str]]:
    """The seed's rows, header first."""
    out = [["company_key", "name", "industry", "source_system"]]
    for c in companies:
        (system,) = set(c) - META
        out.append([c["key"], c["name"], c["industry"], system])
    return out


def main():
    companies = yaml.safe_load((ROOT / "companies.yml").read_text(encoding="utf-8"))
    with open(ROOT / "dbt" / "seeds" / "companies.csv", "w", newline="", encoding="utf-8") as f:
        csv.writer(f, lineterminator="\n").writerows(rows(companies))


if __name__ == "__main__":
    main()
