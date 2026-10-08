"""Turn companies.yml into the dbt seed dbt/seeds/companies.csv (company_key, name, industry, source_system)."""
import csv
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
META = {"key", "name", "industry"}


def main():
    companies = yaml.safe_load((ROOT / "companies.yml").read_text(encoding="utf-8"))
    with open(ROOT / "dbt" / "seeds" / "companies.csv", "w", newline="", encoding="utf-8") as f:
        out = csv.writer(f, lineterminator="\n")
        out.writerow(["company_key", "name", "industry", "source_system"])
        for c in companies:
            (system,) = set(c) - META
            out.writerow([c["key"], c["name"], c["industry"], system])


if __name__ == "__main__":
    main()
