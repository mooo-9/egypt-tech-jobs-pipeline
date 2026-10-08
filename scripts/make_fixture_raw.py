"""Write the two hand-made raw days under tests/fixtures/raw/ that CI builds dbt against.
Company keys are real keys from companies.yml (the dim_company relationships test needs them). Covers every posted_raw format, city variants, an empty description, and postings seen on both days."""
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.run import SCHEMA  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "raw"


def row(system, company, pid, title, location, posted, desc, day):
    return dict(source_system=system, company_key=company, posting_id=pid, title=title, location=location,
                posted_raw=posted, url=f"https://example.com/{company}/{pid}", description=desc, collected_at=day)


def day_rows(day):
    rows = [
        row("workday", "valeo", "w1", "  Data Engineer ", "Cairo, Egypt", "Posted Today", "Build pipelines with Python and SQL.", day),
        row("workday", "valeo", "w2", "Data Analyst", "Giza, Egypt", "Posted 3 Days Ago", "  ", day),
        row("greenhouse", "tamara", "g1", "ML Engineer", "Cairo, Egypt", "2026-09-28T10:15:00+03:00", "Train models.", day),
        row("lever", "yassir", "l1", "Backend Developer", "Alexandria; Cairo; Dubai", "2026-09-20", None, day),
        row("amazon", "amazon", "a1", "Software Engineer", "Maadi, Egypt", "October  1, 2026", "Java services.", day),
        row("workable", "foodics", "k1", "Intern", "Hurghada, Egypt", "", "Support the team.", day),
    ]
    if day == "2026-10-02":
        rows.append(row("ashby", "thndr", "h1", "Data Scientist", "New Cairo, Egypt", "2026-10-01T08:00:00.000Z", "Analyse data.", day))
    return rows


for day in ("2026-10-01", "2026-10-02"):
    out = OUT / f"date={day}"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(day_rows(day), schema=SCHEMA), out / "postings.parquet", compression="zstd")
    print("wrote", out / "postings.parquet")
