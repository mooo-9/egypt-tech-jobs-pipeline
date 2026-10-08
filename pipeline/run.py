"""Daily run: every extractor in turn, one company's failure never stops the rest."""
import argparse
import json
import os
import sys
import time
from dataclasses import astuple, fields, replace
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from pipeline.extract import load_all
from pipeline.extract.common import DESCRIBERS, EXTRACTORS, Posting

ROOT = Path(__file__).resolve().parent.parent
SCHEMA = pa.schema([(f.name, pa.string()) for f in fields(Posting)])  # description is nullable by default


def _known_descriptions(raw_dir: Path, day: str) -> dict[tuple[str, str], str]:
    """Descriptions already collected on earlier days; the latest day wins."""
    known = {}
    for path in sorted(raw_dir.glob("date=*/postings.parquet")):
        if path.parent.name.removeprefix("date=") >= day:
            continue
        table = pq.read_table(path, columns=["source_system", "posting_id", "description"])
        for row in table.to_pylist():
            if row["description"] is not None:
                known[(row["source_system"], row["posting_id"])] = row["description"]
    return known


def _collect(company: dict, day: str, known: dict, seen: set) -> tuple[list[Posting], int]:
    """One company's new rows plus the count of failed description fetches."""
    system = next(k for k in company if k in EXTRACTORS)
    rows, describe_errors = [], 0
    for p in EXTRACTORS[system](company, day):
        key = (p.source_system, p.posting_id)
        if key in seen:
            continue
        seen.add(key)
        if p.description is None and key in known:
            p = replace(p, description=known[key])
        elif p.description is None and system in DESCRIBERS:
            try:
                p = replace(p, description=DESCRIBERS[system](company, p))
            except Exception:
                describe_errors += 1  # keep the posting; the next run retries it
        rows.append(p)
    return rows, describe_errors


def _write_atomic(path: Path, write) -> None:
    tmp = path.with_name(path.name + ".tmp")
    write(tmp)
    os.replace(tmp, path)


def run(day: str, companies_path: Path, raw_dir: Path) -> dict:
    load_all()
    companies = yaml.safe_load(Path(companies_path).read_text(encoding="utf-8"))
    started = datetime.now(timezone.utc)
    t0 = time.monotonic()
    known = _known_descriptions(Path(raw_dir), day)
    seen: set = set()
    postings: list[Posting] = []
    results = []
    for company in companies:
        t = time.monotonic()
        entry = {"key": company["key"], "status": "ok", "rows": 0, "error": None, "describe_errors": 0}
        try:
            rows, entry["describe_errors"] = _collect(company, day, known, seen)
            entry["rows"] = len(rows)
            entry["status"] = "ok" if rows else "empty"
            postings += rows
        except Exception as e:
            entry["status"], entry["error"] = "error", f"{type(e).__name__}: {e}"
        entry["duration_s"] = round(time.monotonic() - t, 2)
        results.append(entry)

    summary = {
        "date": day,
        "started_at": started.isoformat(),
        "duration_s": round(time.monotonic() - t0, 2),
        "rows": len(postings),
        "companies": results,
    }
    out = Path(raw_dir) / f"date={day}"
    out.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([dict(zip(SCHEMA.names, astuple(p))) for p in postings], schema=SCHEMA)
    _write_atomic(out / "postings.parquet", lambda p: pq.write_table(table, p, compression="zstd"))
    _write_atomic(out / "run_summary.json", lambda p: p.write_text(json.dumps(summary, indent=2), encoding="utf-8"))
    return summary


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(timezone.utc).date().isoformat())
    ap.add_argument("--companies", type=Path, default=ROOT / "companies.yml")
    ap.add_argument("--raw-dir", type=Path, default=ROOT / "data" / "raw")
    args = ap.parse_args(argv)
    summary = run(args.date, args.companies, args.raw_dir)
    counts = {s: sum(c["status"] == s for c in summary["companies"]) for s in ("ok", "empty", "error")}
    print(f"{summary['date']}: {summary['rows']} rows in {summary['duration_s']}s; {counts}")
    return 0 if summary["rows"] else 1


if __name__ == "__main__":
    sys.exit(main())
