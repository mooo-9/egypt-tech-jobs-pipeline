"""Spark vs dbt parity: spark.skill_demand rebuilds mart_skill_demand_weekly (role_scope 'all') from the raw
Parquet; a real `dbt build` over the same files is the reference. Skipped when pyspark is not installed
(the main CI job installs requirements.txt only; the spark-parity job installs requirements-spark.txt too)."""
import csv
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("pyspark")
from pyspark.sql import functions as F  # noqa: E402

from pipeline.run import SCHEMA  # noqa: E402
from spark.skill_demand import skill_demand_weekly, to_java_regex  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SKILLS = ROOT / "dbt/seeds/skills.csv"


def _row(pid, title, desc, day, system="workday", company="valeo"):
    return dict(source_system=system, company_key=company, posting_id=pid, title=title, location="Cairo, Egypt",
                posted_raw="", url=f"https://example.com/{company}/{pid}", description=desc, collected_at=day)


# Two weeks (Mon 2026-09-28 and Mon 2026-10-05), a Sunday/Monday boundary, a posting seen on several days
# of one week (counts once), a description stored only on the first day, a blank description, a title
# change, and the same posting_id in two source systems.
FIXTURE_DAYS = {
    "2026-09-27": [_row("a", "Python Developer", "We use Docker and Kafka.", "2026-09-27")],  # Sunday
    "2026-09-28": [
        _row("a", "Python Developer", None, "2026-09-28"),  # description known only on the first day
        _row("b", "  Data Engineer ", "  Airflow, dbt and SQL.  ", "2026-09-28"),
        _row("c", "Analyst", "   ", "2026-09-28"),  # blank description: title only
        _row("a", "Marketing Lead", "Go-to-market, not the language Go.", "2026-09-28", "greenhouse", "tamara"),
    ],
    "2026-09-30": [
        _row("a", "Python Developer", None, "2026-09-30"),
        _row("b", "Data Engineer", None, "2026-09-30"),
        _row("d", "Backend Engineer", "Java and Postgres; also AWS", "2026-09-30"),
    ],
    "2026-10-05": [  # Monday: new week
        _row("a", "Senior Python Developer", None, "2026-10-05"),
        _row("d", "Backend Engineer", None, "2026-10-05"),
        _row("e", "BI Developer", "Power BI,\nTableau\tand Excel.", "2026-10-05"),
        _row("f", "ML Engineer", "PyTorch and scikit-learn for LLMs. R, Python", "2026-10-05"),
    ],
}


def _write_fixture(root: Path) -> str:
    for day, rows in FIXTURE_DAYS.items():
        d = root / f"date={day}"
        d.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), d / "postings.parquet")
    return (root / "*/postings.parquet").as_posix()


def _dbt_mart(raw_glob: str, tmp: Path):
    db = tmp / "warehouse.duckdb"
    env = {**os.environ, "DBT_DUCKDB_PATH": str(db), "RAW_GLOB": raw_glob}
    subprocess.run([sys.executable, "-m", "dbt.cli.main", "build", "--profiles-dir", ".", "--target", "duckdb",
                    "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")],
                   cwd=ROOT / "dbt", env=env, check=True, capture_output=True)
    con = duckdb.connect(str(db), read_only=True)
    try:
        return con.execute("select week_start, skill, category, open_postings, share from mart_skill_demand_weekly "
                           "where role_scope = 'all' order by 1, 2").fetchall()
    finally:
        con.close()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("parity").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="session", params=["custom", "shared"])
def raw_glob(request, tmp_path_factory):
    if request.param == "shared":
        return (ROOT / "tests/fixtures/raw/*/postings.parquet").as_posix()
    return _write_fixture(tmp_path_factory.mktemp("raw"))


def test_spark_matches_dbt(spark, raw_glob, tmp_path_factory):
    expected = _dbt_mart(raw_glob, tmp_path_factory.mktemp("dbt"))
    got = sorted(tuple(r) for r in skill_demand_weekly(spark, raw_glob, str(SKILLS)).collect())
    assert expected, "fixture produced no mart rows"
    assert [r[:4] for r in got] == [r[:4] for r in expected]
    assert [r[4] for r in got] == pytest.approx([r[4] for r in expected], abs=1e-9)


def test_description_only_on_first_day_still_matches(spark, tmp_path):
    rows = skill_demand_weekly(spark, _write_fixture(tmp_path), str(SKILLS)).collect()
    docker = {r.week_start.isoformat(): r.open_postings for r in rows if r.skill == "Docker"}
    # posting "workday:a" has Docker only in its 2026-09-27 description; its later rows are null
    assert docker == {"2026-09-21": 1, "2026-09-28": 1, "2026-10-05": 1}


def test_output_types(spark, tmp_path):
    df = skill_demand_weekly(spark, _write_fixture(tmp_path), str(SKILLS))
    assert {f.name: f.dataType.simpleString() for f in df.schema.fields} == {
        "week_start": "date", "skill": "string", "category": "string", "open_postings": "bigint", "share": "double"}


def test_to_java_regex_matches_re2_semantics():
    java = to_java_regex(r"\bgo\s*([,;]|$)")
    assert java.startswith("(?iu)")
    assert r"\s" not in java.replace(r"[ \t\n\f\r]", "")  # Java \s also matches \x0B; RE2 \s does not
    assert java.endswith(r"\z)")  # Java $ also matches before a final newline; RE2 $ does not
    assert to_java_regex(r"a\$b") == r"(?iu)a\$b"  # escaped dollar stays literal


# Strings chosen to separate Java from RE2: vertical tab (Java \s only), Unicode line terminators and a final
# newline (Java $ only), non-ASCII neighbours of \b, non-ASCII case folding, and each skill's punctuation contexts.
VT, LS, NEL, LF, CR = (chr(c) for c in (0x0B, 0x2028, 0x85, 0x0A, 0x0D))
KELVIN, LONG_S, DOTTED_I, DOTLESS_I = (chr(c) for c in (0x212A, 0x17F, 0x130, 0x131))
TRICKY = ["Python", "PYTHON3 developer", "pythonic", "pythoné", "épython", "node.js and NodeJS", "nodeXjs",
          "go, python", "Go-to-market", "Google", "we use go," + VT + "daily", "x and go" + LS, "x or go" + NEL,
          "x or go" + LF, "x or go" + CR, "x, " + VT + "go" + VT + ",", "R, Python", "r studio", "SQL" + VT,
          "sql" + LS, "K8S", "kubernetes", LONG_S + "ql", KELVIN + "8s", DOTTED_I + " git", "git" + DOTLESS_I,
          "Power  BI", "powerbi", "Power" + VT + "BI", "advanced Excel", "excel" + VT + "vba", "line1" + LF + "git",
          "spark" + NEL, "spark" + LF, "SPARK ", "(spark)", ",spark.", "x and spark", "scikit learn",
          "Large Language Models", "py torch", "tensor flow", "google cloud platform", "amazon web services"]


def test_every_skill_pattern_agrees_with_duckdb_on_tricky_text(spark):
    with open(SKILLS, newline="", encoding="utf-8") as f:
        patterns = [r["pattern"] for r in csv.DictReader(f)]
    con = duckdb.connect()
    expected = {(t, p): con.execute("select regexp_matches(?, ?, 'i')", [t, p]).fetchone()[0]
                for t in TRICKY for p in patterns}
    texts = spark.range(1).select(F.explode(F.array(*[F.lit(t) for t in TRICKY])).alias("t"))
    rows = texts.select("t", *[F.col("t").rlike(to_java_regex(p)).alias(f"p{i}") for i, p in enumerate(patterns)]).collect()
    got = {(r.t, p): r[f"p{i}"] for r in rows for i, p in enumerate(patterns)}
    assert got == expected
    assert sum(expected.values()) > 20  # the strings do exercise matches

