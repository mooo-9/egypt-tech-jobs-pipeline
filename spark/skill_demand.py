"""mart_skill_demand_weekly (role_scope 'all') rebuilt with PySpark straight from the raw Parquet files.

dbt stays the production path; this is an independent second implementation, and tests/test_spark_parity.py
checks on every CI run that both produce the same rows. It mirrors the dbt chain
stg_postings -> int_posting_skills -> mart_skill_demand_weekly.

    python -m spark.skill_demand --raw "data/raw/*/postings.parquet" --skills dbt/seeds/skills.csv --out out.parquet
"""
import argparse
import csv
import glob
import re
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def to_java_regex(pattern: str) -> str:
    r"""Make a DuckDB (RE2) skills pattern mean the same thing in Java.

    DuckDB runs it with the 'i' flag, so it is case-insensitive with Unicode case folding: (?iu).
    RE2 `$` matches only at the very end of the text and `\s` is [\t\n\f\r ]; in Java `$` also matches before
    a final line terminator and `\s` also matches \x0B. `\b`, `^`, `|`, groups and `\.` already agree
    (Java 19+ makes `\b` ASCII-only like RE2; the CI job uses Java 21).
    """
    swap = {r"\s": r"[ \t\n\f\r]", "$": r"\z"}
    return "(?iu)" + re.sub(r"\\.|\$", lambda m: swap.get(m.group(0), m.group(0)), pattern)


def skill_demand_weekly(spark: SparkSession, raw_glob: str, skills_csv: str) -> DataFrame:
    """week_start, skill, category, open_postings, share for role_scope 'all'."""
    # The skills seed is read with csv, not Spark's CSV reader (its escape character is a backslash), and
    # built from literals, so no Python worker process is needed.
    with open(skills_csv, newline="", encoding="utf-8") as f:
        rows = [F.struct(F.lit(r["skill"]).alias("skill"), F.lit(r["category"]).alias("category"),
                         F.lit(to_java_regex(r["pattern"])).alias("pattern")) for r in csv.DictReader(f)]
    skills = spark.range(1).select(F.explode(F.array(*rows)).alias("s")).select("s.*")

    # The glob is expanded here, not by Spark: Hadoop's own globbing crashes on Windows without winutils.
    files = sorted(Path(f).resolve().as_posix() for f in glob.glob(raw_glob))
    if not files:
        raise FileNotFoundError(f"no raw files match {raw_glob}")
    stg = spark.read.parquet(*files).select(
        F.concat("source_system", F.lit(":"), "posting_id").alias("posting_key"),
        F.trim("title").alias("title"),
        F.when(F.trim("description") != "", F.trim("description")).alias("description"),
        F.col("collected_at").cast("date").alias("collected_date"))

    # Each posting's latest title, and its latest non-null description (stored only on the first day it is
    # known, so later rows are null).
    latest = stg.groupBy("posting_key").agg(F.max_by("title", "collected_date").alias("title"))
    descriptions = (stg.where(F.col("description").isNotNull()).groupBy("posting_key")
                    .agg(F.max_by("description", "collected_date").alias("description")))
    posting_skills = (
        latest.join(descriptions, "posting_key", "left")
        .withColumn("text", F.concat("title", F.lit(" "), F.coalesce("description", F.lit(""))))
        .crossJoin(F.broadcast(skills))
        .where(F.expr("text rlike pattern"))
        .select("posting_key", "skill", "category").distinct())

    # A posting is open in a week when it was collected on at least one day of it (Monday start).
    open_in_week = stg.select(F.trunc("collected_date", "week").alias("week_start"), "posting_key").distinct()
    totals = open_in_week.groupBy("week_start").agg(F.count("*").alias("total"))
    counts = (open_in_week.join(posting_skills, "posting_key")
              .groupBy("week_start", "skill", "category").agg(F.count("*").alias("open_postings")))
    return (counts.join(totals, "week_start")
            .select("week_start", "skill", "category", "open_postings",
                    (F.col("open_postings") / F.col("total")).cast("double").alias("share")))


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--raw", required=True, help="glob of raw postings Parquet files")
    ap.add_argument("--skills", required=True, help="dbt/seeds/skills.csv")
    ap.add_argument("--out", required=True, help="Parquet file to write")
    args = ap.parse_args(argv)
    spark = (SparkSession.builder.master("local[*]").appName("skill_demand_weekly")
             .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    try:
        # Collected through Arrow and written with pyarrow: Spark's own Parquet writer needs winutils on Windows.
        table = skill_demand_weekly(spark, args.raw, args.skills).orderBy("week_start", "skill").toArrow()
    finally:
        spark.stop()
    pq.write_table(table, args.out)
    print(f"wrote {table.num_rows} rows to {args.out}")


if __name__ == "__main__":
    main()
