"""Build the static dashboard: marts + latest run summary -> site/data.json and a self-contained site/index.html."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

TEMPLATE = Path(__file__).with_name("template.html")
SCOPES = ("all", "data", "ai_ml", "tech")


def _rows(con, sql: str, *params) -> list[dict]:
    cur = con.execute(sql, list(params))
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _health(raw: Path) -> dict:
    summaries = sorted(raw.glob("date=*/run_summary.json"))
    if not summaries:
        return {"last_run": None, "rows": 0, "sources_ok": 0, "sources_empty": [], "sources_failed": []}
    s = json.loads(summaries[-1].read_text(encoding="utf-8"))
    by = lambda status: [c["key"] for c in s["companies"] if c["status"] == status]  # noqa: E731
    return {"last_run": s["started_at"], "rows": s["rows"], "sources_ok": len(by("ok")),
            "sources_empty": by("empty"), "sources_failed": by("error")}


def build(db: Path, raw: Path, out: Path) -> dict:
    con = duckdb.connect(str(db), read_only=True)
    try:
        week = con.execute("select max(week_start) from mart_skill_demand_weekly").fetchone()[0]
        skills = {s: _rows(con, """
            select skill, category, open_postings, share from mart_skill_demand_weekly
            where week_start = ? and role_scope = ?
            order by open_postings desc, skill limit 15""", week, s) for s in SCOPES}

        # trend follows the default filter (tech): the 10 skills most wanted in the latest week
        top10 = [r["skill"] for r in skills["tech"][:10]]
        weeks = [str(r[0]) for r in con.execute(
            "select distinct week_start from mart_skill_demand_weekly order by 1").fetchall()]
        series = {k: [0] * len(weeks) for k in top10}
        for r in _rows(con, """
                select week_start, skill, open_postings from mart_skill_demand_weekly
                where role_scope = 'tech' and skill in (select unnest(?))""", top10):
            series[r["skill"]][weeks.index(str(r["week_start"]))] = r["open_postings"]

        group = lambda col: _rows(con, f"""
            select {col}, count(*) as open_postings from fct_postings
            where is_open group by {col} order by open_postings desc, {col}""")  # noqa: E731
        roles, seniority = group("role_family"), group("seniority")
        top_companies = _rows(con, """
            select c.name, count(*) as open_postings from fct_postings f
            join dim_company c using (company_key) where f.is_open
            group by c.name order by open_postings desc, c.name limit 10""")
        postings_open, companies = con.execute(
            "select count(*), count(distinct company_key) from fct_postings where is_open").fetchone()
        tracked = con.execute("select count(*) from dim_company").fetchone()[0]
    finally:
        con.close()

    data = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "coverage": {"companies": companies, "companies_tracked": tracked, "postings_open": postings_open},
            "skills_week_start": str(week), "skills_this_week": skills,
            "skill_trend": {"weeks": weeks, "series": series},
            "roles": roles, "seniority": seniority,
            "top_companies": top_companies, "health": _health(raw)}
    out.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=2)
    (out / "data.json").write_text(text, encoding="utf-8")
    # inlined too, so index.html works when opened from disk (fetch() of a local file is blocked)
    inline = json.dumps(data).replace("</", r"<\/")
    (out / "index.html").write_text(TEMPLATE.read_text(encoding="utf-8").replace("__DATA__", inline),
                                    encoding="utf-8")
    return data


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", type=Path, default=Path("dbt/warehouse.duckdb"))
    p.add_argument("--raw", type=Path, default=Path("data/raw"))
    p.add_argument("--out", type=Path, default=Path("site"))
    a = p.parse_args(argv)
    build(a.db, a.raw, a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
