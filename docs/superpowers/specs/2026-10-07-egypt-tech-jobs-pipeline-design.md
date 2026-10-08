# Egypt Tech Jobs Pipeline: Design

Date: 2026-10-07
Status: Draft for review

## 1. Purpose

A public data engineering portfolio project that shows real, verifiable skill in the tools a data engineering CV claims: Python, SQL, dbt, Airflow, Docker, Postgres, Parquet, CI/CD and data testing.

It collects job postings from employers hiring in Egypt every day, models them in a warehouse, and publishes a dashboard of which skills and roles are in demand. The dashboard and repo are linked from the CV.

### Success criteria

- A public GitHub repo, `mooo-9/egypt-tech-jobs-pipeline`, with a README that explains the architecture, a diagram, and screenshots.
- A pipeline that runs every day on GitHub Actions without anyone's PC being on, and has run for at least a week before it goes on the CV.
- A public dashboard on GitHub Pages, linked from the CV.
- `dbt build` passes, including data tests, on every daily run.
- `docker compose up` runs the same pipeline under Airflow against Postgres on a local machine.
- Cost: $0.

## 2. Scope

### In scope

- **Company career APIs:** the public job-search endpoints behind employers' own careers pages. The starting set is the 18 employers already configured in the voice assistant project: Workday (10), Oracle Cloud (2), Phenom (2), Eightfold (1), Jibe (1), SmartRecruiters (1) and amazon.jobs (1), including PwC, Dell, Oracle, Mastercard, Visa, Ericsson, Valeo, Talabat, BCG and Amazon.
- **Public job-board APIs of Egyptian tech companies:** Greenhouse, Lever, Ashby and Workable all publish documented, unauthenticated job APIs. A company is added only after one live call returns at least one posting located in Egypt.
- Companies are listed in `companies.yml`. Adding a company is a config change, not a code change.

### Out of scope

- LinkedIn and Wuzzuf. LinkedIn's terms forbid scraping, and it blocks cloud IPs. Wuzzuf sits behind a Cloudflare bot check that a cloud runner cannot pass reliably.
- Salary data (rarely present in these APIs).
- LLM-based extraction (cost, and results are not deterministic).
- Spark and Kafka (the data is a few hundred rows a day; the README explains how the design would scale).

## 3. Architecture

```
companies.yml
     │
     ▼
extract (Python)  ──►  data/raw/date=YYYY-MM-DD/postings.parquet   (append-only)
                                   │
                 ┌─────────────────┴──────────────────┐
                 ▼                                    ▼
     GitHub Actions (daily)                  Airflow (local, Docker)
     DuckDB view over Parquet                load Parquet → Postgres raw.postings
                 │                                    │
                 └──────────── dbt build ─────────────┘
                     (same models, two targets)
                                   │
                                   ▼
              dashboard/build.py → JSON + HTML → GitHub Pages
```

## 4. Extraction

- One module per job system in `pipeline/extract/`, each exposing `fetch(company: dict) -> list[Posting]`.
- The logic for Workday, SmartRecruiters, Oracle Cloud, Eightfold, Jibe, Phenom and amazon.jobs is ported from the voice assistant's tested collectors. Greenhouse, Lever, Ashby and Workable are new modules.
- Only postings located in Egypt are kept. The Egypt location filter is shared by all modules.
- `Posting` fields: `source_system`, `company_key`, `posting_id` (the system's own ID), `title`, `location`, `posted_raw`, `url`, `description` (nullable), `collected_at`.
- **Descriptions are fetched only for postings not seen before.** Postings already seen are matched on `(source_system, posting_id)` against earlier raw files. A description is stored once, in the row for the first day it is known; later rows carry null, and dbt reads each posting's latest stored description.
- **Politeness:** at most 1 request per second per host, a 20-second timeout, and 3 retries with backoff. A descriptive User-Agent names the repo.
- **Isolation:** each company runs in its own try/except. One failing company is recorded in the run summary and never stops the run.
- **Output:** `data/raw/date=YYYY-MM-DD/postings.parquet` (zstd-compressed) and `data/raw/date=YYYY-MM-DD/run_summary.json` (per company: status, rows, error message, duration). Re-running the same day overwrites only that day's files.

## 5. Warehouse and dbt

The warehouse is rebuilt fully from the raw Parquet files on every run. At this size the rebuild takes seconds, it is idempotent, and it keeps no state between runs.

### Source

`raw.postings` has one row per posting per day it was seen.

- **DuckDB target:** a source defined over `read_parquet('data/raw/**/postings.parquet', hive_partitioning = true)`.
- **Postgres target:** a table that the Airflow load task fills from the same Parquet files.

### Models

| Layer | Model | Grain | Notes |
|---|---|---|---|
| staging | `stg_postings` | posting × day | Casts types, trims text, standardizes the city, and turns relative dates ("Posted 3 Days Ago") into real dates using `collected_at` |
| intermediate | `int_posting_lifecycle` | posting | `first_seen`, `last_seen`, `is_open` (seen on the latest day), `days_open` |
| intermediate | `int_posting_skills` | posting × skill | Matches description and title against the `skills` seed (`skill`, `category`, `pattern`) |
| intermediate | `int_posting_roles` | posting | `role_family` (data_engineering, data_analysis, ai_ml, software, other) and `seniority` (intern, junior, mid, senior), from the `title_rules` seed |
| marts | `dim_company` | company | Name, industry, source system |
| marts | `fct_postings` | posting | Company, role, seniority, city, lifecycle dates, open flag |
| marts | `fct_posting_skills` | posting × skill | Bridge table |
| marts | `mart_skill_demand_weekly` | week × skill | Open postings mentioning the skill, and their share of all open postings |
| marts | `mart_role_demand_daily` | day × role family | Open postings and new postings |

### Cross-database SQL

Regex matching differs between DuckDB and Postgres. A macro, `regex_match(column, pattern)`, uses `adapter.dispatch` to pick the right syntax. No other model relies on database-specific SQL.

### Data tests

- `unique` and `not_null` on every model's key.
- `accepted_values` on `role_family` and `seniority`.
- `relationships` from both facts to `dim_company`.
- Source freshness on `raw.postings.collected_at`: warn after 1 day, error after 2. This runs as its own `dbt source freshness` step, because `dbt build` does not check freshness.
- Singular tests: no posting dated in the future; every skill share is between 0 and 1; the latest run loaded at least one posting.

## 6. Orchestration

### GitHub Actions: `daily.yml`

- Runs every day at 04:00 UTC (06:00 or 07:00 in Cairo, depending on daylight saving), and can also be started by hand.
- Steps: set up Python 3.12 → install the pinned requirements → `python -m pipeline.run` → commit the new `data/raw/date=…/` files as `data: YYYY-MM-DD` → `dbt source freshness --target duckdb` → `dbt build --target duckdb` → `python -m dashboard.build` → deploy `site/` to GitHub Pages.
- If extraction or `dbt build` fails, the job fails, the dashboard is not updated, GitHub emails the owner, and the previous dashboard stays live. Raw data is committed before `dbt build`, so it is kept when only dbt fails.

### GitHub Actions: `ci.yml`

Runs on pull requests: `pytest`, then `dbt build --target duckdb` against the fixed sample data in `tests/fixtures/raw/`. It makes no network calls.

### Local Airflow: `airflow/docker-compose.yml`

- Services: the official Airflow image (LocalExecutor) and Postgres 16, which holds both the Airflow metadata and the warehouse database.
- DAG `egypt_tech_jobs_daily`: `extract` → `load_to_postgres` → `dbt_build` (`dbt build --target postgres`). It runs daily, with `catchup=False`.
- It uses the same `pipeline` package and the same dbt project, mounted into the containers.
- It requires Docker Desktop, which the owner installs. This is the last stage of the build; everything else works without it.

## 7. Dashboard

- `dashboard/build.py` reads the marts and writes `site/data.json`. `dashboard/template.html` (Chart.js from a CDN) renders it. No server is needed.
- Panels:
  - The most-wanted skills this week, with a role filter: all, data, AI/ML.
  - Weekly demand for the top 10 skills.
  - Open postings by role family and by seniority.
  - The top hiring companies.
  - Pipeline health: time of the last successful run, postings collected, and which sources succeeded or failed. A failed test stops publishing, so a stale "last successful run" time is how visitors see a failure.
- A note at the bottom states the data source: public career APIs of N employers hiring in Egypt, not the whole market.
- Styled to match the HR Bias Detection demo, and usable on a phone.

## 8. Testing

- **Extractors:** each module is tested against saved real API responses in `tests/fixtures/http/`. Tests never call the network.
- **Classification:** table-driven tests for role and seniority rules on tricky titles, such as "Data Engineer Intern", "Senior BI Analyst", "ML Ops Engineer" and "Software Engineer, Data Platform".
- **Skill patterns:** each seed pattern has a test for what it must match and what it must not, for example "R" must not match every capital R, and "Go" must not match "Google".
- **dbt:** the data tests in Section 5, run against the fixture data in CI.
- **End to end:** one pytest run calls `pipeline.run` with mocked HTTP into a temporary folder, then runs `dbt build` on the result.

## 9. Repository layout

```
pipeline/
  extract/          workday.py, smartrecruiters.py, oracle_cloud.py, eightfold.py,
                    jibe.py, phenom.py, amazon.py, greenhouse.py, lever.py,
                    ashby.py, workable.py, common.py
  run.py            reads companies.yml, runs every extractor, writes Parquet and the run summary
  load.py           Parquet → Postgres (used by Airflow)
companies.yml
dbt/
  models/{staging,intermediate,marts}/
  seeds/skills.csv, seeds/title_rules.csv
  macros/regex_match.sql
  tests/
  profiles.yml      duckdb and postgres targets, read from environment variables
dashboard/          build.py, template.html
airflow/            dags/egypt_tech_jobs_daily.py, docker-compose.yml
data/raw/           date=YYYY-MM-DD/postings.parquet, run_summary.json
tests/              fixtures/, test_*.py
.github/workflows/  daily.yml, ci.yml
README.md
```

## 10. Risks

| Risk | Mitigation |
|---|---|
| A career API changes shape or blocks the runner | Per-company isolation and the run summary; the dashboard's health panel shows the failure |
| Too few tech postings for meaningful trends | Add Egyptian tech companies through the Greenhouse, Lever, Ashby and Workable APIs; the dashboard states its coverage honestly |
| The repo grows from daily data files | Postings are stored as zstd Parquet (estimated under 200 KB a day, about 70 MB a year); a yearly compaction is possible if needed |
| dbt does not support the newest Python | Pin Python 3.12 in CI and Docker |
| GitHub disables scheduled workflows in repos with no activity for 60 days | The daily data commit counts as activity |
