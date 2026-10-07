# Egypt Tech Jobs Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collect Egypt job postings daily from public career APIs, model them with dbt on DuckDB (GitHub Actions) and Postgres (local Airflow), and publish a skills-demand dashboard on GitHub Pages.

**Architecture:** A Python package (`pipeline`) runs one extractor per job system and writes an append-only, date-partitioned Parquet file plus a run summary. A single dbt project rebuilds the warehouse from those files on either target. A static dashboard is generated from the marts. GitHub Actions runs everything daily; Docker Compose runs the same steps under Airflow locally.

**Tech Stack:** Python 3.12, requests, pyarrow, PyYAML, pytest, responses (HTTP mocking), dbt-core 1.9, dbt-duckdb, dbt-postgres, DuckDB, Postgres 16, Apache Airflow 2.10 (official image), Chart.js 4, GitHub Actions, GitHub Pages.

**Spec:** `docs/superpowers/specs/2026-10-07-egypt-tech-jobs-pipeline-design.md`

## Global Constraints

- Python is pinned to 3.12 locally (`uv venv --python 3.12`), in CI and in Docker.
- Raw data path: `data/raw/date=YYYY-MM-DD/postings.parquet` (zstd) and `data/raw/date=YYYY-MM-DD/run_summary.json`.
- `Posting` fields, exactly: `source_system`, `company_key`, `posting_id`, `title`, `location`, `posted_raw`, `url`, `description` (nullable), `collected_at`.
- Politeness: at most 1 request per second per host, a 20-second timeout, 3 retries with backoff, and the User-Agent `egypt-tech-jobs-pipeline (+https://github.com/mooo-9/egypt-tech-jobs-pipeline)`.
- Only postings whose location (or title, if the location is empty) matches `egypt|cairo|giza|alexandria` (case-insensitive) are kept.
- `role_family` ∈ {`data_engineering`, `data_analysis`, `ai_ml`, `software`, `other`}; `seniority` ∈ {`intern`, `junior`, `mid`, `senior`}.
- Source freshness on `collected_at`: warn after 1 day, error after 2.
- Daily schedule: cron `0 4 * * *` (UTC).
- Tests never call the network. Only fixture-recording scripts do.
- Port collector logic from `the voice assistant repo's core/career/sources.py` and company settings from `the voice assistant repo's core/career/companies.json`. Do not import from that repo.
- Cost: $0. No paid APIs, no LLM calls.

## Review Focus

1. **A company returns zero Egypt postings, or its API errors.** The run continues, `run_summary.json` records `status: "empty"` or `"error"` with the message, and the run fails only if every company returned zero rows. Tested in Task 5 (`test_run_continues_when_one_company_fails`, `test_run_fails_when_all_companies_empty`).
2. **The same posting appears twice in one day** (pagination overlap, or one company listed under two systems). Rows are de-duplicated on `(source_system, posting_id)` before writing. Tested in Task 5 (`test_duplicate_postings_written_once`).
3. **Relative and odd date strings:** "Posted Today", "Posted Yesterday", "Posted 3 Days Ago", "Posted 30+ Days Ago", ISO dates, epoch milliseconds and empty strings. These parse to a date or to null, never to an error. Tested in Task 6 (a dbt unit test on `stg_postings`).
4. **HTML and non-ASCII text in descriptions.** Tags are stripped before skill matching, so `<li>SQL</li>` matches SQL. Arabic text passes through unchanged. Tested in Task 2 (`test_clean_text_strips_html_keeps_arabic`) and Task 7 (a skill unit test with an HTML description).
5. **Re-running the same day.** That day's files are replaced; earlier days are untouched, and the warehouse result is the same. Tested in Task 5 (`test_rerun_same_day_overwrites_only_that_day`).

---

### Task 1: Project scaffold and shared extraction helpers

**Files:**
- Create: `pyproject.toml`, `requirements.txt`, `.gitignore`, `pipeline/__init__.py`, `pipeline/extract/__init__.py`, `pipeline/extract/common.py`, `tests/test_common.py`

**Interfaces:**
- Produces:
  - `pipeline.extract.common.Posting`: a frozen dataclass with the Global Constraints fields; `collected_at: str` (ISO date) and `description: str | None`.
  - `pipeline.extract.common.http_get_json(url: str, params: dict | None = None) -> dict` and `http_post_json(url: str, body: dict) -> dict`. Both apply the politeness rules.
  - `pipeline.extract.common.in_egypt(location: str, title: str = "") -> bool`
  - `pipeline.extract.common.clean_text(html: str | None) -> str | None`: strips tags and unescapes entities.
  - `pipeline.extract.common.EXTRACTORS: dict[str, Callable[[dict, str], list[Posting]]]`: the registry, filled by later tasks. Signature: `fetch(company: dict, collected_at: str)`.
  - `pipeline.extract.common.DESCRIBERS: dict[str, Callable[[dict, Posting], str | None]]`: the registry of `describe` functions, for systems whose list response has no description.
  - `pipeline.extract.load_all()`: imports every extractor module so both registries are filled.

- [ ] **Step 1:** Create a Python 3.12 environment with `uv venv --python 3.12` and install `requirements.txt` (requests, pyarrow, PyYAML, pytest, responses, beautifulsoup4, dbt-core~=1.9, dbt-duckdb~=1.9, dbt-postgres~=1.9, duckdb). Verify with `.venv\Scripts\python -c "import dbt.version, duckdb, pyarrow; print('ok')"`. Expected: `ok`.
- [ ] **Step 2: Write failing tests** in `tests/test_common.py`:
  - `test_in_egypt_matches_cities`: `in_egypt("New Cairo, Egypt")`, `in_egypt("Giza")` and `in_egypt("", "Analyst - Alexandria")` are all true; `in_egypt("Dubai, UAE")` is false.
  - `test_clean_text_strips_html_keeps_arabic`: `clean_text("<li>SQL &amp; Python</li><p>مهندس بيانات</p>")` contains `"SQL & Python"` and `"مهندس بيانات"`, and contains no `<`. `clean_text(None) is None`.
  - `test_http_get_json_retries_then_succeeds`: using `responses`, return 503 twice then 200 `{"ok": 1}`. The result is `{"ok": 1}` and 3 calls were made.
  - `test_http_sends_user_agent`: the request header `User-Agent` equals the Global Constraints value.
  - `test_rate_limit_per_host`: with `time.monotonic` and `time.sleep` patched, two calls to the same host cause a sleep of about 1 second; calls to two different hosts cause no sleep.
- [ ] **Step 3:** Run `pytest tests/test_common.py -v`. Expected: FAIL (module missing).
- [ ] **Step 4:** Implement `common.py`. Use a `requests.Session`, a module-level `dict[host, last_call_time]` for rate limiting, and `BeautifulSoup(...).get_text(" ", strip=True)` in `clean_text`.
- [ ] **Step 5:** Run `pytest tests/test_common.py -v`. Expected: PASS.
- [ ] **Step 6:** Commit (`.gitignore` excludes `.venv/`, `target/`, `dbt_packages/`, `*.duckdb`, `site/`). Message: `Scaffold the project and shared extraction helpers`.

### Task 2: Workday and SmartRecruiters extractors, with descriptions

**Files:**
- Create: `pipeline/extract/workday.py`, `pipeline/extract/smartrecruiters.py`, `scripts/record_fixture.py`, `tests/fixtures/http/workday_*.json`, `tests/fixtures/http/smartrecruiters_*.json`, `tests/test_workday.py`, `tests/test_smartrecruiters.py`

**Interfaces:**
- Consumes: `Posting`, `http_get_json`, `http_post_json`, `in_egypt`, `clean_text`, `EXTRACTORS` (Task 1).
- Produces:
  - `workday.fetch(company, collected_at) -> list[Posting]`, registered as `EXTRACTORS["workday"]`. Company config keys: `workday: {host, tenant, site}`.
  - `workday.describe(company, posting) -> str | None`, registered as `DESCRIBERS["workday"]`.
  - `smartrecruiters.fetch` and `smartrecruiters.describe`, registered in both registries as `"smartrecruiters"`. Config key: `smartrecruiters: {company}`.
  - `scripts/record_fixture.py <system> <company_key>` saves live responses into `tests/fixtures/http/`. This is the only code that calls the network outside a real run.

- [ ] **Step 1:** Record fixtures for one Workday company (e.g. Mastercard) and for the SmartRecruiters company, using `scripts/record_fixture.py`. Save the list response and one detail response for each.
- [ ] **Step 2: Write failing tests** with `responses` serving the fixtures:
  - `test_workday_keeps_only_egypt`: every returned `Posting` passes `in_egypt`, and has `source_system == "workday"` and a non-empty `posting_id`.
  - `test_workday_paginates_until_total`: two pages of fixtures. Every posting from both pages is returned, and no request is made past `total`.
  - `test_workday_posting_id_is_stable`: `posting_id` comes from the Workday `externalPath` (the job requisition ID after the last `_`), not from the list index.
  - `test_workday_describe_returns_clean_text`: `describe()` returns text with no HTML tags.
  - `test_smartrecruiters_fetch_and_describe`: the same assertions for SmartRecruiters; the description is joined from `jobAd.sections.*.text`.
- [ ] **Step 3:** Run `pytest tests/test_workday.py tests/test_smartrecruiters.py -v`. Expected: FAIL.
- [ ] **Step 4:** Implement both modules, porting `_workday` and `_smartrecruiters` from the voice assistant repo `sources.py` (page size 20, max 100, `searchText: "Egypt"`). Workday's detail call is `GET https://{host}/wday/cxs/{tenant}/{site}{externalPath}` → `jobPostingInfo.jobDescription`. `fetch` leaves `description=None`; `describe` fills it.
- [ ] **Step 5:** Run the tests. Expected: PASS.
- [ ] **Step 6:** Commit: `Add Workday and SmartRecruiters extractors`.

### Task 3: Oracle Cloud, Eightfold, Jibe, Phenom and Amazon extractors

**Files:**
- Create: `pipeline/extract/{oracle_cloud,eightfold,jibe,phenom,amazon}.py`, fixtures, `tests/test_ported_extractors.py`

**Interfaces:**
- Consumes: Task 1 helpers.
- Produces: `fetch(company, collected_at) -> list[Posting]` for each, registered under `oracle_cloud`, `eightfold`, `jibe`, `phenom` and `amazon`. Each sets `description` when the list response already includes it (Amazon `description`, Jibe `data.description`), and `None` otherwise. These systems have no `describe`.

- [ ] **Step 1:** Record one list fixture per system.
- [ ] **Step 2: Write failing tests**, parametrized over the five systems: `test_<system>_returns_egypt_postings_with_ids` (non-empty list, all pass `in_egypt`, unique `posting_id`s, correct `source_system`), and `test_jibe_stops_at_max_pages` (stops after 20 pages even if more are offered).
- [ ] **Step 3:** Run. Expected: FAIL.
- [ ] **Step 4:** Implement by porting `_oracle_cloud`, `_eightfold`, `_jibe`, `_phenom` and `_amazon` from the voice assistant repo `sources.py`, mapping each system's own job ID to `posting_id`.
- [ ] **Step 5:** Run. Expected: PASS.
- [ ] **Step 6:** Commit: `Port the Oracle, Eightfold, Jibe, Phenom and Amazon extractors`.

### Task 4: Job-board API extractors and the company list

**Files:**
- Create: `pipeline/extract/{greenhouse,lever,ashby,workable}.py`, `companies.yml`, `scripts/check_company.py`, fixtures, `tests/test_board_extractors.py`, `tests/test_companies_config.py`

**Interfaces:**
- Produces:
  - `fetch` for `greenhouse` (`GET https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true`), `lever` (`GET https://api.lever.co/v0/postings/{site}?mode=json`), `ashby` (`GET https://api.ashbyhq.com/posting-api/job-board/{board}`) and `workable` (`GET https://apply.workable.com/api/v1/widget/accounts/{account}?details=true`). Each sets `description` from the list response.
  - `companies.yml`: a list of `{key, name, industry, <system>: {...}}`.
  - `scripts/check_company.py <system> <id>` makes one live call and prints how many postings are located in Egypt.

- [ ] **Step 1: Write failing tests**, one per board system, from recorded fixtures: Egypt-only results, `description` is non-empty and contains no tags, and `posting_id` is the system's ID.
- [ ] **Step 2:** Write `test_companies_config.py`: every entry has `key`, `name` and `industry`; keys are unique; each entry has exactly one system key, and that key is in `EXTRACTORS`.
- [ ] **Step 3:** Run. Expected: FAIL.
- [ ] **Step 4:** Implement the four modules.
- [ ] **Step 5:** Build `companies.yml`. Port the 18 API-backed firms from the voice assistant repo `companies.json`, then find Egyptian tech companies on the four board systems. Add a company only if `scripts/check_company.py` shows at least 1 Egypt posting, and record the date checked in a YAML comment.
- [ ] **Step 6:** Run all tests. Expected: PASS.
- [ ] **Step 7:** Commit: `Add Greenhouse, Lever, Ashby and Workable extractors and the company list`.

### Task 5: The run: isolation, description carry-forward, Parquet and summary

**Files:**
- Create: `pipeline/run.py`, `tests/test_run.py`

**Interfaces:**
- Consumes: `load_all()`, `EXTRACTORS` and `DESCRIBERS` (Tasks 1–4), and `companies.yml`. A new posting gets `DESCRIBERS[system](company, posting)` only when its `description` is `None` and the system has a describer.
- Produces:
  - `pipeline.run.run(day: str, companies_path: Path, raw_dir: Path) -> dict`: returns the summary; writes `raw_dir/date=<day>/postings.parquet` and `run_summary.json`.
  - CLI: `python -m pipeline.run [--date YYYY-MM-DD]` (default: today in UTC). It exits with code 1 when every company returned zero rows.
  - Summary shape: `{"date", "started_at", "duration_s", "rows", "companies": [{"key", "status": "ok"|"empty"|"error", "rows", "error", "duration_s"}]}`.

- [ ] **Step 1: Write failing tests**, with extractors replaced by fakes:
  - `test_run_writes_parquet_with_schema`: the Parquet columns equal the `Posting` fields, and the compression is zstd.
  - `test_run_continues_when_one_company_fails`: one fake raises. The others are written, and the failing company has `status == "error"` and a message.
  - `test_run_fails_when_all_companies_empty`: the CLI exits with 1, and `run_summary.json` is still written.
  - `test_duplicate_postings_written_once`: the same `(source_system, posting_id)` returned twice gives one row.
  - `test_description_carried_forward_not_refetched`: an earlier day's Parquet has a description for posting X. Today's run reuses it and `describe` is not called for X, but is called once for a new posting Y.
  - `test_rerun_same_day_overwrites_only_that_day`: run day D twice and day D−1 once. D's file holds only the second run's rows, and D−1's file is unchanged.
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `run.py`. Read earlier days with `pyarrow.dataset` to find known IDs; run companies one after another (the rate limit is per host); wrap each company in try/except; write to a temporary file, then `os.replace` it into place.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5:** Do one live run, `python -m pipeline.run`. Expected: summary `rows > 0`; Parquet written; at least half the companies `ok`. Commit that day's data along with the code.
- [ ] **Step 6:** Commit: `Run every extractor daily into partitioned Parquet with a run summary`.

### Task 6: dbt project, sources and staging

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/models/staging/{_sources.yml,stg_postings.sql,_staging.yml}`, `dbt/macros/regex_match.sql`, `dbt/macros/parse_posted.sql`, `tests/fixtures/raw/date=2026-10-01/postings.parquet`, `tests/fixtures/raw/date=2026-10-02/postings.parquet`, `scripts/make_fixture_raw.py`

**Interfaces:**
- Consumes: the raw Parquet layout (Task 5).
- Produces:
  - Source `raw.postings`. DuckDB reads `read_parquet('{{ env_var("RAW_GLOB", "../data/raw/*/postings.parquet") }}', hive_partitioning = true)`; Postgres reads table `raw.postings`.
  - `stg_postings` columns: `source_system, company_key, posting_id, posting_key (= source_system||':'||posting_id), title, location, city, posted_date (date, nullable), url, description_clean, collected_date (date)`.
  - Macro `regex_match(col, pattern)`, with the `duckdb__` version using `regexp_matches(col, pattern, 'i')` and the `postgres__` version using `col ~* pattern`.
  - Profiles: `duckdb` target (`path: warehouse.duckdb`) and `postgres` target (from `PG_HOST`, `PG_USER`, `PG_PASSWORD`, `PG_DB`).

- [ ] **Step 1:** Write `scripts/make_fixture_raw.py`, which generates two small, hand-written fixture days covering the Review Focus date strings and duplicates. Run it.
- [ ] **Step 2: Write a failing dbt unit test** (`_staging.yml`, `unit_tests:`) for `stg_postings` with rows where `posted_raw` is `"Posted Today"`, `"Posted Yesterday"`, `"Posted 3 Days Ago"`, `"Posted 30+ Days Ago"`, `"2026-09-28"`, `"1727481600000"` and `""`, all with collected date 2026-10-02. Expected `posted_date`: 2026-10-02, 2026-10-01, 2026-09-29, 2026-09-02, 2026-09-28, 2024-09-28 and null.
- [ ] **Step 3:** Run `cd dbt && RAW_GLOB=../tests/fixtures/raw/*/postings.parquet dbt build --target duckdb --select stg_postings`. Expected: FAIL.
- [ ] **Step 4:** Implement the source, `stg_postings`, `regex_match` and `parse_posted` (a dispatched macro for each database).
- [ ] **Step 5:** Add schema tests: `posting_key` not null; the `(posting_key, collected_date)` pair is unique (`dbt_utils`-free: a singular test). Run `dbt build`. Expected: PASS.
- [ ] **Step 6:** Commit: `Add the dbt project, raw source and staging model`.

### Task 7: Intermediate models and seeds

**Files:**
- Create: `dbt/seeds/skills.csv`, `dbt/seeds/title_rules.csv`, `dbt/seeds/_seeds.yml`, `dbt/models/intermediate/{int_posting_lifecycle,int_posting_skills,int_posting_roles}.sql`, `dbt/models/intermediate/_intermediate.yml`

**Interfaces:**
- Consumes: `stg_postings`, `regex_match`.
- Produces:
  - `int_posting_lifecycle(posting_key, company_key, title, city, url, first_seen, last_seen, is_open, days_open)`. `is_open` means `last_seen` equals the latest `collected_date` in the data.
  - `int_posting_skills(posting_key, skill, category)`, distinct.
  - `int_posting_roles(posting_key, role_family, seniority)`. The first matching rule by `priority` wins; the defaults are `other` and `mid`.
  - `skills.csv` columns `skill,category,pattern`, with at least these skills: Python, SQL, Spark, Airflow, dbt, Kafka, AWS, Azure, GCP, Docker, Kubernetes, Power BI, Tableau, Excel, Pandas, TensorFlow, PyTorch, scikit-learn, LLM, Java, JavaScript, Go, R, Snowflake, Databricks, PostgreSQL, Git, Linux. Patterns use word boundaries.
  - `title_rules.csv` columns `field,priority,pattern,value`.

- [ ] **Step 1: Write failing dbt unit tests:**
  - `int_posting_skills`: the description `"<li>SQL</li> and Go-to-market"` produces SQL only, not Go. `"Google Cloud (GCP)"` produces GCP, not Go. `"R and Python"` produces R and Python. `"Our Requirements"` does not produce R.
  - `int_posting_roles`: "Data Engineer Intern" → data_engineering/intern. "Senior BI Analyst" → data_analysis/senior. "ML Ops Engineer" → ai_ml/mid. "Software Engineer, Data Platform" → data_engineering/mid. "Graduate Software Engineer" → software/junior. "Accountant" → other/mid.
  - `int_posting_lifecycle`: a posting seen on 10-01 and 10-02, when the latest day is 10-02, gives `is_open = true` and `days_open = 2`. A posting seen only on 10-01 gives `is_open = false`.
- [ ] **Step 2:** Run `dbt build --select +int_posting_skills +int_posting_roles +int_posting_lifecycle`. Expected: FAIL.
- [ ] **Step 3:** Write the seeds and the three models. Skills are matched on `title || ' ' || coalesce(description_clean, '')` from each posting's latest row.
- [ ] **Step 4:** Add `accepted_values` tests on `role_family` and `seniority`. Run `dbt build`. Expected: PASS.
- [ ] **Step 5:** Commit: `Add lifecycle, skill and role models with their seeds`.

### Task 8: Marts, data tests and freshness

**Files:**
- Create: `dbt/models/marts/{dim_company,fct_postings,fct_posting_skills,mart_skill_demand_weekly,mart_role_demand_daily}.sql`, `dbt/models/marts/_marts.yml`, `dbt/seeds/companies.csv` (generated), `scripts/companies_to_seed.py`, `dbt/tests/{assert_no_future_postings,assert_skill_share_in_range,assert_latest_run_has_rows}.sql`

**Interfaces:**
- Consumes: the intermediate models; `companies.yml` (converted into a seed by `scripts/companies_to_seed.py`).
- Produces: the marts with the grains in spec Section 5. `mart_skill_demand_weekly(week_start, skill, category, role_scope, open_postings, share)`, where `role_scope` ∈ {`all`, `data`, `ai_ml`} (`data` = data_engineering + data_analysis). `mart_role_demand_daily(day, role_family, open_postings, new_postings)`.

- [ ] **Step 1: Write a failing dbt unit test** for `mart_skill_demand_weekly`: 4 open postings, 2 of which mention SQL, give `share = 0.5` for SQL with `role_scope = 'all'`.
- [ ] **Step 2:** Write the three singular tests from the spec; the `relationships` tests from `fct_postings` and `fct_posting_skills` to `dim_company`; and the source freshness (`loaded_at_field: collected_at`, warn 1 day, error 2 days).
- [ ] **Step 3:** Run `dbt build`. Expected: FAIL.
- [ ] **Step 4:** Implement `companies_to_seed.py` and the five marts.
- [ ] **Step 5:** Run `dbt build` on the fixtures, then on the real data from Task 5. Expected: PASS both times. `dbt source freshness` on the real data: PASS.
- [ ] **Step 6:** Commit: `Add marts, data tests and source freshness`.

### Task 9: Dashboard

**Files:**
- Create: `dashboard/build.py`, `dashboard/template.html`, `tests/test_dashboard.py`

**Interfaces:**
- Consumes: the marts in `dbt/warehouse.duckdb`; the latest `run_summary.json`.
- Produces: `python -m dashboard.build --db dbt/warehouse.duckdb --raw data/raw --out site` writes `site/index.html` and `site/data.json`. `data.json` keys: `generated_at, coverage{companies, postings_open}, skills_this_week{all,data,ai_ml}[{skill,category,open_postings,share}], skill_trend{weeks[], series{skill: [counts]}}, roles[{role_family, open_postings}], seniority[{seniority, open_postings}], top_companies[{name, open_postings}], health{last_run, rows, sources_ok, sources_failed[]}`.

- [ ] **Step 1: Write failing tests:** `test_build_writes_expected_keys` (run against a warehouse built from the fixtures; `data.json` has every key above); `test_top_skills_limited_to_15` (each list in `skills_this_week` has at most 15 items, sorted by `open_postings` descending); `test_health_lists_failed_sources` (a summary with one `error` company appears in `sources_failed`).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement `build.py` with `duckdb`. Write `template.html` (Chart.js 4 from cdnjs) with these panels: skills this week with the all/data/AI-ML filter, the top-10 skill trend, roles, seniority, top companies, pipeline health and the coverage note. Use the same dark style as the HR Bias Detection demo, and make it fit a 375px-wide phone screen with no sideways scrolling.
- [ ] **Step 4:** Run the tests. Expected: PASS. Then build it from the real data and open `site/index.html` in the browser preview at desktop and phone widths. Expected: every panel renders, there are no console errors, and the phone layout has no sideways scrolling.
- [ ] **Step 5:** Commit: `Add the static dashboard`.

### Task 10: GitHub Actions, the public repo and Pages

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/daily.yml`

**Interfaces:**
- Consumes: the CLIs from Tasks 5, 8 and 9.
- Produces: daily runs and a public dashboard at `https://mooo-9.github.io/egypt-tech-jobs-pipeline/`.

- [ ] **Step 1:** Write `ci.yml` (on pull requests: Python 3.12, install, `pytest`, `dbt deps` if needed, then `dbt build --target duckdb` with `RAW_GLOB` set to the fixtures).
- [ ] **Step 2:** Write `daily.yml` (cron `0 4 * * *` plus `workflow_dispatch`; `permissions: contents: write, pages: write, id-token: write`). Steps: run → commit `data/raw/date=…` as `data: YYYY-MM-DD` by `github-actions[bot]` → `dbt seed` → `dbt source freshness` → `dbt build` → `dashboard.build` → `actions/upload-pages-artifact` → `actions/deploy-pages`.
- [ ] **Step 3:** **Ask the owner before this step**, because it publishes. Create the public repo `mooo-9/egypt-tech-jobs-pipeline`, push `main`, enable Pages with source "GitHub Actions", and start `daily.yml` by hand.
- [ ] **Step 4:** Verify: the run is green, a `data:` commit appears, and the Pages URL returns 200 with the dashboard. Open a test pull request; `ci.yml` is green.
- [ ] **Step 5:** Commit any fixes: `Run the pipeline daily on GitHub Actions and publish to Pages`.

### Task 11: README

**Files:**
- Create: `README.md`, `docs/images/dashboard.png`

- [ ] **Step 1:** Write the README: a one-paragraph summary; links to the live dashboard and the tests badge; a Mermaid architecture diagram matching spec Section 3; the data model table; how each run is tested; "Run it locally" (uv, `python -m pipeline.run`, dbt) and "Run it with Airflow" (Task 12); design decisions (full rebuild, Parquet partitions, why no Spark or Kafka at this volume and how it would scale); data coverage and limits.
- [ ] **Step 2:** Capture a dashboard screenshot at 1440px with Playwright into `docs/images/dashboard.png`.
- [ ] **Step 3:** Check that every command in the README runs as written in a fresh shell. Expected: each succeeds.
- [ ] **Step 4:** Commit: `Write the README`.

### Task 12: Local Airflow with Postgres

**Prerequisite:** the owner installs Docker Desktop with WSL2. This needs admin rights and possibly a restart; do not start the task until `docker info` succeeds.

**Files:**
- Create: `airflow/docker-compose.yml`, `airflow/Dockerfile`, `airflow/dags/egypt_tech_jobs_daily.py`, `pipeline/load.py`, `tests/test_load.py`, `tests/test_dag.py`

**Interfaces:**
- Consumes: `pipeline.run.run`, the dbt project (`--target postgres`).
- Produces:
  - `pipeline.load.load_raw(raw_dir: Path, dsn: str) -> int`: replaces `raw.postings` with every Parquet day; returns the row count.
  - DAG `egypt_tech_jobs_daily`: `extract` (PythonOperator calling `run`) → `load_to_postgres` → `dbt_build` (BashOperator: `dbt seed && dbt build --target postgres`). `schedule="0 4 * * *"`, `catchup=False`.

- [ ] **Step 1: Write failing tests:** `test_dag_loads_and_has_three_tasks_in_order` (DagBag has no import errors; task IDs and dependencies as above); `test_load_raw_is_idempotent` (against Postgres from `docker compose up postgres`: loading twice gives the same row count).
- [ ] **Step 2:** Run. Expected: FAIL.
- [ ] **Step 3:** Implement: `Dockerfile` (FROM `apache/airflow:2.10.x-python3.12`, then install requirements); `docker-compose.yml` (postgres:16, airflow-init, webserver, scheduler; mount `pipeline/`, `dbt/`, `data/` and `companies.yml`); `load.py` using `COPY` through psycopg; the DAG.
- [ ] **Step 4:** Run `docker compose up -d`, trigger the DAG in the Airflow UI at `localhost:8080`, and confirm all three tasks succeed. In psql, `select count(*) from marts.fct_postings` matches the DuckDB count for the same data.
- [ ] **Step 5:** Run the tests. Expected: PASS. Add a screenshot of the Airflow graph to the README.
- [ ] **Step 6:** Commit: `Run the same pipeline under Airflow with Postgres locally`.

### After the plan

Once the daily run has been green for 7 days in a row, add the project to the CV with its dashboard and code links. Use real numbers from the dashboard (companies covered, postings tracked, tests).
