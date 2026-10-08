{# DuckDB has no load step: expose the raw Parquet files as the view raw.postings on every run, so
   source('raw', 'postings') is a real relation (dbt unit tests need one). Postgres: the Airflow load task owns the table. #}
{% macro create_raw_postings() %}{{ return(adapter.dispatch('create_raw_postings')()) }}{% endmacro %}

{% macro duckdb__create_raw_postings() %}
    {% do run_query('create schema if not exists raw') %}
    {% do run_query("create or replace view raw.postings as select * from read_parquet('" ~ env_var('RAW_GLOB', '../data/raw/*/postings.parquet') ~ "', hive_partitioning = true)") %}
{% endmacro %}

{% macro postgres__create_raw_postings() %}{% endmacro %}
