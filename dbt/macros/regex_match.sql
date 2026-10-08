{# Case-insensitive regex tests and extraction that work on both DuckDB and Postgres. #}
{% macro regex_match(col, pattern) %}{{ return(adapter.dispatch('regex_match')(col, pattern)) }}{% endmacro %}

{% macro duckdb__regex_match(col, pattern) %}regexp_matches({{ col }}, {{ pattern }}, 'i'){% endmacro %}

{% macro postgres__regex_match(col, pattern) %}({{ col }} ~* {{ pattern }}){% endmacro %}

{# First (leftmost) match of pattern in col; empty string (DuckDB) or null (Postgres) when none. #}
{% macro regex_extract(col, pattern) %}{{ return(adapter.dispatch('regex_extract')(col, pattern)) }}{% endmacro %}

{% macro duckdb__regex_extract(col, pattern) %}regexp_extract({{ col }}, {{ pattern }}){% endmacro %}

{% macro postgres__regex_extract(col, pattern) %}substring({{ col }} from {{ pattern }}){% endmacro %}
