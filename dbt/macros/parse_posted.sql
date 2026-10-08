{#
  Turn a raw posted string into a date, or null. Never errors on a value it does not recognise.
  Handled: "Posted Today", "Posted Yesterday", "Posted N Days Ago" / "Posted 30+ Days Ago" (30+ counts as 30),
  "YYYY-MM-DD" and full ISO timestamps (the date part as written, no timezone shift),
  epoch milliseconds, and "Month D, YYYY". Anything else, including "", is null.
  `collected` is the collected date column that relative strings count back from.
#}
{% macro parse_posted(posted, collected) %}{{ return(adapter.dispatch('parse_posted')(posted, collected)) }}{% endmacro %}

{% macro duckdb__parse_posted(posted, collected) %}
case
    when {{ posted }} is null then null
    when lower(trim({{ posted }})) = 'posted today' then {{ collected }}
    when lower(trim({{ posted }})) = 'posted yesterday' then {{ collected }} - 1
    when {{ regex_match(posted, "'^posted \d+\+? days? ago$'") }}
        then {{ collected }} - cast(regexp_extract({{ posted }}, '\d+') as integer)
    when regexp_matches({{ posted }}, '^\d{4}-\d{2}-\d{2}') then try_cast(substr({{ posted }}, 1, 10) as date)
    when regexp_matches({{ posted }}, '^\d{12,13}$') then cast(epoch_ms(cast({{ posted }} as bigint)) as date)
    when regexp_matches({{ posted }}, '^[A-Za-z]+\s+\d{1,2},\s+\d{4}$')
        then cast(try_strptime(regexp_replace(trim({{ posted }}), '\s+', ' ', 'g'), '%B %d, %Y') as date)
    else null
end
{% endmacro %}

{# ponytail: untested until Task 12 (no Postgres here). A date like 2026-02-30 would raise in Postgres; DuckDB nulls it. #}
{% macro postgres__parse_posted(posted, collected) %}
case
    when {{ posted }} is null then null
    when lower(trim({{ posted }})) = 'posted today' then {{ collected }}
    when lower(trim({{ posted }})) = 'posted yesterday' then {{ collected }} - 1
    when {{ regex_match(posted, "'^posted \d+\+? days? ago$'") }}
        then {{ collected }} - cast(substring({{ posted }} from '\d+') as integer)
    when {{ posted }} ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])' then cast(substr({{ posted }}, 1, 10) as date)
    when {{ posted }} ~ '^\d{12,13}$' then cast(to_timestamp(cast({{ posted }} as bigint) / 1000.0) at time zone 'UTC' as date)
    when {{ posted }} ~ '^[A-Za-z]+\s+\d{1,2},\s+\d{4}$'
        then to_date(regexp_replace(trim({{ posted }}), '\s+', ' ', 'g'), 'FMMonth FMDD, YYYY')
    else null
end
{% endmacro %}
