{# Each posting's most recent row from stg_postings (one row per posting_key). #}
{% macro latest_postings() %}
(
    select * from (
        select *, row_number() over (partition by posting_key order by collected_date desc) as rn
        from {{ ref('stg_postings') }}
    ) ranked
    where rn = 1
)
{% endmacro %}
