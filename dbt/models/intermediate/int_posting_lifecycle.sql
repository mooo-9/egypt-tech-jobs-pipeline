-- is_open: the posting was still there on the latest day anything was collected.
with seen as (

    select
        posting_key,
        min(collected_date) as first_seen,
        max(collected_date) as last_seen
    from {{ ref('stg_postings') }}
    group by posting_key

)

select
    p.posting_key,
    p.company_key,
    p.title,
    p.city,
    p.url,
    seen.first_seen,
    seen.last_seen,
    seen.last_seen = (select max(collected_date) from {{ ref('stg_postings') }}) as is_open,
    seen.last_seen - seen.first_seen + 1 as days_open
from {{ latest_postings() }} as p
join seen on seen.posting_key = p.posting_key
