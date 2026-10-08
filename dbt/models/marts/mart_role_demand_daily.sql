-- open = collected that day; new = first seen that day. One row per collection day x role family seen.
with days as (

    select distinct collected_date as day from {{ ref('stg_postings') }}

),

families as (

    select distinct role_family from {{ ref('fct_postings') }}

),

open_counts as (

    select s.collected_date as day, f.role_family, count(distinct s.posting_key) as open_postings
    from {{ ref('stg_postings') }} as s
    join {{ ref('fct_postings') }} as f on f.posting_key = s.posting_key
    group by s.collected_date, f.role_family

),

new_counts as (

    select first_seen as day, role_family, count(*) as new_postings
    from {{ ref('fct_postings') }}
    group by first_seen, role_family

)

select
    d.day,
    f.role_family,
    coalesce(o.open_postings, 0) as open_postings,
    coalesce(n.new_postings, 0) as new_postings
from days as d
cross join families as f
left join open_counts as o on o.day = d.day and o.role_family = f.role_family
left join new_counts as n on n.day = d.day and n.role_family = f.role_family
