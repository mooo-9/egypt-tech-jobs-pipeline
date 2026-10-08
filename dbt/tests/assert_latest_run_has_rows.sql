-- The latest collection day loaded at least one posting (also fails when nothing was loaded at all).
select 'latest run loaded no postings' as problem
where not exists (
    select 1 from {{ ref('stg_postings') }}
    where collected_date = (select max(collected_date) from {{ ref('stg_postings') }})
)
