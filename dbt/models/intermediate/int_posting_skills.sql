-- Skills are matched on the title from each posting's latest row plus its latest non-null description.
-- The raw files store a description only on the first day it is known (later rows are null), and many
-- postings have none at all, so those match on the title alone.
with descriptions as (

    select
        posting_key,
        description_clean,
        row_number() over (partition by posting_key order by collected_date desc) as rn
    from {{ ref('stg_postings') }}
    where description_clean is not null

)

select distinct
    p.posting_key,
    s.skill,
    s.category
from {{ latest_postings() }} as p
left join descriptions as d on d.posting_key = p.posting_key and d.rn = 1
join {{ ref('skills') }} as s
    on {{ regex_match("p.title || ' ' || coalesce(d.description_clean, '')", 's.pattern') }}
