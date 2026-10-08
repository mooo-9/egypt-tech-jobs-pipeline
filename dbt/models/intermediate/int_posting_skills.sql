-- Skills are matched on title + description from each posting's latest row; many postings have no
-- description, so those match on the title alone.
select distinct
    p.posting_key,
    s.skill,
    s.category
from {{ latest_postings() }} as p
join {{ ref('skills') }} as s
    on {{ regex_match("p.title || ' ' || coalesce(p.description_clean, '')", 's.pattern') }}
