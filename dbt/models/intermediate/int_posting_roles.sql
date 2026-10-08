-- Per field the matching rule with the lowest priority number wins; no match falls back to other / mid.
with p as (

    select posting_key, title from {{ latest_postings() }}

),

ranked as (

    select
        p.posting_key,
        r.field,
        r.value,
        row_number() over (partition by p.posting_key, r.field order by r.priority) as rn
    from p
    join {{ ref('title_rules') }} as r
        on {{ regex_match('p.title', 'r.pattern') }}

)

select
    p.posting_key,
    coalesce(rf.value, 'other') as role_family,
    coalesce(sn.value, 'mid') as seniority
from p
left join ranked as rf on rf.posting_key = p.posting_key and rf.field = 'role_family' and rf.rn = 1
left join ranked as sn on sn.posting_key = p.posting_key and sn.field = 'seniority' and sn.rn = 1
