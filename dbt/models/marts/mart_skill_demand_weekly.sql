-- A posting is open in a week when it was collected on at least one day of that week (Monday start).
-- role_scope all = every role family; data = data_engineering + data_analysis; ai_ml; tech = software +
-- data_engineering + data_analysis + ai_ml. share = open postings in the scope mentioning the skill /
-- all open postings in the scope that week.
with scopes as (

    select * from (values
        ('all', 'data_engineering'), ('all', 'data_analysis'), ('all', 'ai_ml'), ('all', 'software'), ('all', 'other'),
        ('data', 'data_engineering'), ('data', 'data_analysis'),
        ('ai_ml', 'ai_ml'),
        ('tech', 'software'), ('tech', 'data_engineering'), ('tech', 'data_analysis'), ('tech', 'ai_ml')
    ) as s(role_scope, role_family)

),

seen_in_week as (

    select distinct cast(date_trunc('week', collected_date) as date) as week_start, posting_key
    from {{ ref('stg_postings') }}

),

open_in_scope as (

    select w.week_start, s.role_scope, w.posting_key
    from seen_in_week as w
    join {{ ref('fct_postings') }} as f on f.posting_key = w.posting_key
    join scopes as s on s.role_family = f.role_family

),

scope_totals as (

    select week_start, role_scope, count(*) as total
    from open_in_scope
    group by week_start, role_scope

),

skill_counts as (

    select o.week_start, o.role_scope, k.skill, k.category, count(*) as open_postings
    from open_in_scope as o
    join {{ ref('fct_posting_skills') }} as k on k.posting_key = o.posting_key
    group by o.week_start, o.role_scope, k.skill, k.category

)

select
    c.week_start,
    c.skill,
    c.category,
    c.role_scope,
    c.open_postings,
    cast(c.open_postings as double precision) / t.total as share
from skill_counts as c
join scope_totals as t on t.week_start = c.week_start and t.role_scope = c.role_scope
