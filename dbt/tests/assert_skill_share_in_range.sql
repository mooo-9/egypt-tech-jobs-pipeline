-- A skill's share of open postings is a fraction.
select week_start, skill, role_scope, share
from {{ ref('mart_skill_demand_weekly') }}
where share < 0 or share > 1
