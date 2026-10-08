-- A posting lists each skill at most once.
select posting_key, skill, count(*) as n
from {{ ref('int_posting_skills') }}
group by posting_key, skill
having count(*) > 1
