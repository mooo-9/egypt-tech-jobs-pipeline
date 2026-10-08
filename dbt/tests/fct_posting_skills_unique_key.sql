-- (posting_key, skill) is the key of the bridge table.
select posting_key, skill, count(*) as n
from {{ ref('fct_posting_skills') }}
group by posting_key, skill
having count(*) > 1
