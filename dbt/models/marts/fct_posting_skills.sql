select s.posting_key, f.company_key, s.skill, s.category
from {{ ref('int_posting_skills') }} as s
join {{ ref('fct_postings') }} as f on f.posting_key = s.posting_key
