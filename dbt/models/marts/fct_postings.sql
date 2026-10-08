select
    l.posting_key,
    l.company_key,
    l.title,
    l.city,
    l.url,
    l.posted_date,
    l.first_seen,
    l.last_seen,
    l.is_open,
    l.days_open,
    r.role_family,
    r.seniority
from {{ ref('int_posting_lifecycle') }} as l
join {{ ref('int_posting_roles') }} as r on r.posting_key = l.posting_key
