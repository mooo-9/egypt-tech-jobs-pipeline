-- A posting appears at most once per collection day.
select posting_key, collected_date, count(*) as n
from {{ ref('stg_postings') }}
group by posting_key, collected_date
having count(*) > 1
