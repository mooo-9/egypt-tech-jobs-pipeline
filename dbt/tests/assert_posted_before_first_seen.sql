-- A posting cannot have been posted after the first day it was collected (null posted dates are fine).
select posting_key, posted_date, first_seen
from {{ ref('fct_postings') }}
where posted_date > first_seen
