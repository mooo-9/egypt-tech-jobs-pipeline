-- A posting cannot have been posted after the day it was last collected (null posted dates are fine).
select posting_key, posted_date, last_seen
from {{ ref('fct_postings') }}
where posted_date > last_seen
