-- A posting cannot have been posted after the first day it was collected (null posted dates are fine).
-- Warn, not error: a repost whose first rows had no parsable date can trip this, and that
-- should not stop the day's dashboard update.
{{ config(severity='warn') }}
select posting_key, posted_date, first_seen
from {{ ref('fct_postings') }}
where posted_date > first_seen
