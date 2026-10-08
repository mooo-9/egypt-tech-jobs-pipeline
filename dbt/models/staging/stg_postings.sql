-- City: the earliest of these places in the location text. Greater Cairo districts roll up to
-- Cairo or Giza (6th of October, Sheikh Zayed and Smart Village are in Giza). Any other Egyptian
-- location is 'Egypt (other)'; no mention of Egypt (e.g. "4 Locations") is null.
with src as (

    select
        source_system,
        company_key,
        posting_id,
        trim(title) as title,
        trim(replace(location, chr(160), ' ')) as location,
        posted_raw,
        url,
        description,
        cast(collected_at as date) as collected_date
    from {{ source('raw', 'postings') }}

)

select
    source_system,
    company_key,
    posting_id,
    source_system || ':' || posting_id as posting_key,
    title,
    location,
    case {{ regex_extract('lower(location)', "'cairo|giza|alexandria|alex|nasr city|maadi|heliopolis|6th of october|sheikh zayed|smart village'") }}
        when 'cairo' then 'Cairo'
        when 'nasr city' then 'Cairo'
        when 'maadi' then 'Cairo'
        when 'heliopolis' then 'Cairo'
        when 'giza' then 'Giza'
        when '6th of october' then 'Giza'
        when 'sheikh zayed' then 'Giza'
        when 'smart village' then 'Giza'
        when 'alexandria' then 'Alexandria'
        when 'alex' then 'Alexandria'
        else case when {{ regex_match('location', "'egypt'") }} then 'Egypt (other)' end
    end as city,
    {{ parse_posted('posted_raw', 'collected_date') }} as posted_date,
    url,
    nullif(trim(description), '') as description_clean,
    collected_date
from src
