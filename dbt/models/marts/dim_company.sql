select company_key, name, industry, source_system
from {{ ref('companies') }}
