-- Runs the real seed patterns over known texts and titles; returns every case the seeds get wrong.
with skill_cases as (

    select * from (values
        ('<li>SQL</li> and Go-to-market', 'SQL', true),
        ('<li>SQL</li> and Go-to-market', 'Go', false),
        ('Google Cloud (GCP)', 'GCP', true),
        ('Google Cloud (GCP)', 'Go', false),
        ('R and Python', 'R', true),
        ('R and Python', 'Python', true),
        ('Our Requirements', 'R', false),
        ('Python, Go, Java', 'Go', true),
        ('Backend Engineer (Go)', 'Go', true),
        ('Golang Developer', 'Go', true),
        ('we go above and beyond', 'Go', false),
        ('cost benefit analysis and go/no-go decisions', 'Go', false),
        ('Google', 'Go', false),
        ('Java services', 'JavaScript', false),
        ('JavaScript', 'Java', false),
        ('PostgreSQL', 'SQL', false),
        ('Statistics, R, SAS', 'R', true),
        ('Research and Development', 'R', false),
        ('Spark plug maker, we spark joy', 'Spark', false),
        ('Python, Spark, Kafka', 'Spark', true),
        ('Excel in a fast team', 'Excel', false),
        ('Advanced Excel and Power BI', 'Excel', true),
        ('Advanced Excel and Power BI', 'Power BI', true)
    ) as c(text, skill, should_match)

),

skill_misses as (

    select c.text as input, c.skill as expected, cast(c.should_match as varchar) as detail
    from skill_cases as c
    join {{ ref('skills') }} as s on s.skill = c.skill
    where {{ regex_match('c.text', 's.pattern') }} <> c.should_match

),

title_cases as (

    select * from (values
        ('Data Engineer Intern', 'data_engineering', 'intern'),
        ('Senior BI Analyst', 'data_analysis', 'senior'),
        ('ML Ops Engineer', 'ai_ml', 'mid'),
        ('Software Engineer, Data Platform', 'data_engineering', 'mid'),
        ('Graduate Software Engineer', 'software', 'junior'),
        ('Accountant', 'other', 'mid')
    ) as c(title, role_family, seniority)

),

title_got as (

    select
        c.*,
        coalesce((
            select r.value from {{ ref('title_rules') }} as r
            where r.field = 'role_family' and {{ regex_match('c.title', 'r.pattern') }}
            order by r.priority limit 1
        ), 'other') as got_role_family,
        coalesce((
            select r.value from {{ ref('title_rules') }} as r
            where r.field = 'seniority' and {{ regex_match('c.title', 'r.pattern') }}
            order by r.priority limit 1
        ), 'mid') as got_seniority
    from title_cases as c

)

select input, expected, detail from skill_misses
union all
select title, role_family || '/' || seniority, got_role_family || '/' || got_seniority
from title_got
where got_role_family <> role_family or got_seniority <> seniority
