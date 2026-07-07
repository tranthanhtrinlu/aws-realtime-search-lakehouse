with ranked as (
    select
        event_date,
        keyword,
        search_count,
        row_number() over (
            partition by event_date
            order by search_count desc
        ) as rank_in_day
    from {{ ref('stg_keyword_trends') }}
)

select
    event_date,
    keyword,
    search_count,
    rank_in_day
from ranked
where rank_in_day <= 5
order by event_date, rank_in_day