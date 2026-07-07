{{ config(materialized='table') }}

select
    event_date,
    keyword,
    search_count
from {{ source('logsearch_spectrum', 'gold_keyword_trends') }}