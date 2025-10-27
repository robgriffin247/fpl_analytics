with 

source as (
  select
    id::int as gameweek,
    is_current::boolean as is_current,
    _dlt_load_id::double as _dlt_load_id
  from {{ source("fpl", "events") }}
),

latest_load_per_gameweek as (
  select 
    gameweek,
    true as most_recent, 
    max(_dlt_load_id) as _dlt_load_id 
  from source 
  where is_current group by 1
),

filter_to_most_recent_per_gameweek as (
  select 
    source.*
  from source
    left join latest_load_per_gameweek using(gameweek, _dlt_load_id)
  where latest_load_per_gameweek.most_recent
    and is_current
)

select * from filter_to_most_recent_per_gameweek
