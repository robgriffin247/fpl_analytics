with 

source as (
  select
    id::int as gameweek,
    is_current::boolean as is_current,
    _dlt_load_id::double as _dlt_load_id
  from {{ source("fpl", "events") }}
),


-- This identifies the most recent dlt load that occured for each gameweek
latest_load_per_gameweek as (
  select 
    gameweek,
    max(_dlt_load_id) as _dlt_load_id 
  from source 
  where is_current 
  group by 1
  order by gameweek
)

select * from latest_load_per_gameweek
