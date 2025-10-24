with 

source as (
  select
    id::int as position_id,
    plural_name_short::varchar as position,
    _dlt_load_id::double as _dlt_load_id
  from {{ source("fpl", "element_types") }}
),

most_recent as (
  select
    position_id,
    position
  from source 
  where _dlt_load_id = (select max(_dlt_load_id) from source)
)

select * from most_recent
