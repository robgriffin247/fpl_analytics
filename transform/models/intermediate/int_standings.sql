with 

source as (
  select * 
  from {{ ref("stg_standings") }}
)

select * from source