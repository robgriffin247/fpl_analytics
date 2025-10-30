with 
source as (
  select
    id::int as team_id,
    short_name::varchar as team_abbreviation,
    strength_attack_home::int as team_strength_home_attack,
    strength_defence_home::int as team_strength_home_defence,
    strength_attack_away::int as team_strength_away_attack,
    strength_defence_away::int as team_strength_away_defence,
    _dlt_load_id::double as _dlt_load_id
  from {{ source("fpl", "teams") }}
),

most_recent_load as (
  select *
  from source 
  where _dlt_load_id = (select max(_dlt_load_id) from source)
)

select * from most_recent_load
