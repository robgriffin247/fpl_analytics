with 

source as (
  select
    matchday::int as gameweek,
    home_score_full_time::int as home_goals,
    away_score_full_time::int as away_goals,
    home_team_tla::varchar as home_team_abbreviation,
    away_team_tla::varchar as away_team_abbreviation,
    _dlt_load_id::double as _dlt_load_id
  from {{ source("football_data", "fixtures") }}
)

select * from source
