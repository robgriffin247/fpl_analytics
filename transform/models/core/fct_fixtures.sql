with 

source as (
  select
    gameweek,
    home_goals,
    away_goals,
    home_team_abbreviation,
    away_team_abbreviation
  from {{ ref("int_fixtures") }}
)

select * from source