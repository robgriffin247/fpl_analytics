with 

source as (
  select
    gameweek,
    home_goals,
    away_goals,
    case when home_team_abbreviation='NOT' then 'NFO' else home_team_abbreviation end as home_team_abbreviation,
    case when away_team_abbreviation='NOT' then 'NFO' else away_team_abbreviation end as away_team_abbreviation
  from {{ ref("stg_fixtures") }}
)

select * from source