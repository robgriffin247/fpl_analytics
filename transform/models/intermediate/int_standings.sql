with 

source as (
  select
    position,
    team,
    case when team_abbreviation='NOT' then 'NFO' else team_abbreviation end as team_abbreviation,
    games,
    wins,
    draws,
    losses,
    points,
    goals_for,
    goals_against,
    goal_difference
  from {{ ref("stg_standings") }}
)

select * from source