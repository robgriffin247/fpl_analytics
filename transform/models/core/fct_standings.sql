with 

source as (
  select
    position,
    team,
    team_abbreviation,
    games,
    wins,
    draws,
    losses,
    points,
    goals_for,
    goals_against,
    goal_difference 
  from {{ ref("int_standings") }}
)

select * from source