with

standings as (
  select
    position::int as position,
    team_tla::varchar as team_abbreviation,
    played_games::int as games,
    won::int as wins,
    draw::int as draws,
    lost::int as losses,
    points::int as points,
    goals_for::int as goals_for,
    goals_against::int as goals_against,
    goal_difference::int as goal_difference
  from {{ source("football_data", "standings")}}
)

select * from standings