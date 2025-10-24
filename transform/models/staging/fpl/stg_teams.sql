with 
teams as (
    select
        id::int as team_id,
        short_name::varchar as team_abbreviation,
        strength_attack_home::int as team_strength_home_attack,
        strength_defence_home::int as team_strength_home_defence,
        strength_attack_away::int as team_strength_away_attack,
        strength_defence_away::int as team_strength_away_defence,
        _dlt_load_id
    from {{ source("fpl", "teams") }}
),

gameweeks as (
    select *
    from {{ ref("stg_gameweeks") }}
),

latest_load_per_gameweek as (
  select 
    gameweek, 
    max(_dlt_load_id) as _dlt_load_id 
  from gameweeks 
  where is_coming_gameweek group by 1
),

gameweek_to_teams as (
  select 
    teams.*,
    latest_load_per_gameweek.gameweek as gameweek 
  from teams
    left join latest_load_per_gameweek using(_dlt_load_id)
),

select_latest_load as (
  select * 
  from gameweek_to_teams
  where gameweek is not null
)

select * from select_latest_load