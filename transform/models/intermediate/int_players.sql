with

-- sources
players as (
  select
    _dlt_load_id,
    player_id,
    player,
    position_id,
    team_id,
    cost,
    form,
    availability,
    dreamteam,
    dreamteam_appearances,
    points_appearance,
    points_total,
    starts_total,
    minutes_total,
    goals_total,
    xgoals_total,
    assists_total,
    xassists_total,
    concedes_total,
    xconcedes_total,
  from {{ ref("stg_players") }}
),

teams as (
  select * 
  from {{ ref("stg_teams") }}
),

positions as (
  select * 
  from {{ ref("stg_positions") }}
),

team_fixtures as (
  select * exclude(gameweek),
    gameweek-1 as gameweek
  from {{ ref("int_team_fixtures") }}
),

gameweeks as (
  select *
  from {{ ref("stg_gameweeks") }}
),

loads as (
  select 
    load_id as _dlt_load_id, 
    inserted_at as load_timestamp 
  from {{ source("fpl", "_dlt_loads")}}
),

-- merge in columns
add_gameweek as (
  select 
    players.*,
    gameweeks.gameweek as gameweek 
  from players
    left join gameweeks using(_dlt_load_id)
  where gameweeks.gameweek is not null

),

add_load_timestamp as (
  select
    add_gameweek.*,
    loads.load_timestamp
  from add_gameweek 
    left join loads using(_dlt_load_id)
),

add_team as (
  select
    add_load_timestamp.*,
    teams.team_abbreviation as team
  from add_load_timestamp 
    left join teams using(team_id, gameweek)
),

add_position as (
  select
    add_team.*,
    positions.position
  from add_team 
    left join positions using(position_id)
),

add_fixtures as (
  select
    add_position.*,
    team_fixtures.previous_opponent as opponent,
    team_fixtures.previous_defending_prospects as defending_bias,
    team_fixtures.previous_attacking_prospects as attacking_bias,
    team_fixtures.fixtures_next,
    team_fixtures.fixtures_next_3,
    team_fixtures.attacking_prospects,
    team_fixtures.defending_prospects,
    team_fixtures.attacking_prospects_icon,
    team_fixtures.defending_prospects_icon,
    team_fixtures.attacking_prospects_next_3,
    team_fixtures.defending_prospects_next_3,
    team_fixtures.attacking_prospects_icon_next_3,
    team_fixtures.defending_prospects_icon_next_3,
    team_fixtures.defending_prospects_icon_next_3 || '/' || team_fixtures.attacking_prospects_icon_next_3 as prospects_icon_next_3
  from add_position 
    left join team_fixtures on add_position.team=team_fixtures.team_abbreviation and add_position.gameweek=team_fixtures.gameweek
),
-- Variable fixes
variable_fixes as (
  select
    * exclude(player, cost, availability, minutes_total),
    replace(player, '''','') as player,
    cost/10 as cost,
    coalesce(availability, 100) as availability,
    coalesce(minutes_total, 0) as minutes_total,
  from add_fixtures
),

-- Derivations
-- -- Appearances
derive_appearances as (
  select
    *,
    case 
      when isnan(points_total / points_appearance) then 0 
      else (points_total / points_appearance)::int end as appearances_total,
  from variable_fixes
),

add_player_team as (
  select 
    *, 
    player || ' [' || team || ']' as player_team,
  from derive_appearances
),

add_minutes_derivations as (
  select
    *,
    minutes_total - lag(minutes_total) over (partition by player_id order by gameweek) as minutes,
    minutes_total / gameweek as minutes_gameweek
  from add_player_team
),

add_points_derivations as (
  select
    *,
    points_total - lag(points_total) over (partition by player_id order by gameweek) as points,
    points_total / gameweek as points_gameweek,
    case when isnan(points_total / minutes_total * 90) then 0 else points_total / minutes_total * 90 end as points_90,
    case when isnan(points_total / cost) then 0 else points_total / cost end as points_cost
  from add_minutes_derivations
),

add_goals_derivations as (
  select
    *,
    goals_total - lag(goals_total) over (partition by player_id order by gameweek) as goals,
    goals_total / gameweek as goals_gameweek,
    case when isnan(goals_total / minutes_total * 90) then 0 else goals_total / minutes_total * 90 end as goals_90,
    case when isnan(goals_total / cost) then 0 else goals_total / cost end as goals_cost
  from add_points_derivations
),

add_xgoals_derivations as (
  select
    *,
    xgoals_total - lag(xgoals_total) over (partition by player_id order by gameweek) as xgoals,
    xgoals_total / gameweek as xgoals_gameweek,
    case when isnan(xgoals_total / minutes_total * 90) then 0 else xgoals_total / minutes_total * 90 end as xgoals_90,
    case when isnan(xgoals_total / cost) then 0 else xgoals_total / cost end as xgoals_cost
  from add_goals_derivations
),
add_assists_derivations as (
  select
    *,
    assists_total - lag(assists_total) over (partition by player_id order by gameweek) as assists,
    assists_total / gameweek as assists_gameweek,
    case when isnan(assists_total / minutes_total * 90) then 0 else assists_total / minutes_total * 90 end as assists_90,
    case when isnan(assists_total / cost) then 0 else assists_total / cost end as assists_cost
  from add_xgoals_derivations
),

add_xassists_derivations as (
  select
    *,
    xassists_total - lag(xassists_total) over (partition by player_id order by gameweek) as xassists,
    xassists_total / gameweek as xassists_gameweek,
    case when isnan(xassists_total / minutes_total * 90) then 0 else xassists_total / minutes_total * 90 end as xassists_90,
    case when isnan(xassists_total / cost) then 0 else xassists_total / cost end as xassists_cost
  from add_assists_derivations
),

add_concedes_derivations as (
  select
    *,
    concedes_total - lag(concedes_total) over (partition by player_id order by gameweek) as concedes,
    concedes_total / gameweek as concedes_gameweek,
    case when isnan(concedes_total / minutes_total * 90) then 0 else concedes_total / minutes_total * 90 end as concedes_90,
    case when isnan(concedes_total / cost) then 0 else concedes_total / cost end as concedes_cost
  from add_xassists_derivations
),

add_xconcedes_derivations as (
  select
    *,
    xconcedes_total - lag(xconcedes_total) over (partition by player_id order by gameweek) as xconcedes,
    xconcedes_total / gameweek as xconcedes_gameweek,
    case when isnan(xconcedes_total / minutes_total * 90) then 0 else xconcedes_total / minutes_total * 90 end as xconcedes_90,
    case when isnan(xconcedes_total / cost) then 0 else xconcedes_total / cost end as xconcedes_cost
  from add_concedes_derivations
),

add_x_ratios as (
  select
    *,
    goals / xgoals as g_xg,
    goals_total / xgoals_total as g_xg_total,
    assists / xassists as a_xa,
    assists_total / xassists_total as a_xa_total,
    concedes / xconcedes as c_xc,
    concedes_total / xconcedes_total as c_xc_total,
  from add_xconcedes_derivations
),

add_form_change as (
  select 
    *,
    form - lag(form) over (partition by player_id order by gameweek) as form_change
  from add_x_ratios
),

add_minutes_available as (
  select 
    *,
    case when availability <100 then coalesce(minutes, 0) else 90 end as minutes_available,
    sum(case when availability <100 then coalesce(minutes, 0) else 90 end) over (partition by player_id order by gameweek) as minutes_available_total,
    minutes / (case when availability <100 then coalesce(minutes, 0) else 90 end) as minutes_played_available,
  from add_form_change
),


add_previous_three_rolling_sums as (
  select
    *,
    sum(minutes) over (partition by player_id order by gameweek rows between 2 preceding and current row) as minutes_last_3,
    sum(minutes_available) over (partition by player_id order by gameweek rows between 2 preceding and current row) as minutes_available_last_3,
    sum(points) over (partition by player_id order by gameweek rows between 2 preceding and current row) as points_last_3,
    sum(form_change) over (partition by player_id order by gameweek rows between 2 preceding and current row) as form_change_last_3,
    sum(goals) over (partition by player_id order by gameweek rows between 2 preceding and current row) as goals_last_3,
    sum(xgoals) over (partition by player_id order by gameweek rows between 2 preceding and current row) as xgoals_last_3,
    sum(defending_prospects) over (partition by player_id order by gameweek rows between 2 preceding and current row) as defending_prospects_last_3,
    sum(attacking_prospects) over (partition by player_id order by gameweek rows between 2 preceding and current row) as attacking_prospects_last_3,
  from add_minutes_available
),

add_minutes_played_rate as (
  select 
    *,
    minutes/minutes_available as minutes_played_available,
    minutes_last_3/minutes_available_last_3 as minutes_played_available_last_3,
  from add_previous_three_rolling_sums
),

add_previous_three_gxg as (
  select
    *,
    goals_last_3/xgoals_last_3 as g_xg_last_3
  from add_minutes_played_rate
),

select_columns as (
  select
    player_id,
    player,
    team_id,
    team,
    --opponent,
    --defending_bias, 
    --attacking_bias,
    player_team,
    position_id,
    position,
    gameweek,
    cost,
    form,
    form_change,
    form_change_last_3,
    dreamteam,
    dreamteam_appearances,
    appearances_total,
    starts_total,
    availability,
    minutes_available,
    minutes_available_total,
    minutes_available_last_3,
    minutes_played_available,
    minutes_played_available_last_3,

    minutes,
    minutes_total,
    minutes_gameweek,
    minutes_last_3,


    points,
    points_total,
    points_gameweek,
    points_90,
    points_cost,
    points_last_3,
    
    goals,
    goals_last_3,
    --goals_gameweek,
    --goals_90,
    --goals_cost,
    goals_total,
    
    xgoals,
    xgoals_last_3,
    --xgoals_gameweek,
    --xgoals_90,
    --xgoals_cost,
    xgoals_total,
    
    g_xg,
    g_xg_total,
    g_xg_last_3,
    
    --assists,
    --assists_gameweek,
    --assists_90,
    --assists_cost,
    --assists_total,

    --xassists,
    --xassists_gameweek,
    --xassists_90,
    --xassists_cost
    --xassists_total,

    --a_xa,
    --a_xa_total,

    --concedes,
    --concedes_gameweek,
    --concedes_90,
    --concedes_cost,
    --concedes_total,

    --xconcedes,
    --xconcedes_gameweek,
    --xconcedes_90,
    --xconcedes_cost,
    --xconcedes_total,

    --c_xc,
    --c_xc_total,

    --fixtures_next,
    fixtures_next_3,
    --attacking_prospects,
    --defending_prospects,
    --attacking_prospects_icon,
    --defending_prospects_icon,
    defending_prospects_last_3,
    attacking_prospects_last_3,
    defending_prospects_next_3,
    attacking_prospects_next_3,
    --attacking_prospects_icon_next_3,
    --defending_prospects_icon_next_3,
    prospects_icon_next_3,

    --_dlt_load_id,
    --load_timestamp,

  from add_previous_three_gxg
)

select * from select_columns
