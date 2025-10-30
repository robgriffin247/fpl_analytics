with

player_gameweek_stats as (
  select
    player_id,
    player,
    team_id,
    position_id,
    cost,
    availability,
    availability_next,
    form,
    xpoints,
    xpoints_next,
    points,
    points_total,
    minutes_total,
    goals_total,
    xgoals_total,
    _dlt_load_id,
  from {{ ref("stg_player_gameweek_stats") }}
),

teams as (
  select
    team_id,
    team_abbreviation
  from {{ ref("stg_teams") }}
),

positions as (
  select
    position_id,
    position
  from {{ ref("stg_positions") }}
),

gameweeks as (
  select
    gameweek,
    _dlt_load_id
  from {{ ref("stg_gameweeks") }}
),

merge_data as (
  select
    player_gameweek_stats.* exclude(team_id, position_id, _dlt_load_id),
    teams.team_abbreviation as team,
    positions.position,
    gameweeks.gameweek
  from player_gameweek_stats
    left join teams using(team_id)
    left join positions using(position_id)
    left join gameweeks using(_dlt_load_id)
),

variable_fixes as (
  select
    player_id,
    replace(player, '''', '') as player,
    team,
    position,
    gameweek,
    coalesce(cost/10, 0) as cost,
    coalesce(availability, 100) as availability,
    coalesce(availability_next, 100) as availability_next,
    coalesce(form, 0) as form,  
    coalesce(xpoints, 0) as xpoints,
    coalesce(xpoints_next, 0) as xpoints_next,
    coalesce(points, 0) as points,
    coalesce(points_total, 0) as points_total,
    coalesce(minutes_total, 0) as minutes_total,
    coalesce(goals_total, 0) as goals_total,
    coalesce(xgoals_total, 0) as xgoals_total
  from merge_data
),

-- some data comes in as totals so need to get lag (will be null in GW6)
week_stats as (
  select
    *,
    minutes_total - lag(minutes_total) over (partition by player_id order by gameweek) as minutes,
    goals_total - lag(goals_total) over (partition by player_id order by gameweek) as goals,
    xgoals_total - lag(xgoals_total) over (partition by player_id order by gameweek) as xgoals,
    form - lag(form) over (partition by player_id order by gameweek) as form_change,
    cost - lag(cost) over (partition by player_id order by gameweek) as cost_change
  from variable_fixes
),

values_per as (
  select
    *,
    (minutes_total/gameweek)::int as minutes_gw,
    points_total/gameweek as points_gw,
    points_total/cost as points_cost,
    points_total/cost/gameweek as points_cost_gw,
    points_total/cost/minutes_total*90 as points_cost_90,
    case when minutes_total=0 then 0 else points_total/minutes_total*90 end as points_90,
    goals_total/gameweek as goals_gw,
    xgoals_total/gameweek as xgoals_gw,
  from week_stats
),

rolling_values as (
  select
    *,
    avg(points) over (partition by player_id order by gameweek rows between 2 preceding and current row) as points_last_3,
    (avg(minutes) over (partition by player_id order by gameweek rows between 2 preceding and current row))::int as minutes_last_3,
  from values_per
),

add_ratios as (
  select
    *,
    case when not isnan(goals/xgoals) then goals/xgoals else 0 end as goals_xratio,
    coalesce(case when not isnan(goals_total/xgoals_total) then goals_total/xgoals_total else 0 end, 0) as goals_total_xratio,
  from rolling_values
),

select_columns as (
  select
    player_id,
    player,
    team,
    position,
    gameweek,
    cost,
    cost_change,
    availability,
    availability_next,
    form,
    form_change,
    minutes,
    minutes_total,
    minutes_last_3,
    minutes_gw,
    xpoints,
    xpoints_next,
    points,
    points_total,
    points_last_3,
    points_gw,
    points_90,
    points_cost,
    points_cost_gw,
    points_cost_90,
    goals,
    goals_total,
    goals_gw,
    xgoals,
    xgoals_total,
    xgoals_gw,
    goals_xratio,
    goals_total_xratio
  from add_ratios
)

select * from select_columns