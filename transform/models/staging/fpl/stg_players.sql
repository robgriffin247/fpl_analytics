with 

source as (
  select *
  from {{ source("fpl", "elements") }}
  where _dlt_load_id::double in (select _dlt_load_id from {{ ref("stg_gameweeks") }})
),

remove_non_selectables as (
  select * 
  from source 
  where can_select and not removed and status!='u'
),

select_columns as (
  select
    _dlt_load_id::double as _dlt_load_id,
    id::int as player_id,
    web_name::varchar as player,
    element_type::int as position_id,
    team::int as team_id,
    coalesce(now_cost::int, 0) as cost,
    coalesce(form::float, 0) as form,
    chance_of_playing_next_round::int as availability,
    in_dreamteam::boolean as dreamteam,
    coalesce(dreamteam_count::int, 0) as dreamteam_appearances,
    coalesce(points_per_game::float, 0) as points_appearance,
    coalesce(total_points::int, 0) as points_total,
    coalesce(starts::int, 0) as starts_total,
    coalesce(minutes::int, 0) as minutes_total,
    coalesce(goals_scored::int, 0) as goals_total,
    coalesce(expected_goals::float, 0) as xgoals_total,
    coalesce(assists::int, 0) as assists_total,
    coalesce(expected_assists::float, 0) as xassists_total,
    coalesce(goals_conceded::int, 0) as concedes_total,
    coalesce(expected_goals_conceded::float, 0) as xconcedes_total,
  from remove_non_selectables
)

select * from select_columns
