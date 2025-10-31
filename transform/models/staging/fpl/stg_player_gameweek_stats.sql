with

source as (
  select
    id::int as player_id,
    web_name::varchar as player,
    team::int as team_id,
    element_type::int as position_id,
    now_cost::int as cost,
    chance_of_playing_this_round::int as availability,
    chance_of_playing_next_round::int as availability_next,
    form::float as form,
    ep_this::float as xpoints,
    ep_next::float as xpoints_next,
    event_points::int as points,
    total_points::int as points_total,
    minutes::int as minutes_total,
    goals_scored::int as goals_total,
    expected_goals::float as xgoals_total,
    assists::int as assists_total,
    expected_assists::float as xassists_total,
    goals_conceded::int as conceded_total,
    clean_sheets::int as clean_sheets_total,
    defensive_contribution::int as defensive_contributions_total,
    bonus::int as bonus_total,
    _dlt_load_id::double as _dlt_load_id,
  from {{ source("fpl", "elements") }}
  where status != 'u'
),

most_recent_load_per_gameweek as (
  select
    *
  from source
  where _dlt_load_id in (select _dlt_load_id from {{ref("stg_gameweeks")}} ) 
)

select * from most_recent_load_per_gameweek