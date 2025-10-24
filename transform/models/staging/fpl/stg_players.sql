with 

players as (
    select *
    from {{ source("fpl", "elements") }}
),

gameweeks as (
    select *
    from {{ ref("stg_gameweeks") }}
),

loads as (
  select load_id as _dlt_load_id, inserted_at as load_timestamp from {{ source("fpl", "_dlt_loads")}}
),

latest_load_per_gameweek as (
  select 
    gameweek, 
    max(_dlt_load_id) as _dlt_load_id 
  from gameweeks 
  where is_coming_gameweek group by 1
),

gameweek_to_players as (
  select 
    players.*,
    latest_load_per_gameweek.gameweek as gameweek 
  from players
    left join latest_load_per_gameweek using(_dlt_load_id)
    where latest_load_per_gameweek.gameweek is not null

),

remove_non_selectables as (
    select * 
    from gameweek_to_players 
    where can_select and not removed and status!='u'
),

add_load_timestamp as (
  select
    remove_non_selectables.*,
    loads.load_timestamp
  from remove_non_selectables left join loads using(_dlt_load_id)
),

select_columns as (
    select
        _dlt_load_id::double as _dlt_load_id,
        load_timestamp,
        id::int as player_id,
        web_name::varchar as display_name,
        first_name::varchar as first_name,
        second_name::varchar as second_name,
        element_type::int as position_id,
        team::int as team_id,
        gameweek::int as gameweek,
        now_cost::int as current_cost,
        total_points::int as total_points,
        form::float as current_form,
        status::varchar as availability_code,
        chance_of_playing_next_round::int as availability_percent,
        in_dreamteam::boolean as current_dreamteam,
        dreamteam_count::int as dreamteam_appearances,
        selected_by_percent::float as selection_percent,
        --round(total_points / points_per_game, 0)::int as appearances,
        points_per_game::float as points_per_appearance,
        starts::int as starts,
        minutes::int as minutes_played,
        goals_scored::int as goals_scored,
        expected_goals::float as expected_goals_scored,
        assists::int as goals_assisted,
        expected_assists::float as expected_goals_assisted,
        goals_conceded::int as goals_conceded,
        expected_goals_conceded::float as expected_goals_conceded,
       
    from add_load_timestamp
)

select * from select_columns
