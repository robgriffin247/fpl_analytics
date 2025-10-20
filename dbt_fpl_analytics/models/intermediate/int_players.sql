with 
players as (
    select
        _dlt_load_id,
        player_id,
        display_name,
        first_name,
        second_name,
        position_id,
        team_id,
        gameweek,
        current_cost,
        total_points,
        current_form,
        availability_code,
        availability_percent,
        current_dreamteam,
        dreamteam_appearances,
        selection_percent,
        points_per_appearance,
        starts,
        minutes_played,
        goals_scored,
        expected_goals_scored,
        goals_assisted,
        expected_goals_assisted,
        goals_conceded,
        expected_goals_conceded,
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
    select *
    from {{ ref("int_team_fixtures") }}
),

variable_fixes as (
    select
        * exclude(display_name, current_cost, availability_percent, minutes_played),
        replace(display_name, '''','') as display_name,
        current_cost/10 as current_cost,
        coalesce(availability_percent, 100) as availability_percent,
        coalesce(minutes_played, 0) as minutes_played,
    from players
),

add_appearances as (
    select
        *,
        case when isnan(total_points / points_per_appearance) then 0 else total_points / points_per_appearance end as appearances,
    from variable_fixes
),

new_variables as (
    select
        * exclude(points_per_appearance),
        first_name || ' ' || second_name as full_name,
        case when isnan(minutes_played / (gameweek-1)) then 0 else minutes_played / (gameweek-1) end as minutes_played_per_gameweek,
        case availability_code
            when 'a' then 'Available'
            when 'i' then 'Injured'
            when 'd' then 'Doubtful (' || coalesce(availability_percent, 100)::varchar || '%)'
            when 's' then 'Suspended'
            else 'Unknown Code (' || availability_percent::varchar || '%)' end as availability_status,
        case when isnan(total_points / appearances) then 0 else total_points / appearances end as points_per_appearance,
        case when isnan(total_points / minutes_played * 90) then 0 else total_points / minutes_played * 90 end as points_per_90,
        case when isnan(total_points / (gameweek-1)) then 0 else total_points / (gameweek-1) end as points_per_gameweek,
        case when isnan(total_points / current_cost) then 0 else total_points / current_cost end as points_per_cost,
        case when isnan(goals_scored / minutes_played * 90) then 0 else goals_scored / minutes_played * 90 end as goals_scored_per_90,
        case when isnan(goals_assisted / minutes_played * 90) then 0 else goals_assisted / minutes_played * 90 end as goals_assisted_per_90,
        case when isnan(goals_conceded / minutes_played * 90) then 0 else goals_conceded / minutes_played * 90 end as goals_conceded_per_90,
        case when isnan(expected_goals_scored / minutes_played * 90) then 0 else expected_goals_scored / minutes_played * 90 end as expected_goals_scored_per_90,
        case when isnan(expected_goals_assisted / minutes_played * 90) then 0 else expected_goals_assisted / minutes_played * 90 end as expected_goals_assisted_per_90,
        case when isnan(expected_goals_conceded / minutes_played * 90) then 0 else expected_goals_conceded / minutes_played * 90 end as expected_goals_conceded_per_90,
        case when isnan(goals_scored / expected_goals_scored) then 0 else goals_scored / expected_goals_scored end as xg_ratio,
        case when isnan(goals_assisted / expected_goals_assisted) then 0 else goals_assisted / expected_goals_assisted end as xa_ratio,
        case when isnan(goals_conceded / expected_goals_conceded) then 0 else goals_conceded / expected_goals_conceded end as xc_ratio,
    from add_appearances
),

decode_team as (
    select
        new_variables.*,
        teams.team_abbreviation as team
    from new_variables left join teams using(team_id, gameweek)
),

decode_position as (
    select
        decode_team.*,
        positions.position
    from decode_team left join positions using(position_id)
),

add_fixtures as (
    select
        decode_position.*,
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
    from
        decode_position 
            left join team_fixtures
                on decode_position.team=team_fixtures.team_abbreviation
                    and decode_position.gameweek=team_fixtures.gameweek
),

add_name_with_team as (
    select 
        *, 
        display_name || ' [' || team || ']' as name_team,
    from add_fixtures
),

select_columns as (
    select
        _dlt_load_id,
        gameweek,
        player_id,
        position_id,
        team_id,
        availability_code,
        availability_percent,

        display_name,
        full_name,
        name_team,
        team,
        position,
        current_cost,
        current_form,
        availability_status,
        appearances,
        starts,
        minutes_played,
        minutes_played_per_gameweek, 
        total_points,
        points_per_appearance
        points_per_90,
        points_per_gameweek,
        points_per_cost,
        points_per_appearance,
        current_dreamteam,
        dreamteam_appearances,
        selection_percent,
        goals_scored,
        goals_scored_per_90,
        expected_goals_scored,
        expected_goals_scored_per_90,
        xg_ratio,
        goals_assisted,
        goals_assisted_per_90,
        expected_goals_assisted,
        expected_goals_assisted_per_90,
        goals_conceded,
        goals_conceded_per_90,
        expected_goals_conceded,
        expected_goals_conceded_per_90,
        fixtures_next,
        attacking_prospects,
        defending_prospects,
        attacking_prospects_icon,
        defending_prospects_icon,
        fixtures_next_3,
        attacking_prospects_next_3,
        defending_prospects_next_3,
        attacking_prospects_icon_next_3,
        defending_prospects_icon_next_3,
        prospects_icon_next_3
    from add_name_with_team
)

select * from select_columns
