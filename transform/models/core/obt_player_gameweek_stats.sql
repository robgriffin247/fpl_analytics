with

stats as (
  select
    player_id,
    player,
    team_id,
    team,
    position_id,
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
    goals_total_xratio,
  from {{ ref("int_player_gameweek_stats") }}
),

team_fixtures as (
  select *
  from {{ ref("int_team_fixtures") }}
),

merge_fixtures as (
  select
    stats.*,
    team_fixtures.fixture,
    team_fixtures.form_results,
    team_fixtures.form_icons,
    team_fixtures.form_points,
    team_fixtures.form_scored,
    team_fixtures.form_conceded,
    team_fixtures.next_fixture,
    team_fixtures.next_fixtures
  from stats 
    left join team_fixtures using(team, gameweek)
),

merge_opponent_form as (
  select
    merge_fixtures.*,
    team_fixtures.form_results as next_fixture_form_results,
    team_fixtures.form_icons as next_fixture_form_icons,
    team_fixtures.form_points as next_fixture_form_points,
    team_fixtures.form_scored as next_fixture_form_scored,
    team_fixtures.form_conceded as next_fixture_form_conceded
  from merge_fixtures left join team_fixtures on merge_fixtures.gameweek=team_fixtures.gameweek and merge_fixtures.team=team_fixtures.next_fixture
)

select * from merge_opponent_form
