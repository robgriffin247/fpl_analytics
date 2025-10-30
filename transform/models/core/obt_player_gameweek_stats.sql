with

stats as (
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
    goals_total_xratio,
  from {{ ref("int_player_gameweek_stats") }}
),

fixtures as (
  select
    gameweek,
    home_goals,
    away_goals,
    home_team_abbreviation,
    away_team_abbreviation
  from {{ ref("int_fixtures") }}
),

home_fixtures as (
  select
    gameweek,
    home_team_abbreviation as team,
    away_team_abbreviation as fixture,
    true as home,
    home_goals as scored,
    away_goals as conceded,
    case 
      when home_goals is null then null
      when home_goals>away_goals then 'W'
      when home_goals<away_goals then 'L'
      when home_goals=away_goals then 'D' else null end as result,
    case 
      when home_goals is null then null
      when home_goals>away_goals then 3
      when home_goals<away_goals then 0
      when home_goals=away_goals then 1 else null end as points
  from fixtures
),

away_fixtures as (
  select
    gameweek,
    away_team_abbreviation as team,
    home_team_abbreviation as fixture,
    false as home,
    away_goals as scored,
    home_goals as conceded,
    case 
      when away_goals is null then null
      when away_goals>home_goals then 'W'
      when away_goals<home_goals then 'L'
      when away_goals=home_goals then 'D' else null end as result,
    case 
      when away_goals is null then null
      when away_goals>home_goals then 3
      when away_goals<home_goals then 0
      when away_goals=home_goals then 1 else null end as points
  from fixtures
),

team_fixtures as (
  select 
    *,
    lag(fixture, -1) over (partition by team order by gameweek) as next_fixture,
  from 
    ((select * from home_fixtures) union all (select * from away_fixtures))
),

rolling_values as (
  select
    *,
    case when scored is not null then listagg(result, '') over (partition by team order by gameweek rows between 4 preceding and 0 preceding) else null end as form_results,
    case when scored is not null then avg(points) over (partition by team order by gameweek rows between 4 preceding and 0 preceding) else null end as form_points,
    case when scored is not null then avg(scored) over (partition by team order by gameweek rows between 4 preceding and 0 preceding) else null end as form_scored,
    case when scored is not null then avg(conceded) over (partition by team order by gameweek rows between 4 preceding and 0 preceding) else null end as form_conceded,
    listagg(next_fixture, '-') over (partition by team order by gameweek rows between current row and 2 following) as next_fixtures
  from team_fixtures
),

merge_fixtures as (
  select
    stats.*,
    team_values.fixture,
    team_values.form_results,
    team_values.form_points,
    team_values.form_scored,
    team_values.form_conceded,
    team_values.next_fixture,
    team_values.next_fixtures
  from stats 
    left join rolling_values as team_values using(team, gameweek)
),

merge_opponent_form as (
  select
    merge_fixtures.*,
    rolling_values.form_results as next_fixture_form_results,
    rolling_values.form_points as next_fixture_form_points,
    rolling_values.form_scored as next_fixture_form_scored,
    rolling_values.form_conceded as next_fixture_form_conceded
  from merge_fixtures left join rolling_values on merge_fixtures.gameweek=rolling_values.gameweek and merge_fixtures.team=rolling_values.next_fixture
),

add_form_icons as (
  select
    *,
    replace(replace(replace(form_results, 'W', ':green_heart:'), 'D', ':yellow_heart:'), 'L', ':heart:') as form_icons,
    replace(replace(replace(next_fixture_form_results, 'W', ':green_heart:'), 'D', ':yellow_heart:'), 'L', ':heart:') as next_fixture_form_icons
  from merge_opponent_form
)

select * from add_form_icons
