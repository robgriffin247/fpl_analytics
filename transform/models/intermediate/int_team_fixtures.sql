with 

source as (
    select * from {{ ref("int_fixtures") }}
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
  from source
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
  from source
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

add_form_icons as (
  select
    *,
    replace(replace(replace(form_results, 'W', ':green_heart:'), 'D', ':yellow_heart:'), 'L', ':heart:') as form_icons
  from rolling_values
)


select * from add_form_icons