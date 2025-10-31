with 

source as (
  select
    gameweek,
    home_goals,
    away_goals,
    home_team_abbreviation,
    away_team_abbreviation
  from {{ ref("int_fixtures") }}
),

team_fixtures as (
  select * from {{ref("int_team_fixtures")}}
),

merge_in_home_team_form as (
  select 
    source.*,
    team_fixtures.form_icons as home_team_form,
  from source 
    left join (select * from team_fixtures where home) as team_fixtures
      on source.gameweek=team_fixtures.gameweek and source.home_team_abbreviation=team_fixtures.team
),

merge_in_away_team_form as (
  select 
    merge_in_home_team_form.*,
    team_fixtures.form_icons as away_team_form,
  from merge_in_home_team_form 
    left join (select * from team_fixtures where not home) as team_fixtures
      on merge_in_home_team_form.gameweek=team_fixtures.gameweek 
        and merge_in_home_team_form.home_team_abbreviation=team_fixtures.fixture
)

select * from merge_in_away_team_form