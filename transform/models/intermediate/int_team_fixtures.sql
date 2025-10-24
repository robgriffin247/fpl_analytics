with

fixtures as (
  select * 
  from {{ ref("stg_fixtures") }}
),

teams as (
  select * 
  from {{ ref("stg_teams") }} 
  where gameweek=(select max(gameweek) from {{ref("stg_teams") }} )
),

fix_nfo as (
  select * exclude(home_team_abbreviation, away_team_abbreviation),
    case when home_team_abbreviation='NOT' then 'NFO' else home_team_abbreviation end as home_team_abbreviation,
    case when away_team_abbreviation='NOT' then 'NFO' else away_team_abbreviation end as away_team_abbreviation,
  from fixtures
),

home_fixtures as (
  select
    gameweek,
    home_team_abbreviation as team_abbreviation,
    away_team_abbreviation as fixtures_next,
    true as home,
  from fix_nfo
)
,

away_fixtures as (
  select
    gameweek,
    away_team_abbreviation as team_abbreviation,
    home_team_abbreviation as fixtures_next,
    false as home,
  from fix_nfo
),

all_team_fixtures as (
  select * from home_fixtures 
  union all
  select * from away_fixtures
),

merge_in_team_strengths as (
  select 
    all_team_fixtures.*,
    case when home then teams.team_strength_home_attack else teams.team_strength_away_attack end as team_attack_strength,
    case when home then teams.team_strength_home_defence else teams.team_strength_away_defence end as team_defence_strength,
    case when home then opponents.team_strength_away_attack else opponents.team_strength_home_attack end as opponent_attack_strength,    
    case when home then opponents.team_strength_away_defence else opponents.team_strength_home_defence end as opponent_defence_strength    
  from all_team_fixtures 
    left join teams using(team_abbreviation)
    left join teams as opponents on all_team_fixtures.fixtures_next=opponents.team_abbreviation
),

rescale_strength as (
  select
    * exclude(team_attack_strength,team_defence_strength,opponent_attack_strength,opponent_defence_strength),
    (team_attack_strength-1000)/100 as team_attack_strength,
    (team_defence_strength-1000)/100 as team_defence_strength,
    (opponent_attack_strength-1000)/100 as opponent_attack_strength,
    (opponent_defence_strength-1000)/100 as opponent_defence_strength,
  from merge_in_team_strengths
),

add_prospects as (
  select 
    *,
    team_attack_strength - opponent_defence_strength as attacking_prospects,
    team_defence_strength - opponent_attack_strength as defending_prospects,
  from rescale_strength
),

add_prospect_categories as (
  select 
    *,
    ntile(5) over (order by attacking_prospects) as attacking_prospects_category,
    ntile(5) over (order by defending_prospects) as defending_prospects_category,
  from add_prospects
),

add_prospect_icons as (
  select
    *,
    case attacking_prospects_category
      when 1 then ':heart:' 
      when 2 then ':orange_heart:' 
      when 3 then ':yellow_heart:' 
      when 4 then ':green_heart:' 
      when 5 then ':blue_heart:' 
      else null end as attacking_prospects_icon,
    case defending_prospects_category
      when 1 then ':heart:' 
      when 2 then ':orange_heart:' 
      when 3 then ':yellow_heart:' 
      when 4 then ':green_heart:' 
      when 5 then ':blue_heart:' 
      else null end as defending_prospects_icon
  from add_prospect_categories
),

add_rolling_average_prospects as (
  select
    *,
    avg(attacking_prospects) over (
      partition by team_abbreviation
      order by gameweek 
      rows between current row and 2 following) as attacking_prospects_next_3,
    avg(defending_prospects) over (
      partition by team_abbreviation
      order by gameweek 
      rows between current row and 2 following) as defending_prospects_next_3
  from add_prospect_icons
),

add_fixtures_next_3 as (
  select
    *,
    string_agg(fixtures_next, '-') over (
      partition by team_abbreviation 
      order by gameweek 
      rows between current row and 2 following) as fixtures_next_3
  from add_rolling_average_prospects
),

add_icons_next_3 as (
  select
    *,
    string_agg(attacking_prospects_icon, '') over (
      partition by team_abbreviation 
      order by gameweek 
      rows between current row and 2 following) as attacking_prospects_icon_next_3,
    string_agg(defending_prospects_icon, '') over (
      partition by team_abbreviation 
      order by gameweek 
      rows between current row and 2 following) as defending_prospects_icon_next_3
  from add_fixtures_next_3
)

select * from add_icons_next_3