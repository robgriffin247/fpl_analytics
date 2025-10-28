import streamlit as st
import duckdb
import polars as pl
from loaders import load_obt_players_df, load_gameweek_players_df
from visuals import render_stats_table, goal_xg_plot

# FRONT END =================================================================================
st.set_page_config(layout="wide", page_title="FPL Analytics")

st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])


with t2:
    player_selector = st.container()

with t3:
    standings_table = st.container()

# BACK END ==================================================================================
# General
obt_players_df = load_obt_players_df()
gameweek_players_df = load_gameweek_players_df(obt_players_df)

def get_range(column):
    return [gameweek_players_df[column].min(), gameweek_players_df[column].max()]

def get_options(column, sort=None):
    if sort!=None:
        return gameweek_players_df[[column, sort]].unique().sort(pl.col(sort))[column].to_list()
    
    return gameweek_players_df[[column]].unique().sort(pl.col(column))[column].to_list()

position_options = get_options("position", "position_id")
player_options = get_options("player_team", "player_id")
team_options = get_options("team")

cost_range = get_range("cost")
form_range = get_range("form")
availability_range = get_range("availability")

appearances_range = get_range("appearances_total")
starts_range = get_range("starts_total")
minutes_gameweek_range = get_range("minutes_gameweek")

points_gameweek_range = get_range("points_gameweek")
defending_prospects_range = get_range("defending_prospects_next_3")
attacking_prospects_range = get_range("attacking_prospects_next_3")


# T1
with t1:
    
    t1_c1, t1_c2, t1_c3, t1_c4 = st.columns([5,3,3,3], gap="large")

    selected_positions = t1_c1.multiselect("Positon(s)", options=position_options)
    selected_players_t1 = t1_c1.multiselect("Player(s)", options=player_options, key="selected_players_t1")
    selected_teams = t1_c1.multiselect("Team(s)", options=team_options)

    selected_cost = t1_c2.slider("Cost (£M)", value=cost_range, min_value=cost_range[0], max_value=cost_range[1], step=0.1)
    selected_form = t1_c2.slider("Form", value=form_range, min_value=form_range[0], max_value=form_range[1], step=0.1)
    selected_availability = t1_c2.slider("Availability", value=availability_range, min_value=availability_range[0], max_value=availability_range[1], step=25)

    selected_appearances = t1_c3.slider("Appearances", value=appearances_range, min_value=appearances_range[0], max_value=appearances_range[1], step=1)
    selected_starts = t1_c3.slider("Starts", value=starts_range, min_value=starts_range[0], max_value=starts_range[1], step=1)
    selected_minutes_gameweek = t1_c3.slider("Mins/GW", value=minutes_gameweek_range, min_value=minutes_gameweek_range[0], max_value=minutes_gameweek_range[1], step=1.0)

    selected_defending_prospects = t1_c4.slider("Def. Prospects", value=defending_prospects_range, min_value=defending_prospects_range[0], max_value=defending_prospects_range[1], step=1.0)
    selected_attacking_prospects = t1_c4.slider("Att. Prospects", value=attacking_prospects_range, min_value=attacking_prospects_range[0], max_value=attacking_prospects_range[1], step=1.0)
    selected_points_gameweek = t1_c4.slider("Pts/GW", value=points_gameweek_range, min_value=points_gameweek_range[0], max_value=points_gameweek_range[1], step=1.0)

    st.divider()

    with duckdb.connect() as con:
        filtered_gameweek_players_df = con.sql(
            f"""
                select 
                    player_team, 
                    position,
                    cost,
                    --availability,
                    form,
                    form_change_last_3,
                    --dreamteam,
                    --dreamteam_appearances,
                    minutes_total,
                    minutes_last_3,
                    minutes_gameweek,
                    points_total,
                    points_last_3,
                    points_gameweek,
                    points_90,
                    points_cost,
                    goals_total,
                    --goals_last_3,
                    --xgoals_last_3,
                    xgoals_total,
                    g_xg_total,
                    --g_xg_last_3,
                    fixtures_next_3,
                    prospects_icon_next_3,
                    defending_prospects_next_3,
                    attacking_prospects_next_3,


                from gameweek_players_df 
                where true 
                    and position in {selected_positions if len(selected_positions)>0 else position_options}
                    and player_team in {selected_players_t1 if len(selected_players_t1)>0 else player_options}
                    and team in {selected_teams if len(selected_teams)>0 else team_options}
                    and cost between {selected_cost[0]} and {selected_cost[1]}
                    and form between {selected_form[0]} and {selected_form[1]}
                    and availability between {selected_availability[0]} and {selected_availability[1]}
                    and appearances_total between {selected_appearances[0]} and {selected_appearances[1]}
                    and starts_total between {selected_starts[0]} and {selected_starts[1]}
                    and minutes_gameweek between {selected_minutes_gameweek[0]} and {selected_minutes_gameweek[1]}
                    and defending_prospects_next_3 between {selected_defending_prospects[0]} and {selected_defending_prospects[1]}
                    and attacking_prospects_next_3 between {attacking_prospects_range[0]} and {attacking_prospects_range[1]}
                    and points_gameweek between {selected_points_gameweek[0]} and {selected_points_gameweek[1]}

            """).pl()

    render_stats_table(filtered_gameweek_players_df)

    c1, c2, c3, c4 = st.columns(4)
    c1.plotly_chart(goal_xg_plot(filtered_gameweek_players_df))




# T2
selected_players_t2 = player_selector.multiselect("Player(s)", options=player_options, max_selections=5, key="selected_players_t2")

with duckdb.connect() as con:
    filtered_players_df = (con.sql(
        f"""select * 
            from obt_players_df 
            where player_team in {selected_players_t2}
            """).pl())

with t2:
    if filtered_players_df.shape[0] > 0:
        st.dataframe(filtered_players_df)

    

