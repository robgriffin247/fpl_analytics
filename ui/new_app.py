import streamlit as st
import duckdb
from loaders import load_obt_player_gameweek_stats
import polars as pl

st.set_page_config(layout="wide", page_title="FPL Analytics")

player_gameweek_stats = load_obt_player_gameweek_stats()




# make dynamic
gameweek_scouting_df = player_gameweek_stats.filter(pl.col("gameweek")==9)





st.header("Analytics")


def get_range(column):
    return [column.min(), column.max()]

stats_table = st.container()
c1, c2, c3, c4, c5 = st.columns([4,2,2,2,2], gap="large")

def create_slider(column, title, hint=None):
    _range = get_range(gameweek_scouting_df[column])
    return st.slider(title, 
                     value=_range, 
                     min_value=_range[0], 
                     max_value=_range[1], 
                     key=f"selected_{column}",
                     help=hint)

with c1:
    st.multiselect("Position", options=["GKP", "DEF", "MID", "FWD"])
    st.multiselect("Player", options=["Cash"])
    st.multiselect("Team", options=["AVL"])
    
with c2:
    create_slider("minutes_gw", "Minutes/GW")
    create_slider("minutes_last_3", "Minutes/GW (last 3)", hint="Average minutes played during the last three gameweeks.")
    create_slider("cost", "Cost")

with c3:
    create_slider("points_gw", "Points/GW")
    create_slider("form", "Points/GW (last 3)", hint="Average points scored during the last three gameweeks. This is player form of Fantasy PL.")
    create_slider("availability_next", "Availability (%)", hint="Likelihood of being available according to Fantasy PL.")

with c4:
    create_slider("points_cost", "Points/£M", hint="Points earned over season, divided by current cost; a measure of value-for-money.")
    create_slider("xgoals_gw", "xGoals/GW", hint="Expected goals per gameweek.")
    create_slider("xpoints_next", "xPoints", hint="Expected points in next fixture.")

with c5:
    create_slider("form_points", "Team Form: Points", hint="Average points per game for the player's team during the last 5 gameweeks.")
    create_slider("form_scored", "Team Form: Scored", hint="Average goals scored per game for the player's team during the last 5 gameweeks.")
    create_slider("form_conceded", "Team Form: Conceded", hint="Average goals conceded per game for the player's team during the last 5 gameweeks.")
    


with duckdb.connect() as con:
    filtered_gameweek_scouting_df = con.sql(f"""
    select 
        player,
        team,
        position,
        cost,
        minutes_gw,
        minutes_last_3,
        points_gw,
        points_last_3,
        points_90,
        points_cost,
        goals_total,
        goals_gw,
        xgoals_total,
        xgoals_gw,
        goals_total_xratio,
        availability_next,
        xpoints_next,
        next_fixtures,
        form_icons,
        next_fixture_form_icons,
        form_points,
        next_fixture_form_points,
        form_scored,
        next_fixture_form_scored,
        form_conceded,
        next_fixture_form_conceded
    from gameweek_scouting_df
    where true 
        and minutes_gw between {st.session_state["selected_minutes_gw"][0]-0.01} and {st.session_state["selected_minutes_gw"][1]+0.01}
        and minutes_last_3 between {st.session_state["selected_minutes_last_3"][0]-0.01} and {st.session_state["selected_minutes_last_3"][1]+0.01}
        and points_gw between {st.session_state["selected_points_gw"][0]-0.01} and {st.session_state["selected_points_gw"][1]+0.01}
        and availability_next between {st.session_state["selected_availability_next"][0]-0.01} and {st.session_state["selected_availability_next"][1]+0.01}
        and cost between {st.session_state["selected_cost"][0]-0.01} and {st.session_state["selected_cost"][1]+0.01}
        and points_cost between {st.session_state["selected_points_cost"][0]-0.01} and {st.session_state["selected_points_cost"][1]+0.01}
        and form between {st.session_state["selected_form"][0]-0.01} and {st.session_state["selected_form"][1]+0.01}
        and xgoals_gw between {st.session_state["selected_xgoals_gw"][0]} and {st.session_state["selected_xgoals_gw"][1]+0.01}
        and xpoints_next between {st.session_state["selected_xpoints_next"][0]-0.01} and {st.session_state["selected_xpoints_next"][1]+0.01}
        and form_points between {st.session_state["selected_form_points"][0]-0.01} and {st.session_state["selected_form_points"][1]+0.01}
        and form_scored between {st.session_state["selected_form_scored"][0]-0.01} and {st.session_state["selected_form_scored"][1]+0.01}
        and form_conceded between {st.session_state["selected_form_conceded"][0]-0.01} and {st.session_state["selected_form_conceded"][1]+0.01}
    """).pl()


stats_table.dataframe(filtered_gameweek_scouting_df)