import streamlit as st
import duckdb
import polars as pl

from loaders import load_obt_player_gameweek_stats, filter_current_player_stats, load_fct_standings
from visuals import render_filtered_current_player_stats, render_standings
from utils import get_sorted_options
from inputs import create_slider


# def web_app():


# FRONTEND ========================================================================================
st.set_page_config(layout="wide", page_title="FPL Analytics")
st.header("FPL Analytics")

player_stats = load_obt_player_gameweek_stats()
with duckdb.connect() as con:
    current_gameweek = (
        con.sql("select max(gameweek) as gameweek from player_stats")
        .pl()["gameweek"]
        .to_list()[0]
        + 1
    )

t1, t2 = st.tabs([f"Stats: GW{current_gameweek}", "Standings & Fixtures"])
with t1:
    t1c1, t1c2, t1c3, t1c4, t1c5 = st.columns([4, 2, 2, 2, 2], gap="large")
    st.html("<br/>")
    stats_table = st.container()
    footer_container = st.container()

with t2:
    t2c1, t2c2 = st.columns([6,3])
    standings_table = t2c1.container()

# DYNAMIC BACKEND =================================================================================
with duckdb.connect() as con:
    current_player_stats = con.sql(
        "select * from player_stats where gameweek=(select max(gameweek) from player_stats)"
    ).pl()

with t1c1:
    st.multiselect(
        "Position",
        options=get_sorted_options(current_player_stats, "position"),
        key="selected_position",
    )
    st.multiselect(
        "Player",
        options=get_sorted_options(current_player_stats, "player_team", "player_id"),
        key="selected_player_current_stats",
    )
    st.multiselect(
        "Team",
        options=get_sorted_options(current_player_stats, "team"),
        key="selected_team",
    )

with t1c2:
    create_slider(current_player_stats, "minutes_gw", "Minutes/wk")
    create_slider(
        current_player_stats,
        "minutes_last_3",
        "Minutes/wk (last 3)",
        hint="Average minutes played during the last three gameweeks.",
    )
    create_slider(current_player_stats, "cost", "Cost")

with t1c3:
    create_slider(current_player_stats, "points_gw", "Points/wk")
    create_slider(
        current_player_stats,
        "form",
        "Points/wk (last 3)",
        hint="Average points scored during the last three gameweeks. This is player form of Fantasy PL.",
    )
    create_slider(
        current_player_stats,
        "availability_next",
        "Availability (%)",
        hint="Likelihood of being available according to Fantasy PL.",
        step=25,
    )

with t1c4:
    create_slider(
        current_player_stats,
        "points_cost_gw",
        "Points/£M/wk",
        hint="Points earned over season, divided by current cost and per gameweek; a measure of value-for-money.",
    )
    create_slider(
        current_player_stats,
        "xgoals_gw",
        "xGoals/wk",
        hint="Expected goals per gameweek.",
    )
    create_slider(
        current_player_stats,
        "xpoints_next",
        "xPoints",
        hint="Expected points in next fixture.",
    )

with t1c5:
    create_slider(
        current_player_stats,
        "form_points",
        "Team Form: Points",
        hint="Average points per game for the player's team during the last 5 gameweeks.",
    )
    create_slider(
        current_player_stats,
        "form_scored",
        "Team Form: Scored",
        hint="Average goals scored per game for the player's team during the last 5 gameweeks.",
    )
    create_slider(
        current_player_stats,
        "form_conceded",
        "Team Form: Conceded",
        hint="Average goals conceded per game for the player's team during the last 5 gameweeks.",
    )

filtered_current_player_stats = filter_current_player_stats(current_player_stats)

with stats_table:
    render_filtered_current_player_stats(filtered_current_player_stats)

with footer_container.expander("Guide"):

    st.markdown("""
    - Mins: Minutes played
    - Pts: Points scored
    - .../wk: Gameweek average of metric over the season
    - ... (last 3): Gameweek average of metric over the last three gameweeks 
    - xGoals: Expected goals
    - xG Conv.: Expected goal conversion rate (Goals / xGoals)
    - Av. (%): Availability for the coming gameweek, as % likelihood available
    - xPts: Expected points in coming gameweek (tightly linked to Pts (3), which is player form)
    - TF: Team form over last 5 gameweeks, average of metric
    - OF: Opponent form over last 5 gameweeks, average of metric
    - ... (GF): Goals scored by the team/opponent
    - ... (GA): Goals conceded by the team/opponent
    """)

# with trends_df:
#     st.dataframe(player_stats)


with standings_table:
    render_standings(load_fct_standings())