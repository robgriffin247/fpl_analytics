import streamlit as st
import duckdb
import polars as pl

from loaders import load_obt_player_gameweek_stats, filter_current_player_stats
from visuals import render_filtered_current_player_stats
from utils import get_sorted_options
from inputs import create_slider


def web_app():

    player_stats = load_obt_player_gameweek_stats()

    with duckdb.connect() as con:
        current_player_stats = con.sql(
            "select * from player_stats where gameweek=(select max(gameweek) from player_stats)"
        ).pl()

    # FRONTEND ========================================================================================
    with duckdb.connect() as con:
        current_gameweek = con.sql(
            "select max(gameweek) as gameweek from player_stats"
        ).pl()["gameweek"].to_list()[0] + 1
        
    st.set_page_config(layout="wide", page_title="FPL Analytics")
    st.header("Analytics")
    t1, t2 = st.tabs([f"Stats: GW{current_gameweek}", "Trends"])
    with t1:
        c1, c2, c3, c4, c5 = st.columns([4, 2, 2, 2, 2], gap="large")
        st.html("<br/>")
        stats_table = st.container()
    with t2:
        trends_df = st.container()

    # BACKEND =========================================================================================

    with c1:
        st.multiselect(
            "Position",
            options=get_sorted_options(current_player_stats, "position"),
            key="selected_position",
        )
        st.multiselect(
            "Player",
            options=get_sorted_options(
                current_player_stats, "player_team", "player_id"
            ),
            key="selected_player_current_stats",
        )
        st.multiselect(
            "Team",
            options=get_sorted_options(current_player_stats, "team"),
            key="selected_team",
        )

    with c2:
        create_slider(current_player_stats, "minutes_gw", "Minutes/wk")
        create_slider(
            current_player_stats,
            "minutes_last_3",
            "Minutes/wk (last 3)",
            hint="Average minutes played during the last three gameweeks.",
        )
        create_slider(current_player_stats, "cost", "Cost")

    with c3:
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

    with c4:
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

    with c5:
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

    with trends_df:
        st.dataframe(player_stats)

if __name__ == "__main__":
    web_app()
