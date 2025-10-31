import duckdb
import streamlit as st
import polars as pl
import emoji
import os 

from utils import get_sorted_options

cache_hours = 12


@st.cache_data(
    ttl=cache_hours * 60 * 60,
    max_entries=100,
    show_spinner="Loading data from database...",
)
def load_fct_standings():
    with duckdb.connect(
        f"md:{os.environ['DESTINATION__MOTHERDUCK__DATABASE']}"
    ) as con:
        df = con.sql("select * from core.fct_standings").pl()
    return df

@st.cache_data(
    ttl=cache_hours * 60 * 60,
    max_entries=100,
    show_spinner="Loading data from database...",
)
def load_obt_player_gameweek_stats():
    with duckdb.connect(
        f"md:{os.environ['DESTINATION__MOTHERDUCK__DATABASE']}"
    ) as con:
        return (
            con.sql(
                "select *, player || ' [' || team || ']' as player_team from core.obt_player_gameweek_stats"
            )
            .pl()
            .with_columns(
                pl.col("form_icons").map_elements(
                    lambda x: emoji.emojize(x, language="alias")
                )
            )
            .with_columns(
                pl.col("next_fixture_form_icons").map_elements(
                    lambda x: emoji.emojize(x, language="alias")
                )
            )
        )


def filter_current_player_stats(df):
    with duckdb.connect() as con:
        df = con.sql(
            f"""
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
            points_cost_gw,
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
        from df
        where true 
            and position in {st.session_state["selected_position"] if len(st.session_state["selected_position"])>0 else get_sorted_options(df, "position")}
            and player_team in {st.session_state["selected_player_current_stats"] if len(st.session_state["selected_player_current_stats"])>0 else get_sorted_options(df, "player_team", "player_id")}
            and team in {st.session_state["selected_team"] if len(st.session_state["selected_team"])>0 else get_sorted_options(df, "team")}
            and minutes_gw between {st.session_state["selected_minutes_gw"][0]-0.01} and {st.session_state["selected_minutes_gw"][1]+0.01}
            and minutes_last_3 between {st.session_state["selected_minutes_last_3"][0]-0.01} and {st.session_state["selected_minutes_last_3"][1]+0.01}
            and points_gw between {st.session_state["selected_points_gw"][0]-0.01} and {st.session_state["selected_points_gw"][1]+0.01}
            and availability_next between {st.session_state["selected_availability_next"][0]-0.01} and {st.session_state["selected_availability_next"][1]+0.01}
            and cost between {st.session_state["selected_cost"][0]-0.01} and {st.session_state["selected_cost"][1]+0.01}
            and points_cost_gw between {st.session_state["selected_points_cost_gw"][0]-0.01} and {st.session_state["selected_points_cost_gw"][1]+0.01}
            and form between {st.session_state["selected_form"][0]-0.01} and {st.session_state["selected_form"][1]+0.01}
            and xgoals_gw between {st.session_state["selected_xgoals_gw"][0]} and {st.session_state["selected_xgoals_gw"][1]+0.01}
            and xpoints_next between {st.session_state["selected_xpoints_next"][0]-0.01} and {st.session_state["selected_xpoints_next"][1]+0.01}
            and form_points between {st.session_state["selected_form_points"][0]-0.01} and {st.session_state["selected_form_points"][1]+0.01}
            and form_scored between {st.session_state["selected_form_scored"][0]-0.01} and {st.session_state["selected_form_scored"][1]+0.01}
            and form_conceded between {st.session_state["selected_form_conceded"][0]-0.01} and {st.session_state["selected_form_conceded"][1]+0.01}
        order by position_id, points_cost_gw desc
        """
        ).pl()
    return df
