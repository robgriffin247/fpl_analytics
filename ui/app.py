import streamlit as st
import duckdb
from loaders import load_obt_player_gameweek_stats
import polars as pl


def get_range(column):
    return [column.min(), column.max()]

def create_slider(column, title, hint=None):
    _range = get_range(gameweek_scouting_df[column])
    return st.slider(title, 
                     value=_range, 
                     min_value=_range[0], 
                     max_value=_range[1], 
                     key=f"selected_{column}",
                     help=hint)

def get_latest_stats_df():
    with duckdb.connect() as con:
        df = con.sql(f"""
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
        from gameweek_scouting_df
        where true 
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
        """).pl()
    return df



# FRONTEND ========================================================================================
st.set_page_config(layout="wide", page_title="FPL Analytics")
st.header("Analytics")
st.divider()
c1, c2, c3, c4, c5 = st.columns([4,2,2,2,2], gap="large")
st.html("<br/>")
stats_table = st.container()
st.divider()




# BACKEND =========================================================================================
player_gameweek_stats = load_obt_player_gameweek_stats()

# make dynamic
gameweek_scouting_df = player_gameweek_stats.filter(pl.col("gameweek")==9)

with c1:
    # make dynamic and add select to duck in get_latest_stats_table()
    st.multiselect("Position", options=["GKP", "DEF", "MID", "FWD"])
    st.multiselect("Player", options=["Cash"])
    st.multiselect("Team", options=["AVL"])

with c2:
    create_slider("minutes_gw", "Minutes/wk")
    create_slider("minutes_last_3", "Minutes/wk (last 3)", hint="Average minutes played during the last three gameweeks.")
    create_slider("cost", "Cost")

with c3:
    create_slider("points_gw", "Points/wk")
    create_slider("form", "Points/wk (last 3)", hint="Average points scored during the last three gameweeks. This is player form of Fantasy PL.")
    create_slider("availability_next", "Availability (%)", hint="Likelihood of being available according to Fantasy PL.")

with c4:
    create_slider("points_cost_gw", "Points/£M/wk", hint="Points earned over season, divided by current cost and per gameweek; a measure of value-for-money.")
    create_slider("xgoals_gw", "xGoals/wk", hint="Expected goals per gameweek.")
    create_slider("xpoints_next", "xPoints", hint="Expected points in next fixture.")

with c5:
    create_slider("form_points", "Team Form: Points", hint="Average points per game for the player's team during the last 5 gameweeks.")
    create_slider("form_scored", "Team Form: Scored", hint="Average goals scored per game for the player's team during the last 5 gameweeks.")
    create_slider("form_conceded", "Team Form: Conceded", hint="Average goals conceded per game for the player's team during the last 5 gameweeks.")
    
with stats_table:
    st.dataframe(get_latest_stats_df(),
                 column_config={
                     "player":st.column_config.TextColumn("Player"),
                     "team":st.column_config.TextColumn("Team"),
                     "position":st.column_config.TextColumn("Position"),
                     "cost":st.column_config.NumberColumn("Cost", format="%.1f"),
                     "minutes_gw":st.column_config.NumberColumn("Mins/wk", format="%.0f"),
                     "minutes_last_3":st.column_config.NumberColumn("Mins (3)", format="%.0f"),
                     "points_gw":st.column_config.NumberColumn("Pts/wk", format="%.1f"),
                     "points_last_3":st.column_config.NumberColumn("Pts (3)", format="%.1f"),
                     "points_90":st.column_config.NumberColumn("Pts/90", format="%.1f"),
                     "points_90":st.column_config.NumberColumn("Pts/90", format="%.1f"),
                     "points_cost_gw":st.column_config.NumberColumn("Pts/£M/wk", format="%.1f"),
                     "goals_total":st.column_config.NumberColumn("Goals", format="%.0f"),
                     "goals_gw":st.column_config.NumberColumn("Goals/wk", format="%.2f"),
                     "xgoals_total":st.column_config.NumberColumn("xGoals", format="%.2f"),
                     "xgoals_gw":st.column_config.NumberColumn("xGoals/wk", format="%.2f"),
                     "goals_total_xratio":st.column_config.NumberColumn("xG Conv.", format="%.2f"),
                     "availability_next":st.column_config.NumberColumn("Av. (%)", format="%.0f"),
                     "xpoints_next":st.column_config.NumberColumn("xPts", format="%.1f"),
                     "next_fixtures":st.column_config.TextColumn("Fixtures"),
                     "form_icons":st.column_config.TextColumn("Team Form (TF)"),
                     "next_fixture_form_icons":st.column_config.TextColumn("Opp. Form (OF)"),
                     "form_points":st.column_config.NumberColumn("TF (Pts)", format="%.1f"),
                     "next_fixture_form_points":st.column_config.NumberColumn("OF (Pts)", format="%.1f"),
                     "form_scored":st.column_config.NumberColumn("TF (GF)", format="%.1f"),
                     "next_fixture_form_scored":st.column_config.NumberColumn("OF (GF)", format="%.1f"),
                     "form_conceded":st.column_config.NumberColumn("TF (GA)", format="%.1f"),
                     "next_fixture_form_conceded":st.column_config.NumberColumn("OF (GA)", format="%.1f"),

                     })