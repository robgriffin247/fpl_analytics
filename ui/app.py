import streamlit as st
import duckdb
import polars as pl
from loaders import load_obt_players_df, load_gameweek_players_df

# FRONT END =================================================================================
st.set_page_config(layout="wide", page_title="FPL Analytics")

st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])

with t1:
    # Filters
    filter_c1, filter_c2, filter_c3, filter_c4 = st.columns([5,3,3,3], gap="large")
    # Table
    stats_table = st.container()
    # Column selector


with t2:
    player_selector = st.container()

with t3:
    standings_table = st.container()

# BACK END ==================================================================================
# General
obt_players_df = load_obt_players_df()
gameweek_players_df = load_gameweek_players_df(obt_players_df)


availabilities = [gameweek_players_df["availability_percent"].min(), gameweek_players_df["availability_percent"].max()]
costs = [gameweek_players_df["current_cost"].min(), gameweek_players_df["current_cost"].max()]
forms = [gameweek_players_df["current_form"].min(), gameweek_players_df["current_form"].max()]
minutes_per_week = [gameweek_players_df["minutes_played_per_gameweek"].min(), gameweek_players_df["minutes_played_per_gameweek"].max()]
players = gameweek_players_df[["name_team", "player_id"]].unique().sort(pl.col("player_id"))["name_team"].to_list()
positions = gameweek_players_df[["position", "position_id"]].unique().sort(pl.col("position_id"))["position"].to_list()
teams = gameweek_players_df["team"].unique().sort().to_list()
xg_per_90 = [gameweek_players_df["expected_goals_scored_per_90"].min(), gameweek_players_df["expected_goals_scored_per_90"].max()]


# T1
selected_positions = filter_c1.multiselect("Positon(s)", options=positions)
selected_players_t1 = filter_c1.multiselect("Player(s)", options=players, key="players_t1")
selected_teams = filter_c1.multiselect("Team(s)", options=teams)

selected_cost = filter_c2.slider("Cost (£M)", value=costs, min_value=costs[0], max_value=costs[1], step=0.1)
selected_form = filter_c2.slider("Form", value=forms, min_value=forms[0], max_value=forms[1], step=0.1)
selected_availability = filter_c2.slider("Availability", value=availabilities, min_value=availabilities[0], max_value=availabilities[1], step=25)

selected_minutes_per_week = filter_c3.slider("Mins/GW", value=minutes_per_week, min_value=minutes_per_week[0], max_value=minutes_per_week[1], step=1)

selected_xg_per_90 = filter_c4.slider("xG/90", value=xg_per_90, min_value=xg_per_90[0], max_value=xg_per_90[1], step=0.01)

with duckdb.connect() as con:
    filtered_gameweek_players_df = con.sql(
        f"""
            select 
                name_team, 
                position,
                current_cost,
                current_form,
                availability_percent,
                appearances,
                minutes_played,
                minutes_played_per_gameweek,
                total_points,
                points_per_gameweek,
                points_per_cost,
                goals_scored,
                xg_ratio,
                fixtures_next_3,
                prospects_icon_next_3,
                defending_prospects_next_3,
                attacking_prospects_next_3,
            from gameweek_players_df 
            where true 
                and position in {selected_positions if len(selected_positions)>0 else positions}
                and name_team in {selected_players_t1 if len(selected_players_t1)>0 else players}
                and team in {selected_teams if len(selected_teams)>0 else teams}
                and current_cost between {selected_cost[0]} and {selected_cost[1]}
                and current_form between {selected_form[0]} and {selected_form[1]}
                and availability_percent between {selected_availability[0]} and {selected_availability[1]}
                and minutes_played_per_gameweek between {selected_minutes_per_week[0]} and {selected_minutes_per_week[1]}
                and expected_goals_scored_per_90 between {selected_xg_per_90[0]} and {selected_xg_per_90[1]}
        """).pl()


stats_table.dataframe(filtered_gameweek_players_df,
                      column_config={
                          "name_team": st.column_config.TextColumn("Player"),
                          "position": st.column_config.TextColumn("Pos"),
                          "current_cost": st.column_config.NumberColumn("Cost", format="%.1f"),
                          "current_form": st.column_config.NumberColumn("Form", format="%.1f"),
                          "availability_percent": st.column_config.NumberColumn("Av. %", format="%.0f"),
                          "appearances": st.column_config.NumberColumn("Apps", format="%.0f"),
                          "minutes_played": st.column_config.NumberColumn("Mins", format="%.0f"),
                          "minutes_played_per_gameweek": st.column_config.NumberColumn("/GW", format="%.0f"),
                          "total_points": st.column_config.NumberColumn("Pts", format="%.0f"),
                          "points_per_gameweek": st.column_config.NumberColumn("/GW", format="%.1f"),
                          "points_per_cost": st.column_config.NumberColumn("/£M", format="%.1f"),
                          "goals_scored": st.column_config.NumberColumn("Gls", format="%.0f"),
                          "xg_ratio": st.column_config.NumberColumn("G/xG", format="%.2f"),
                          "fixtures_next_3": st.column_config.TextColumn("Fixtures"),
                          "prospects_icon_next_3": st.column_config.TextColumn("Prospects"),
                          "defending_prospects_next_3": st.column_config.NumberColumn("Def. Prs", format="%.2f"),
                          "attacking_prospects_next_3": st.column_config.NumberColumn("Att. Prs", format="%.2f"),

                      })


# T2
selected_players = player_selector.multiselect("Player(s)", options=players, max_selections=5)

with duckdb.connect() as con:
    filtered_players_df = (con.sql(
        f"""select * 
            from obt_players_df 
            where name_team in {selected_players}
            """).pl())

with t2:
    if filtered_players_df.shape[0] > 0:
        st.dataframe(filtered_players_df)


# T3
