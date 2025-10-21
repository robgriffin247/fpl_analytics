import streamlit as st
import duckdb
import polars as pl

# FRONT END =================================================================================
st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])

with t1:
    # Filters
    c1, c2, c3 = st.columns([4,3,3], gap="large")
    filter_position = c1.container()
    filter_cost = c2.container()
    filter_minutes_per_week = c3.container()
    filter_points_per_week = c3.container()
    # Table
    stats_table = st.container()
    # Column selector


with t2:
    player_selector = st.container()

with t3:
    standings_table = st.container()

# BACK END ==================================================================================
# General
if "df_players" not in st.session_state:
    with duckdb.connect(f"md:{st.secrets['connections']['fpl_analytics']['database']}") as con:
        st.session_state["df_players"] = con.sql("select * from fpl_analytics.core.obt_players").pl()
        st.session_state["df_standings"]= con.sql("select * from fpl_analytics.core.fct_standings").pl()

df_players=st.session_state["df_players"]
df_standings=st.session_state["df_standings"]

costs = [df_players["current_cost"].min(), df_players["current_cost"].max()]
minutes_per_week = [df_players["minutes_played_per_gameweek"].min(), df_players["minutes_played_per_gameweek"].max()]
players = df_players[["name_team", "player_id"]].unique().sort(pl.col("player_id"))["name_team"].to_list()
positions = df_players[["position", "position_id"]].unique().sort(pl.col("position_id"))["position"].to_list()


# T1
selected_cost = filter_cost.slider("Cost (£M)", value=costs, min_value=costs[0], max_value=costs[1], step=0.1)
selected_positions = filter_position.multiselect("Positon(s)", options=positions)
selected_minutes_per_week = filter_minutes_per_week.slider("Mins/Wk", value=minutes_per_week, min_value=minutes_per_week[0], max_value=minutes_per_week[1], step=1)

with duckdb.connect() as con:
    df_player_stats = con.sql(
        f"""
            select * 
            from df_players 
            where 
                current_cost between {selected_cost[0]} and {selected_cost[1]}
                and minutes_played_per_gameweek between {selected_minutes_per_week[0]} and {selected_minutes_per_week[1]}
                and position in {selected_positions if len(selected_positions)>0 else positions}
        """).pl()

stats_table.dataframe(df_player_stats)


# T2
selected_players = player_selector.multiselect("Player(s)", options=players)

with duckdb.connect() as con:
    df_selected_players = (con.sql(
        f"""select * 
            from df_players 
            where name_team in {selected_players}
            """).pl())

# T3
standings_table.dataframe(df_standings)
