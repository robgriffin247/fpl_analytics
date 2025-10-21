import streamlit as st
import duckdb
import polars as pl

# FRONT END =================================================================================
st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])

with t1:
    stats_table = st.container()

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

players = df_players[["name_team", "player_id"]].unique().sort(pl.col("player_id"))["name_team"].to_list()


# T1
stats_table.dataframe(df_players)


# T2
selected_players = player_selector.multiselect("Player(s)", options=players)

with duckdb.connect() as con:
    t2.write(con.sql(f"""select * from df_players where name_team in ('{"','".join(selected_players)}')""").pl())

# T3
standings_table.dataframe(df_standings)