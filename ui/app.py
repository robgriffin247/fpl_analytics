import streamlit as st
import duckdb


# FRONT END =================================================================================
st.header("Analytics")
t1, t2, t3 = st.tabs(["Stats", "Compare", "Fixtures & Standings"])
stats_table = t1.container()
standings_table = t3.container()

# BACK END ==================================================================================
if "df_players" not in st.session_state:
    with duckdb.connect(f"md:{st.secrets['connections']['fpl_analytics']['database']}") as con:
        st.session_state["df_players"] = con.sql("select * from fpl_analytics.core.obt_players").pl()
        st.session_state["df_standings"]= con.sql("select * from fpl_analytics.core.fct_standings").pl()

df_players=st.session_state["df_players"]
df_standings=st.session_state["df_standings"]

stats_table.dataframe(df_players)
standings_table.dataframe(df_standings)