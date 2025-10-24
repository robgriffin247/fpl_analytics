import duckdb
import streamlit as st
import polars as pl
import emoji

cache_hours = 12

@st.cache_data(
    ttl=cache_hours*60*60,
    max_entries=100,
    show_spinner="Loading data from database..."
)
def load_obt_players_df():
    with duckdb.connect(f"md:{st.secrets['connections']['fpl_analytics']['database']}") as con:
        return con.sql("select * from fpl_analytics.core.obt_players").pl().with_columns(pl.col("prospects_icon_next_3").map_elements(lambda x: emoji.emojize(x, language="alias")))
  


def load_gameweek_players_df(data):
    with duckdb.connect("") as con:
        return con.sql(f"select * from data where gameweek=(select max(gameweek) from data)").pl()