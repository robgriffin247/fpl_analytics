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
def load_obt_player_gameweek_stats():
    with duckdb.connect(f"md:{st.secrets['connections']['fpl_analytics']['database']}") as con:
    #with duckdb.connect(f"data/fpl_analytics.duckdb") as con:
        return con.sql("select * from core.obt_player_gameweek_stats").pl().with_columns(pl.col("form_icons").map_elements(lambda x: emoji.emojize(x, language="alias"))).with_columns(pl.col("next_fixture_form_icons").map_elements(lambda x: emoji.emojize(x, language="alias")))
