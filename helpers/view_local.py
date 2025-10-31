import duckdb

with duckdb.connect("data/fpl_analytics.duckdb") as con:
    # print(con.sql("select * from core.obt_player_gameweek_stats"))
    print(con.sql("select to_timestamp(max(load_id::double)) from fpl._dlt_loads"))
