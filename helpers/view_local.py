import duckdb

with duckdb.connect("data/fpl_analytics.duckdb") as con:
    print(con.sql("select gameweek, count(*) from core.obt_players group by gameweek"))
    #print(con.sql("select _dlt_load_id, count(*) from core.obt_players group by _dlt_load_id"))
    print(con.sql("select * from fpl._dlt_loads"))
    
with duckdb.connect("data/raw.duckdb") as con:
    print(con.sql("show all tables"))