import duckdb

with duckdb.connect("data/fpl_analytics.duckdb") as con:
    print(con.sql("select * from core.fct_fixtures where gameweek=9"))
    #print(con.sql("select to_timestamp(max(load_id::double)) from fpl._dlt_loads"))
