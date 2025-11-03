import duckdb
import os
import httpx
import pprint as pp


def query_local_db(query):
    with duckdb.connect("data/fpl_analytics.duckdb") as con:
        data = con.sql(query)
        if data is not None:
            return data.pl()
        return data
    
def test_football_data():
    api_key = os.getenv("FOOTBALL_DATA_API_KEY")
    headers = {"X-Auth-Token": api_key}
    base_url = "https://api.football-data.org/v4/competitions/PL/"
    url = f"{base_url}standings"
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    return(data)

if __name__=="__main__":
    print(query_local_db("select * from (show all tables) where schema='fpl' or schema='football_data'"))
