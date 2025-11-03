import dlt
import httpx
from typing import Iterator, Dict, Any
from datetime import datetime
import os


def load_football_data():
    api_key = os.getenv("FOOTBALL_DATA_API_KEY")
    headers = {"X-Auth-Token": api_key}
    base_url = "https://api.football-data.org/v4/competitions/PL/"

    # Note both tables contain other columns that are not used, so not loaded
    @dlt.resource(name="fixtures", write_disposition="replace")
    def get_fixtures() -> Iterator[Dict[str, Any]]:
        url = f"{base_url}matches"
        response = httpx.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        for match in data.get("matches", []):
            yield {
                "fixture_id": match["id"],
                "season": match["season"]["id"],
                "matchday": match["matchday"],
                "home_team_id": match["homeTeam"]["id"],
                "home_team_name": match["homeTeam"]["name"],
                "home_team_short_name": match["homeTeam"]["shortName"],
                "home_team_tla": match["homeTeam"]["tla"],
                "away_team_id": match["awayTeam"]["id"],
                "away_team_name": match["awayTeam"]["name"],
                "away_team_short_name": match["awayTeam"]["shortName"],
                "away_team_tla": match["awayTeam"]["tla"],
                "home_score_full_time": match["score"]["fullTime"]["home"],
                "away_score_full_time": match["score"]["fullTime"]["away"],
            }

    @dlt.resource(name="standings", write_disposition="replace")
    def get_standings() -> Iterator[Dict[str, Any]]:
        url = f"{base_url}standings"
        response = httpx.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        for standing_type in data.get("standings", []):
            for position in standing_type.get("table", []):
                yield {
                    "season": data["season"]["id"],
                    "position": position["position"],
                    "team_id": position["team"]["id"],
                    "team_name": position["team"]["name"],
                    "team_short_name": position["team"]["shortName"],
                    "team_tla": position["team"]["tla"],
                    "played_games": position["playedGames"],
                    "won": position["won"],
                    "draw": position["draw"],
                    "lost": position["lost"],
                    "points": position["points"],
                    "goals_for": position["goalsFor"],
                    "goals_against": position["goalsAgainst"],
                    "goal_difference": position["goalDifference"],
                }

    @dlt.source
    def football_data_source():
        return [
            get_fixtures(),
            get_standings(),
        ]

    destination = os.getenv("DLT_DESTINATION", "duckdb")

    # Configure destination depending on env.
    if destination == "motherduck":
        dest = dlt.destinations.motherduck(
            credentials={
                "database": "fpl_analytics",
                "motherduck_token": os.environ["MOTHERDUCK_TOKEN"],
            }
        )
    else:
        dest = dlt.destinations.duckdb(credentials="data/fpl_analytics.duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="fpl_analytics__football_data_pipeline",
        destination=dest,
        dataset_name="football_data",
    )

    load_info = pipeline.run(football_data_source())
    return load_info


def load_fpl():
    @dlt.resource(name="fpl_data", write_disposition="append")
    def get_data():
        url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        response = httpx.get(url)
        response.raise_for_status()

        data = response.json()

        for key, value in data.items():
            # Other tables are present, reduced to those that currently get used; it just loads all columns (and nested tables)
            #   this could follow the same approach as above but wanted to keep this data as I might find a use later and will want history
            if isinstance(value, list) and key in [
                "elements",
                "element_types",
                "teams",
                "events",
            ]:
                print(f"> Getting table {key}")
                for item in value:
                    yield dlt.mark.with_table_name(item, key)

    @dlt.source
    def fpl_source():
        return get_data()

    destination = os.getenv("DLT_DESTINATION", "duckdb")

    if destination == "motherduck":
        dest = dlt.destinations.motherduck(
            credentials={
                "database": "fpl_analytics",
                "motherduck_token": os.environ["MOTHERDUCK_TOKEN"],
            }
        )
    else:
        dest = dlt.destinations.duckdb(credentials="data/fpl_analytics.duckdb")

    pipeline = dlt.pipeline(
        pipeline_name="fpl_analytics__fpl_pipeline",
        destination=dest,
        dataset_name="fpl",
    )

    load_info = pipeline.run(fpl_source())
    return load_info


if __name__ == "__main__":
    load_fpl()
    load_football_data()
