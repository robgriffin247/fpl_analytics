import dlt
import httpx
from typing import Iterator, Dict, Any
from pathlib import Path
from datetime import datetime
import os

@dlt.resource(
    name="fixtures", 
    write_disposition="append"
)
def get_pl_fixtures() -> Iterator[Dict[str, Any]]:
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    
    api_key = os.getenv('FOOTBALL_DATA_API_KEY')

    headers = {
        'X-Auth-Token': api_key
    }
        
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    
    for match in data.get('matches', []):
        yield {
            'fixture_id': match['id'],
            'season': match['season']['id'],
            'season_start_date': match['season']['startDate'],
            'season_end_date': match['season']['endDate'],
            'matchday': match['matchday'],
            'stage': match['stage'],
            'group': match.get('group'),
            'utc_date': match['utcDate'],
            'status': match['status'],
            'minute': match.get('minute'),
            'home_team_id': match['homeTeam']['id'],
            'home_team_name': match['homeTeam']['name'],
            'home_team_short_name': match['homeTeam']['shortName'],
            'home_team_tla': match['homeTeam']['tla'],
            'home_team_crest': match['homeTeam']['crest'],
            'away_team_id': match['awayTeam']['id'],
            'away_team_name': match['awayTeam']['name'],
            'away_team_short_name': match['awayTeam']['shortName'],
            'away_team_tla': match['awayTeam']['tla'],
            'away_team_crest': match['awayTeam']['crest'],
            'home_score_full_time': match['score']['fullTime']['home'],
            'away_score_full_time': match['score']['fullTime']['away'],
            'home_score_half_time': match['score']['halfTime']['home'],
            'away_score_half_time': match['score']['halfTime']['away'],
            'winner': match['score'].get('winner'),
            'duration': match['score'].get('duration'),
            'venue': None,  # Not in free tier
            'referees': [],  # Not in free tier
            'extracted_at': datetime.now().isoformat(),
        }


@dlt.resource(name="standings", write_disposition="replace")
def get_pl_standings() -> Iterator[Dict[str, Any]]:
    url = "https://api.football-data.org/v4/competitions/PL/standings"
    
    api_key = os.getenv('FOOTBALL_DATA_API_KEY')
    headers = {'X-Auth-Token': api_key}
    
    response = httpx.get(url, headers=headers)
    response.raise_for_status()
    
    data = response.json()
    
    for standing_type in data.get('standings', []):
        for position in standing_type.get('table', []):
            yield {
                'season': data['season']['id'],
                'stage': standing_type['stage'],
                'type': standing_type['type'],
                'position': position['position'],
                'team_id': position['team']['id'],
                'team_name': position['team']['name'],
                'team_short_name': position['team']['shortName'],
                'team_tla': position['team']['tla'],
                'team_crest': position['team']['crest'],
                'played_games': position['playedGames'],
                'form': position.get('form'),
                'won': position['won'],
                'draw': position['draw'],
                'lost': position['lost'],
                'points': position['points'],
                'goals_for': position['goalsFor'],
                'goals_against': position['goalsAgainst'],
                'goal_difference': position['goalDifference'],
                'extracted_at': datetime.now().isoformat(),
            }


@dlt.source
def football_data_source():
    """DLT source for football-data.org API"""
    return [
        get_pl_fixtures(),
        get_pl_standings(),
    ]


if __name__ == "__main__":
    # Ensure the data directory exists
    Path("./data").mkdir(parents=True, exist_ok=True)
    
    # Create pipeline pointing to local DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="fpl_analytics__football_data_pipeline",
        destination="motherduck",
        dataset_name="football_data",
    )
    
    # Run the pipeline
    load_info = pipeline.run(football_data_source())
    
    # Print load information
    print(load_info)
