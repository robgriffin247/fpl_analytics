import dlt
import httpx
from typing import Iterator, Dict, Any
from pathlib import Path
from datetime import datetime
import os


def load_from_football_data():
    @dlt.resource(
        name="fixtures", 
        write_disposition="append"
    )
    def get_fixtures() -> Iterator[Dict[str, Any]]:
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
    def get_standings() -> Iterator[Dict[str, Any]]:
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
            get_fixtures(),
            get_standings(),
        ]

    pipeline = dlt.pipeline(
        pipeline_name="fpl_analytics__football_data_pipeline",
        destination=dlt.destinations.motherduck(
            credentials={
                "database": "fpl_analytics",
                "motherduck_token": os.environ['MOTHERDUCK_TOKEN']
            }
        ),
        dataset_name="football_data",
    )
    
    load_info = pipeline.run(football_data_source())
    
    return load_info



def load_fpl():
    import os
    
    @dlt.resource(name="fpl_data", write_disposition="append")
    def get_data():
        url = "https://fantasy.premierleague.com/api/bootstrap-static/"
        response = httpx.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    yield dlt.mark.with_table_name(item, key)
            else:
                yield dlt.mark.with_table_name({key: value}, "metadata")

    @dlt.source
    def fpl_source():
        return get_data()

    # Configure destination with credentials dict
    pipeline = dlt.pipeline(
        pipeline_name="fpl_analytics__fpl_pipeline",
        destination=dlt.destinations.motherduck(
            credentials={
                "database": "fpl_analytics",
                "motherduck_token": os.environ['MOTHERDUCK_TOKEN']
            }
        ),
        dataset_name="fpl",
    )

    load_info = pipeline.run(fpl_source())
    return load_info

if __name__=="__main__":
    load_fpl()
    load_from_football_data()