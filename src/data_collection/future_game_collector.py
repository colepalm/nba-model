import json
import pandas as pd

from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(game_date):
    """
    Fetches games scheduled for a specific date in the season.

    :param game_date: The date for which to fetch games (e.g., '2023-10-22').
    :return: DataFrame with games scheduled on the specified date.
    """
    game_date_formatted = pd.to_datetime(game_date).strftime('%m/%d/%Y')
    scoreboard = scoreboardv2.ScoreboardV2(
        game_date=game_date_formatted,
        league_id='00',
        day_offset=0
    )
    response_json = scoreboard.get_json()
    response = json.loads(response_json)

    result_sets = response.get('resultSets', [])
    games_df = pd.DataFrame()

    for result_set in result_sets:
        if result_set['name'] == 'GameHeader':
            headers = result_set['headers']
            rows = result_set['rowSet']
            games_df = pd.DataFrame(rows, columns=headers)
            break

    if games_df.empty:
        print(f"No 'GameHeader' data found for date: {game_date_formatted}")
        return pd.DataFrame()

    return games_df


def create_game_data_df(scoreboard_df):
    # List of expected columns with fallback logic
    columns_needed = {
        'HOME_TEAM_ID': 'HOME_TEAM_ID',
        'VISITOR_TEAM_ID': 'VISITOR_TEAM_ID',
        'HOME_TEAM_WL': 'WL',  # Original column name
        'VISITOR_TEAM_WL': 'WL',
        'HOME_TEAM_FG_PCT': 'FG_PCT',
        'VISITOR_TEAM_FG_PCT': 'FG_PCT',
        'HOME_TEAM_REB': 'REB',
        'VISITOR_TEAM_REB': 'REB',
        'HOME_TEAM_AST': 'AST',
        'VISITOR_TEAM_AST': 'AST'
    }

    # Create home and away data with fallback columns
    def create_team_df(is_home=True):
        prefix = 'HOME_TEAM_' if is_home else 'VISITOR_TEAM_'
        return scoreboard_df[[
            'GAME_ID',
            columns_needed[f'{prefix}ID'],
            *[col for col in columns_needed if col.startswith(prefix)]
        ]].rename(columns={
            columns_needed[f'{prefix}ID']: 'TEAM_ID',
            **{k: v for k, v in columns_needed.items() if k.startswith(prefix)}
        })

    home_df = create_team_df(is_home=True)
    away_df = create_team_df(is_home=False)

    return pd.concat([home_df, away_df], ignore_index=True)

def create_opponents_df(future_games_df):
    opponent_mappings = []

    for index, row in future_games_df.iterrows():
        game_id = row['GAME_ID']
        home_team_id = row['HOME_TEAM_ID']
        visitor_team_id = row['VISITOR_TEAM_ID']

        opponent_mappings.append({
            'GAME_ID': game_id,
            'TEAM_ID': home_team_id,
            'OPPONENT_TEAM_ID': visitor_team_id
        })

        opponent_mappings.append({
            'GAME_ID': game_id,
            'TEAM_ID': visitor_team_id,
            'OPPONENT_TEAM_ID': home_team_id
        })

    opponents_df = pd.DataFrame(opponent_mappings)
    return opponents_df

class CustomNBAStatsHTTP(NBAStatsHTTP):
    def send_api_request(self, endpoint, parameters, referer=None, timeout=45, headers=None):
        headers = headers or {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Referer': 'https://www.nba.com/'
        }
        return super().send_api_request(
            endpoint,
            parameters,
            referer=referer,
            timeout=timeout,
            headers=headers
        )

# Replace the default HTTP client
NBAStatsHTTP = CustomNBAStatsHTTP