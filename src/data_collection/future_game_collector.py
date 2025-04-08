import json

import pandas as pd

from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(date):
    """
    Fetches games and properly structures team IDs with home/away flags
    """
    try:
        date_str = date.strftime('%m/%d/%Y')
        scoreboard = scoreboardv2.ScoreboardV2(
            game_date=date_str,
            league_id='00',
            day_offset=0
        )

        # Get raw JSON response
        response = json.loads(scoreboard.get_json())
        result_sets = response.get('resultSets', [])

        # Extract GameHeader and LineScore data
        game_header = next((rs for rs in result_sets if rs['name'] == 'GameHeader'), None)
        line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)

        if not game_header or not line_score:
            print(f"Incomplete data for {date_str}")
            return pd.DataFrame()

        # Create DataFrames
        game_header_df = pd.DataFrame(game_header['rowSet'], columns=game_header['headers'])
        line_score_df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])

        # Merge to get home/away context
        merged_df = line_score_df.merge(
            game_header_df[['GAME_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID']],
            on='GAME_ID',
            how='left'
        )

        # Add home/away flag using team ID comparison
        merged_df['IS_HOME'] = merged_df['TEAM_ID'] == merged_df['HOME_TEAM_ID']
        merged_df['GAME_DATE'] = pd.to_datetime(date_str)

        return merged_df

    except Exception as e:
        print(f"Error fetching {date_str}: {str(e)}")
        return pd.DataFrame()


def create_game_data_df(scoreboard_df):
    """
    Creates a unified game data DataFrame with team-specific statistics.
    Handles both home and visitor teams from LineScore data.
    """
    if scoreboard_df.empty:
        return pd.DataFrame()

    # Ensure we have the required columns
    required_columns = ['GAME_ID', 'TEAM_ID', 'WL', 'FG_PCT', 'REB', 'AST']
    missing_cols = [col for col in required_columns if col not in scoreboard_df.columns]

    if missing_cols:
        print(f"Warning: Missing columns in scoreboard data - {missing_cols}")
        return pd.DataFrame()

    # Select and rename columns to standard format
    game_data = scoreboard_df[[
        'GAME_ID',
        'TEAM_ID',
        'WL',
        'FG_PCT',
        'REB',
        'AST'
    ]].copy()

    # Add game date if available
    if 'GAME_DATE' in scoreboard_df.columns:
        game_data['GAME_DATE'] = scoreboard_df['GAME_DATE']

    return game_data

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