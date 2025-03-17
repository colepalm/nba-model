import json

import pandas as pd

from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(date):
    """
    Fetches games scheduled for a specific date in the season.

    :param date: The date for which to fetch games (e.g., '2023-10-22').
    :return: DataFrame with games scheduled on the specified date.
    """
    try:
        date_str = date.strftime('%m/%d/%Y')
        scoreboard = scoreboardv2.ScoreboardV2(
            game_date=date_str,
            league_id='00',
            day_offset=0
        )

        response_json = scoreboard.get_json()
        response = json.loads(response_json)

        result_sets = response.get('resultSets', [])

        # Initialize empty DataFrames for components
        game_header = pd.DataFrame()
        line_score = pd.DataFrame()

        # Parse result sets
        for result_set in result_sets:
            if result_set['name'] == 'GameHeader':
                game_header = pd.DataFrame(
                    result_set['rowSet'],
                    columns=result_set['headers']
                )
            elif result_set['name'] == 'LineScore':
                line_score = pd.DataFrame(
                    result_set['rowSet'],
                    columns=result_set['headers']
                )

        # Check if we have the minimum required data
        if game_header.empty:
            print(f"No 'GameHeader' data found for date: {date_str}")
            return pd.DataFrame()

        # Merge GameHeader with LineScore if available
        if not line_score.empty:
            merged_df = game_header.merge(
                line_score,
                on='GAME_ID',
                how='left',
                suffixes=('', '_LINE')
            )
        else:
            merged_df = game_header.copy()
            print(f"No 'LineScore' data found for date: {date_str}")

            # Add date context
            merged_df['GAME_DATE'] = pd.to_datetime(date_str)

            # Add fallback columns if necessary
            required_columns = {
                'HOME_TEAM_WL': 'W',
                'HOME_TEAM_FG_PCT': 0.0,
                'HOME_TEAM_REB': 0,
                'HOME_TEAM_AST': 0,
                'VISITOR_TEAM_WL': 'L',
                'VISITOR_TEAM_FG_PCT': 0.0,
                'VISITOR_TEAM_REB': 0,
                'VISITOR_TEAM_AST': 0
            }

            for col, default in required_columns.items():
                if col not in merged_df.columns:
                    print(f"Creating fallback column {col}")
                    merged_df[col] = default

        return merged_df

    except Exception as e:
        print(f"Error fetching games for {date_str}: {str(e)}")
        return pd.DataFrame()


def create_game_data_df(scoreboard_df):
    """Create unified game data DataFrame with flexible column handling"""
    # Define expected column patterns
    column_map = {
        'HOME_TEAM_': {
            'id': 'HOME_TEAM_ID',
            'stats': ['WL', 'FG_PCT', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS']
        },
        'VISITOR_TEAM_': {
            'id': 'VISITOR_TEAM_ID',
            'stats': ['WL', 'FG_PCT', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS']
        }
    }

    dfs = []

    for prefix, config in column_map.items():
        # Verify available columns
        available_stats = []
        for stat in config['stats']:
            col_name = f"{prefix}{stat}"
            if col_name in scoreboard_df.columns:
                available_stats.append(stat)
            else:
                print(f"Warning: Column {col_name} not found, excluding from dataset")

        # Create base DataFrame
        team_df = scoreboard_df[['GAME_ID', config['id']]].copy()
        team_df.rename(columns={config['id']: 'TEAM_ID'}, inplace=True)

        # Add available stats
        for stat in available_stats:
            src_col = f"{prefix}{stat}"
            team_df[stat] = scoreboard_df[src_col]

        dfs.append(team_df)

    # Combine home and visitor data
    game_data_df = pd.concat(dfs, ignore_index=True)

    # Add essential metadata
    if 'GAME_DATE' in scoreboard_df.columns:
        game_data_df['GAME_DATE'] = scoreboard_df['GAME_DATE']

    return game_data_df

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