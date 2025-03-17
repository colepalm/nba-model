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
            day_offset='0',
            league_id='00'
        )

        # Get available data components
        data = {}
        for component in ['GameHeader', 'LineScore', 'TeamStats']:
            try:
                df = scoreboard.get_data_frames()[0]
                if not df.empty:
                    data[component] = df
            except Exception as e:
                print(f"Warning: {component} data unavailable for {date_str}")

        # Merge available components
        merged_df = pd.DataFrame()
        if 'GameHeader' in data:
            merged_df = data['GameHeader']
            if 'LineScore' in data:
                merged_df = merged_df.merge(data['LineScore'], on='GAME_ID')
            if 'TeamStats' in data:
                merged_df = merged_df.merge(data['TeamStats'], on='GAME_ID')

        return merged_df

    except Exception as e:
        print(f"Critical error fetching {date_str}: {str(e)}")
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