import pandas as pd
from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season)

    # Extract relevant information from the API response
    games = game_data.get_data_frames()[0]

    return games

def merge_team_and_game_data(team_stats, game_data):
    # Assuming there's a common key, such as 'team_id'
    common_key = 'TEAM_ID'

    # Merge dataframes based on the common key
    combined_data = pd.merge(team_stats, game_data, on=common_key, how='inner')

    return combined_data