import pandas as pd
from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season=season)

    # Extract relevant information from the API response
    games = game_data.get_data_frames()[0]

    return games

def merge_home_and_away(game_data, opponents_df):
    # Merge dataframes based on the common key
    combined_data = pd.merge(opponents_df, game_data, on=['GAME_ID', 'TEAM_ID'], how='left')

    return combined_data

def identify_opponents(game_log):
    # Ensure the data is sorted by GAME_ID for consistency
    game_log = game_log.sort_values(by='GAME_ID')

    # List to store opponent information
    opponent_mappings = []

    # For each game, find the two teams and assign them as each other's opponent
    for game_id, group in game_log.groupby('GAME_ID'):
        if len(group) == 2:
            team_ids = group['TEAM_ID'].values
            opponent_mappings.append({'GAME_ID': game_id, 'TEAM_ID': team_ids[0], 'OPPONENT_TEAM_ID': team_ids[1]})
            opponent_mappings.append({'GAME_ID': game_id, 'TEAM_ID': team_ids[1], 'OPPONENT_TEAM_ID': team_ids[0]})

    # Convert the list of dictionaries to a DataFrame
    opponents_df = pd.DataFrame(opponent_mappings)

    return opponents_df
