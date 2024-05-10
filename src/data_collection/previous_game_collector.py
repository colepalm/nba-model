import pandas as pd
from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season=season)

    games = game_data.get_data_frames()[0]

    return games

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


def prepare_data(game_data_df, team_stats_df, opponents_df):
    # First, merge the game data with opponent IDs
    game_with_opponents = pd.merge(game_data_df, opponents_df, on='GAME_ID', how='left')

    # Merge team stats for the primary team
    game_with_team_stats = pd.merge(game_with_opponents, team_stats_df, left_on='TEAM_ID', right_on='TEAM_ID',
                                    suffixes=('', '_team'), how='left')

    # Merge team stats for the opponent team
    game_with_full_stats = pd.merge(game_with_team_stats, team_stats_df, left_on='OPPONENT_TEAM_ID', right_on='TEAM_ID',
                                    suffixes=('_team', '_opponent'), how='left')

    return game_with_full_stats
