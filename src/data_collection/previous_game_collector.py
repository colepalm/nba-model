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
    game_with_opponents = pd.merge(game_data_df, opponents_df, on='GAME_ID', how='left')
    print("Step 1 Completed: After Opponent Merge Shape:", game_with_opponents.shape)

    game_with_team_stats = pd.merge(game_with_opponents, team_stats_df, left_on='TEAM_ID_x', right_on='TEAM_ID',
                                    suffixes=('_team_game', '_team_season'), how='left')
    print("Step 2 Completed: After Team Stats Merge Shape:", game_with_team_stats.shape)

    game_with_full_stats = pd.merge(game_with_team_stats, team_stats_df, left_on='OPPONENT_TEAM_ID', right_on='TEAM_ID',
                                    suffixes=('_opponent_game', '_opponent_season'), how='left')
    print("Step 3 Completed: After Opponent Stats Merge Shape:", game_with_full_stats.shape)

    # Ensure that no columns have been incorrectly duplicated or contain overlapping information
    print("Columns in the final dataframe:", game_with_full_stats.columns.tolist())
    print("Unique games represented:", game_with_full_stats['GAME_ID'].nunique())

    duplicates = game_with_full_stats[game_with_full_stats.duplicated(subset=['GAME_ID', 'TEAM_ID_x'])]
    print("Number of duplicate rows:", duplicates.shape[0])

    return game_with_full_stats

