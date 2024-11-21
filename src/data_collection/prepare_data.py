import pandas as pd


def prepare_full_df(game_data_df, team_stats_df, opponents_df):
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