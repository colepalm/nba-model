import pandas as pd

def prepare_full_df(game_data_df, team_stats_df, opponents_df):
    # Merge the opponents info first (this assumes game_data_df has a 'TEAM_ID' column)
    game_with_opponents = pd.merge(
        game_data_df, opponents_df,
        on=['GAME_ID', 'TEAM_ID'],
        how='left'
    )
    print("Step 1 Completed: After Opponent Merge Shape:", game_with_opponents.shape)

    # Merge main team stats
    game_with_team_stats = pd.merge(
        game_with_opponents,
        team_stats_df,
        left_on='TEAM_ID',
        right_on='TEAM_ID',
        suffixes=('', ''),  # No automatic suffix because no conflict exists
        how='left'
    )

    # Manually rename the columns from team_stats_df to add a _team_game suffix.
    # List all the columns that come from team_stats_df that you want to rename:
    stats_cols = ['TEAM_NAME', 'FG_PCT', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS']
    rename_dict = {col: f"{col}_team_game" for col in stats_cols if col in game_with_team_stats.columns}
    game_with_team_stats.rename(columns=rename_dict, inplace=True)
    print("Step 2 Completed: After Main Team Stats Merge, columns:", game_with_team_stats.columns.tolist())

    # Merge opponent team stats with automatic suffixes now:
    game_with_full_stats = pd.merge(
        game_with_team_stats,
        team_stats_df,
        left_on='OPPONENT_TEAM_ID',
        right_on='TEAM_ID',
        suffixes=('_opponent_game', '_opponent_season'),
        how='left'
    )
    print("Step 3 Completed: After Opponent Stats Merge, columns:", game_with_full_stats.columns.tolist())

    return game_with_full_stats
