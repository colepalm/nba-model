import pandas as pd

def prepare_full_df(game_data_df, team_stats_df, opponents_df):
    game_with_opponents = pd.merge(
        game_data_df, opponents_df,
        on=['GAME_ID', 'TEAM_ID'],
        how='left'
    )

    game_with_team_stats = pd.merge(
        game_with_opponents,
        team_stats_df,
        left_on='TEAM_ID',
        right_on='TEAM_ID',
        suffixes=('_team_game', '_team_season'),
        how='left'
    )

    columns_to_rename = {
        'TEAM_NAME': 'TEAM_NAME_team_game',
        'FG_PCT': 'FG_PCT_team_game',
        'FG3_PCT': 'FG3_PCT_team_game',
        'FT_PCT': 'FT_PCT_team_game',
        'REB': 'REB_team_game',
        'AST': 'AST_team_game',
        'TOV': 'TOV_team_game',
        'STL': 'STL_team_game',
        'BLK': 'BLK_team_game',
        'PTS': 'PTS_team_game'
    }
    game_with_team_stats.rename(columns=columns_to_rename, inplace=True)

    game_with_full_stats = pd.merge(
        game_with_team_stats,
        team_stats_df,
        left_on='OPPONENT_TEAM_ID',
        right_on='TEAM_ID',
        suffixes=('_opponent_game', '_opponent_season'),
        how='left'
    )

    print("Columns after opponent merge:", game_with_full_stats.columns.tolist())
    return game_with_full_stats