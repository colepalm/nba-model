import pandas as pd
from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season=season)
    games = game_data.get_data_frames()[0]
    return games

def identify_opponents(game_log):
    game_log = game_log.sort_values(by='GAME_ID')

    opponent_mappings = []

    # For each game, find the two teams and assign them as each other's opponent
    for game_id, group in game_log.groupby('GAME_ID'):
        if len(group) == 2:
            team_ids = group['TEAM_ID'].values
            opponent_mappings.append({'GAME_ID': game_id, 'TEAM_ID': team_ids[0], 'OPPONENT_TEAM_ID': team_ids[1]})
            opponent_mappings.append({'GAME_ID': game_id, 'TEAM_ID': team_ids[1], 'OPPONENT_TEAM_ID': team_ids[0]})

    opponents_df = pd.DataFrame(opponent_mappings)

    return opponents_df
