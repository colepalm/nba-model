from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season)

    # Extract relevant information from the API response
    games = game_data.get_data_frames()[0]

    return games

def merge_team_and_game_data(team_stats, game_data):
    return
    # TODO: IMPLEMENT ME