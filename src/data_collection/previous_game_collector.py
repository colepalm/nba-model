from nba_api.stats.endpoints import leaguegamelog


def fetch_game_data(season):
    game_data = leaguegamelog.LeagueGameLog(season)

    # Extract relevant information from the API response
    games = game_data.get_data_frames()[0]

    return games