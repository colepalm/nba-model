import json

import pandas as pd
from nba_api.stats.endpoints import scoreboardv2

from src.data_collection.previous_game_collector import identify_opponents, prepare_data


def fetch_games_for_date(season, game_date):
    """
    Fetches games scheduled for a specific date in the season.

    :param season: The season (e.g., '2023-24').
    :param game_date: The date for which to fetch games (e.g., '2023-10-22').
    :return: DataFrame with games scheduled on the specified date.
    """
    game_date_formatted = pd.to_datetime(game_date).strftime('%m/%d/%Y')
    scoreboard = scoreboardv2.ScoreboardV2(
        game_date=game_date_formatted,
        league_id='00',
        day_offset=0
    )
    response_json = scoreboard.get_json()
    response = json.loads(response_json)

    result_sets = response.get('resultSets', [])
    games_df = pd.DataFrame()

    for result_set in result_sets:
        if result_set['name'] == 'GameHeader':
            headers = result_set['headers']
            rows = result_set['rowSet']
            games_df = pd.DataFrame(rows, columns=headers)
            break

    if games_df.empty:
        print(f"No 'GameHeader' data found for date: {game_date_formatted}")
        return pd.DataFrame()

    return games_df


def prepare_data_for_date(games_df, team_stats_df):
    # Identify opponents for the games on the specific date
    opponents_df = identify_opponents(games_df)

    # Prepare the data by merging team statistics
    combined_data = prepare_data(games_df, team_stats_df, opponents_df)

    return combined_data