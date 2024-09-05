import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

from src.data_collection.previous_game_collector import identify_opponents, prepare_data


def fetch_games_for_date(season, game_date):
    """
    Fetches games scheduled for a specific date in the season.

    :param season: The season (e.g., '2023-24').
    :param game_date: The date for which to fetch games (e.g., '2023-10-22').
    :return: DataFrame with games scheduled on the specified date.
    """
    # Replace with actual API call to fetch games for a specific date
    upcoming_game_data = leaguegamelog.LeagueGameLog(season=season, direction='ASC')
    games = upcoming_game_data.get_data_frames()[0]

    # Filter games for the specified date
    specific_date_games = games[games['GAME_DATE'] == pd.Timestamp(game_date)]
    return specific_date_games


def prepare_data_for_date(games_df, team_stats_df):
    # Identify opponents for the games on the specific date
    opponents_df = identify_opponents(games_df)

    # Prepare the data by merging team statistics
    combined_data = prepare_data(games_df, team_stats_df, opponents_df)

    return combined_data