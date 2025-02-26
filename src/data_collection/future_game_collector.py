import json

import pandas as pd
from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(game_date):
    """
    Fetches games scheduled for a specific date in the season.

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


def create_game_data_df(scoreboard_df):
    home_df = scoreboard_df[[
        'GAME_ID', 'HOME_TEAM_ID', 'HOME_TEAM_WL', 'HOME_TEAM_FG_PCT', 'HOME_TEAM_REB', 'HOME_TEAM_AST'
    ]].rename(
        columns={
            'HOME_TEAM_ID': 'TEAM_ID',
            'HOME_TEAM_WL': 'WL',
            'HOME_TEAM_FG_PCT': 'FG_PCT',
            'HOME_TEAM_REB': 'REB',
            'HOME_TEAM_AST': 'AST'
        }
    )

    away_df = scoreboard_df[[
        'GAME_ID', 'VISITOR_TEAM_ID', 'VISITOR_TEAM_WL', 'VISITOR_TEAM_FG_PCT', 'VISITOR_TEAM_REB', 'VISITOR_TEAM_AST'
    ]].rename(
        columns={
            'VISITOR_TEAM_ID': 'TEAM_ID',
            'VISITOR_TEAM_WL': 'WL',
            'VISITOR_TEAM_FG_PCT': 'FG_PCT',
            'VISITOR_TEAM_REB': 'REB',
            'VISITOR_TEAM_AST': 'AST'
        }
    )

    # Combine home and away data
    game_data_df = pd.concat([home_df, away_df], ignore_index=True)

    return game_data_df

def create_opponents_df(future_games_df):
    opponent_mappings = []

    for index, row in future_games_df.iterrows():
        game_id = row['GAME_ID']
        home_team_id = row['HOME_TEAM_ID']
        visitor_team_id = row['VISITOR_TEAM_ID']

        opponent_mappings.append({
            'GAME_ID': game_id,
            'TEAM_ID': home_team_id,
            'OPPONENT_TEAM_ID': visitor_team_id
        })

        opponent_mappings.append({
            'GAME_ID': game_id,
            'TEAM_ID': visitor_team_id,
            'OPPONENT_TEAM_ID': home_team_id
        })

    opponents_df = pd.DataFrame(opponent_mappings)
    return opponents_df