import time
from datetime import datetime

import pandas as pd

from src.data_collection.future_game_collector import create_game_data_df, fetch_games_for_date


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


def fetch_and_process_games(start_date="2023-10-18", end_date="2024-04-10", max_retries=3):
    """
    Fetches and processes historical game data from NBA API
    Returns: DataFrame with processed game data
    """
    all_dates = pd.date_range(start_date, end_date)
    scoreboard_dfs = []

    for date in all_dates:
        retries = 0
        success = False
        date_str = date.strftime('%Y-%m-%d')

        while retries < max_retries and not success:
            try:
                print(f"Fetching games for {date_str} (attempt {retries + 1})")
                df = fetch_games_for_date(date)

                if not df.empty:
                    # Add common metadata
                    df['FETCH_DATE'] = datetime.today().date()
                    scoreboard_dfs.append(df)
                    success = True
                else:
                    print(f"No games found for {date_str}")
                    success = True  # Consider empty response valid

            except Exception as e:
                print(f"Error fetching {date_str}: {str(e)}")
                retries += 1
                time.sleep(2 ** retries)  # Exponential backoff

        if not success:
            print(f"Failed to fetch {date_str} after {max_retries} attempts")

    # Process raw scoreboard data
    raw_df = pd.concat(scoreboard_dfs, ignore_index=True) if scoreboard_dfs else pd.DataFrame()

    if not raw_df.empty:
        processed_df = create_game_data_df(raw_df)
        return processed_df
    else:
        raise ValueError("No game data was successfully fetched")
