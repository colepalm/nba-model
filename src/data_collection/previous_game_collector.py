import time

import pandas as pd

from src.data_collection.future_game_collector import fetch_games_for_date


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


def fetch_and_process_games(start_date="2023-10-18", end_date="2024-04-10", max_retries=5):
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
                print("Scoreboard columns:", df.columns.tolist())

                if not df.empty:
                    scoreboard_dfs.append(df)
                    success = True
                else:
                    print(f"No games found for {date_str}")
                    success = True

            except Exception as e:
                print(f"Error fetching {date_str}: {str(e)}")
                if retries >= max_retries:
                    print(f"Permanent failure for {date_str}, skipping...")
                    break

                retries += 1
                sleep_time = min(2 ** retries, 60)  # Cap at 60 seconds
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)

    return pd.concat(scoreboard_dfs, ignore_index=True) if scoreboard_dfs else pd.DataFrame()
