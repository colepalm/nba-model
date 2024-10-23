from datetime import date

import joblib

from src.data_collection.future_game_collector import fetch_games_for_date, prepare_data_for_date
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    season = '2024-25'
    previous_season = '2023-24'
    game_date = date.today()
    team_stats_df = fetch_nba_team_stats(previous_season)
    specific_date_games = fetch_games_for_date(season, game_date)

    if specific_date_games.empty:
        print(f"No games scheduled or data unavailable for {game_date}.")
        return

    combined_data = prepare_data_for_date(specific_date_games, team_stats_df)

    # Ensure the features are in the same format as the training data
    X_specific_date = combined_data.drop(
        [
            'GAME_DATE',
            'GAME_ID',
            'MATCHUP',
            'MIN',
            'OPPONENT_TEAM_ID',
            'TEAM_ABBREVIATION',
            'TEAM_ID_opponent_game',
            'TEAM_ID_opponent_season',
            'TEAM_ID_x',
            'TEAM_ID_y',
            'TEAM_NAME',
            'TEAM_NAME_team_game',
            'TEAM_NAME_team_season',
            'VIDEO_AVAILABLE',
            'WL',  # No WL column should exist for future games, but include it in case
            'PLUS_MINUS'
        ], axis=1, errors='ignore')
    model = joblib.load('nba_game_predictor.pkl')
    print("Model loaded from nba_game_predictor.pkl")

    predictions = model.predict(X_specific_date)

    combined_data['Predicted_Outcome'] = predictions
    combined_data['Predicted_Outcome'] = combined_data['Predicted_Outcome'].map({1: 'W', 0: 'L'})

    print(f"Predictions for Games on {game_date}:")
    print(combined_data[['GAME_ID', 'GAME_DATE', 'MATCHUP', 'Predicted_Outcome']])

if __name__ == "__main__":
    main()