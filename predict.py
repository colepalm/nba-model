from datetime import date

import joblib

from src.data_collection.future_game_collector import fetch_games_for_date, create_game_data_df, \
    create_opponents_df
from src.data_collection.prepare_data import prepare_full_df
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    previous_season = '2023-24'
    game_date = date.today()
    team_stats_df = fetch_nba_team_stats(previous_season)
    scoreboard_df = fetch_games_for_date(game_date)

    if scoreboard_df.empty:
        print(f"No games scheduled or data unavailable for {game_date}.")
        return

    game_data_df = create_game_data_df(scoreboard_df)
    opponents_df = create_opponents_df(scoreboard_df)

    combined_data = prepare_full_df(game_data_df, team_stats_df, opponents_df)

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
            'WL',
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