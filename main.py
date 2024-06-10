from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

from src.data_collection.previous_game_collector import fetch_game_data, identify_opponents, \
 prepare_data

from src.data_collection.season_stat_collector import fetch_nba_team_stats

def main():
    season = '2022-23'

    # Fetch and preprocess NBA team statistics
    team_stats_df = fetch_nba_team_stats(season)

    # Fetch and preprocess game data
    game_data_df = fetch_game_data(season)

    # Fetch opponents
    # TODO: This has every game in it. it needs to be separated by team
    opponents_df = identify_opponents(game_data_df)

    combined_data = prepare_data(game_data_df, team_stats_df, opponents_df)

    # Features
    X = combined_data.drop(
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
            'WL'
         ], axis=1)

    # Target variable
    y = combined_data['WL'].map({'W': 1, 'L': 0})

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Create and train model
    model = RandomForestClassifier(random_state=42)
    model.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    print(f'Accuracy: {accuracy}')
    print(f'Classification Report:\n{report}')
    print(f'Confusion Matrix:\n{conf_matrix}')

if __name__ == "__main__":
    main()
