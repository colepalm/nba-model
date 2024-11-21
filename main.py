import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

from src.data_collection.prepare_data import prepare_full_df
from src.data_collection.previous_game_collector import fetch_game_data, identify_opponents
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    season = '2023-24'

    team_stats_df = fetch_nba_team_stats(season)
    game_data_df = fetch_game_data(season)
    opponents_df = identify_opponents(game_data_df)
    combined_data = prepare_full_df(game_data_df, team_stats_df, opponents_df)

    combined_data.to_csv('combined_data.csv', index=False)
    print("Combined data saved to combined_data.csv")

    # Features: Exclude PLUS_MINUS and any other potential leakage features
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
            'WL',
            'PLUS_MINUS'
         ], axis=1)

    # Target variable
    y = combined_data['WL'].map({'W': 1, 'L': 0})

    # Ensure no overlapping game IDs in train and test sets
    game_ids = combined_data['GAME_ID'].unique()
    train_game_ids, test_game_ids = train_test_split(game_ids, test_size=0.2, random_state=42)

    train_index = combined_data['GAME_ID'].isin(train_game_ids)
    test_index = combined_data['GAME_ID'].isin(test_game_ids)

    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    # Verify no overlapping game IDs
    train_game_ids_set = set(train_game_ids)
    test_game_ids_set = set(test_game_ids)
    overlap = train_game_ids_set.intersection(test_game_ids_set)
    print("Number of overlapping games between train and test sets:", len(overlap))

    # Check class balance
    print("Training set class distribution:", y_train.value_counts())
    print("Test set class distribution:", y_test.value_counts())

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

    # Check feature importances
    feature_importances = model.feature_importances_
    feature_names = X.columns
    importance_df = pd.DataFrame({'Feature': feature_names, 'Importance': feature_importances})
    importance_df = importance_df.sort_values(by='Importance', ascending=False)
    print("Feature importances:\n", importance_df)

    joblib.dump(model, 'nba_game_predictor.pkl')
    print("Model saved to nba_game_predictor.pkl")

if __name__ == "__main__":
    main()
