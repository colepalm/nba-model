import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

from src.data_collection.previous_game_collector import fetch_game_data
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    season = '2022-23'

    # Fetch and preprocess NBA team statistics
    team_stats_df = fetch_nba_team_stats(season)

    # Fetch and preprocess game data
    game_data_df = fetch_game_data(season)  # Implement this function based on the game data source

    # Combine team and game data based on common keys (e.g., team ID)
    combined_data = merge_team_and_game_data(team_stats_df, game_data_df)

    # Define features and target variable
    X = team_stats_df.drop('target_column', axis=1)  # Features
    y = team_stats_df['target_column']  # Target variable

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
