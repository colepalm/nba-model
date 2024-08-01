from sklearn.model_selection import StratifiedShuffleSplit, train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

from src.data_collection.previous_game_collector import fetch_game_data, identify_opponents, prepare_data
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    season = '2022-23'

    # Fetch and preprocess NBA team statistics
    team_stats_df = fetch_nba_team_stats(season)

    # Fetch and preprocess game data
    game_data_df = fetch_game_data(season)

    # Fetch opponents
    opponents_df = identify_opponents(game_data_df)

    combined_data = prepare_data(game_data_df, team_stats_df, opponents_df)

    # Features
    X = combined_data.drop(
        [
            'GAME_DATE',
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

    # Ensure no overlapping game IDs in train and test sets
    game_ids = combined_data['GAME_ID'].unique()
    train_game_ids, test_game_ids = train_test_split(game_ids, test_size=0.2, random_state=42)

    train_index = combined_data['GAME_ID'].isin(train_game_ids)
    test_index = combined_data['GAME_ID'].isin(test_game_ids)

    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    # Check for overlapping game IDs
    train_game_ids = X_train['GAME_ID'].unique()
    test_game_ids = X_test['GAME_ID'].unique()
    overlap = set(train_game_ids).intersection(set(test_game_ids))
    print("Number of overlapping games between train and test sets:", len(overlap))

    # Drop 'GAME_ID' from features after checking for overlap
    X_train = X_train.drop('GAME_ID', axis=1)
    X_test = X_test.drop('GAME_ID', axis=1)

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
