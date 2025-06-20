import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix

from src.cache_manager import CacheManager
from src.data_collection.future_game_collector import create_game_data_df
from src.data_collection.prepare_data import prepare_full_df
from src.data_collection.previous_game_collector import identify_opponents, fetch_and_process_games
from src.data_collection.season_stat_collector import fetch_nba_team_stats


def main():
    season = '2024-25'
    cm = CacheManager()

    # Try to get cached processed data
    processed_key = f"processed_data_{season}"
    combined_data = cm.get(processed_key)

    if combined_data is None:
        print("Cache miss - processing fresh data")

        historical_key = f"historical_games_{season}"
        historical_raw = cm.get_safe(historical_key)
        fresh_dates = pd.date_range("2023-10-18", "2024-04-10")

        if not historical_raw.empty:
            cached_dates = pd.to_datetime(historical_raw['GAME_DATE'].unique())
            missing_dates = [d for d in fresh_dates if d not in cached_dates]
            if missing_dates:
                print(f"Fetching {len(missing_dates)} missing dates")
                new_data = fetch_and_process_games(
                    start_date=missing_dates[0],
                    end_date=missing_dates[-1]
                )
                historical_raw = pd.concat([historical_raw, new_data])

        if historical_raw.empty:
            historical_raw = fetch_and_process_games()

        required_columns = ['GAME_ID', 'TEAM_ID']
        missing = [col for col in required_columns if col not in historical_raw.columns]
        if missing:
            raise ValueError(f"Missing critical columns in raw data: {missing}")

        stats_key = f"team_stats_{season}"
        team_stats_df = cm.get(stats_key) or fetch_nba_team_stats(season)

        game_data_df = create_game_data_df(historical_raw)

        if game_data_df.empty:
            raise ValueError("No game data available for processing")

        opponents_df = identify_opponents(game_data_df)

        combined_data = prepare_full_df(
            game_data_df,
            fetch_nba_team_stats(season),
            opponents_df
        )

    else:
        print("Using cached processed data")

    # Save combined data
    combined_data.to_csv('combined_data.csv', index=False)
    print("Combined data saved to combined_data.csv")

    # Feature engineering and target preparation
    X, y, feature_cols = prepare_features(combined_data)

    X_train, X_test, y_train, y_test = create_train_test_split(X, y, combined_data)

    # Model training and evaluation
    model = train_model(X_train, y_train)
    evaluate_model(model, X_test, y_test, feature_cols)

    # Check feature importances
    feature_importances = model.feature_importances_
    feature_names = X.columns
    importance_df = pd.DataFrame({'Feature': feature_names, 'Importance': feature_importances})
    importance_df = importance_df.sort_values(by='Importance', ascending=False)
    print("Feature importances:\n", importance_df)

    joblib.dump(model, 'nba_game_predictor.pkl')
    print("Model saved to nba_game_predictor.pkl")

def prepare_features(combined_data):
    drop_cols = ['GAME_ID', 'OPPONENT_TEAM_ID', 'TEAM_ID_opponent_game',
                 'TEAM_ID_opponent_season', 'TEAM_NAME', 'TEAM_NAME_team_game']

    X = combined_data.drop(drop_cols, axis=1)
    y = combined_data['WL'].map({'W': 1, 'L': 0})
    feature_cols = X.columns.tolist()

    return X, y, feature_cols


def create_train_test_split(X, y, combined_data):
    game_ids = combined_data['GAME_ID'].unique()
    train_game_ids, test_game_ids = train_test_split(game_ids, test_size=0.2, random_state=42)

    train_index = combined_data['GAME_ID'].isin(train_game_ids)
    test_index = combined_data['GAME_ID'].isin(test_game_ids)

    X_train, X_test = X[train_index], X[test_index]
    y_train, y_test = y[train_index], y[test_index]

    print("TRAIN columns:", X_train.columns.tolist())

    # Verify no overlapping game IDs
    train_game_ids_set = set(train_game_ids)
    test_game_ids_set = set(test_game_ids)
    overlap = train_game_ids_set.intersection(test_game_ids_set)
    print("Number of overlapping games between train and test sets:", len(overlap))

    print("Training set class distribution:", y_train.value_counts())
    print("Test set class distribution:", y_test.value_counts())

    return X_train, X_test, y_train, y_test


def train_model(X_train, y_train):
    X_train_no_na = X_train.dropna()
    y_train_no_na = y_train[X_train_no_na.index]

    # TODO: Consider hyperparameter tuning here
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=None,
        min_samples_split=2,
        random_state=42
    )
    model.fit(X_train_no_na, y_train_no_na)
    return model


def evaluate_model(model, X_test, y_test, feature_cols):
    # TODO: Consider handling missing values in test data consistently
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)

    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    print(f'Accuracy: {accuracy:.4f}')
    print(f'Classification Report:\n{report}')
    print(f'Confusion Matrix:\n{conf_matrix}')

    return {
        'accuracy': accuracy,
        'classification_report': report,
        'confusion_matrix': conf_matrix,
    }

if __name__ == "__main__":
    main()