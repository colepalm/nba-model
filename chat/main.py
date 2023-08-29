import time
import pandas as pd

from nba_api.stats.static import teams
from nba_api.stats.endpoints import teamdashboardbygeneralsplits
from sklearn.preprocessing import StandardScaler

max_retries = 5
timeout_seconds = 30

def main():
    # Step 1: Get a list of all NBA teams
    nba_teams = teams.get_teams()

    # Step 2: Choose a specific season (e.g., 2022-2023)
    season = '2022-23'

    # Step 3: Loop through each team to get its statistics
    team_stats_list = []
    for team in nba_teams:
        team_id = team['id']
        team_name = team['full_name']

        # Fetch team statistics for the given season
        team_stats = fetch_team_stats(team_id, season)

        # Convert the response to a dictionary
        team_stats_dict = team_stats.get_normalized_dict()

        # Append the statistics to the list
        team_stats_list.append({
            'team_id': team_id,
            'team_name': team_name,
            'team_stats': team_stats_dict
        })

    # Convert the list of dictionaries to a DataFrame
    team_stats_df = pd.DataFrame(team_stats_list)

    # Extract relevant statistics columns
    columns_to_keep = ['FG_PCT', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TO', 'STL', 'BLK', 'PTS']
    team_stats_df = team_stats_df[['team_id', 'team_name'] + columns_to_keep]

    # Handle missing values (replace NaNs with zeros for simplicity)
    team_stats_df = team_stats_df.fillna(0)

    # Standardize the numerical features using StandardScaler
    scaler = StandardScaler()
    team_stats_df[columns_to_keep] = scaler.fit_transform(team_stats_df[columns_to_keep])
    print(team_stats_df)

def fetch_team_stats(team_id, season):
    retries = 0
    while retries < max_retries:
        try:
            team_stats = teamdashboardbygeneralsplits.TeamDashboardByGeneralSplits(
                team_id=team_id,
                season=season,
                timeout=timeout_seconds
            )
            return team_stats
        except Exception as e:
            print("Error fetching data:", e)
            retries += 1
            print("Retrying in 5 seconds...")
            time.sleep(1)  # Wait for 5 seconds before retrying
    print("Max retries reached. Could not fetch data.")
    return None

if __name__ == "__main__":
    main()