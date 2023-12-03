import time
import pandas as pd

from nba_api.stats.static import teams
from nba_api.stats.endpoints import teamdashboardbygeneralsplits
from sklearn.preprocessing import StandardScaler

max_retries = 5
timeout_seconds = 30

def fetch_nba_team_stats(season):
    nba_teams = teams.get_teams()

    team_stats_list = []

    for team in nba_teams:
        team_id = team['id']
        team_name = team['full_name']

        team_stats = fetch_team_stats(team_id, season)

        team_stats_dict = team_stats.get_normalized_dict()

        overall_stats = team_stats_dict['OverallTeamDashboard']

        for overall_stat in overall_stats:
            extracted_stat = {
                'TEAM_ID': team_id,
                'TEAM_NAME': team_name,
            }

            columns_to_keep = ['FG_PCT', 'FG3_PCT', 'FT_PCT', 'REB', 'AST', 'TOV', 'STL', 'BLK', 'PTS']

            for column in columns_to_keep:
                extracted_stat[column] = overall_stat[column]

            team_stats_list.append(extracted_stat)

    team_stats_df = pd.DataFrame(team_stats_list)
    team_stats_df = team_stats_df.fillna(0)

    scaler = StandardScaler()
    team_stats_df[columns_to_keep] = scaler.fit_transform(team_stats_df[columns_to_keep])

    return team_stats_df

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
            time.sleep(1)
    print("Max retries reached. Could not fetch data.")
    return None