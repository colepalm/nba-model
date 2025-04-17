import json

import pandas as pd

from nba_api.stats.library.http import NBAStatsHTTP
from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(date):
    try:
        date_str = date.strftime('%Y-%m-%d')  # Changed format to YYYY-MM-DD
        scoreboard = scoreboardv2.ScoreboardV2(
            game_date=date_str,
            league_id='00',
            day_offset=0
        )

        response = json.loads(scoreboard.get_json())
        result_sets = response.get('resultSets', [])

        print(f"Available result sets: {[rs['name'] for rs in result_sets]}")

        line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)
        if line_score:
            print(f"LineScore columns: {line_score['headers']}")

        game_header = next((rs for rs in result_sets if rs['name'] == 'GameHeader'), None)

        if not game_header or not line_score:
            print(f"Incomplete data for {date_str}")
            return pd.DataFrame()

        # Create DataFrames
        game_header_df = pd.DataFrame(game_header['rowSet'], columns=game_header['headers'])
        line_score_df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])

        # Merge to get home/away context
        merged_df = line_score_df.merge(
            game_header_df[['GAME_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID']],
            on='GAME_ID',
            how='left'
        )

        # Add home/away flag using team ID comparison
        merged_df['IS_HOME'] = merged_df['TEAM_ID'] == merged_df['HOME_TEAM_ID']
        merged_df['GAME_DATE'] = pd.to_datetime(date_str)

        return merged_df

    except Exception as e:
        print(f"Error fetching {date_str}: {str(e)}")
        return pd.DataFrame()


def create_game_data_df(scoreboard_df):
    """
    Creates a unified game data DataFrame with team-specific statistics.
    Handles both home and visitor teams from LineScore data.
    """
    if scoreboard_df.empty:
        return pd.DataFrame()

    base_required_columns = ['GAME_ID', 'TEAM_ID', 'FG_PCT', 'REB', 'AST', 'PTS']
    missing_cols = [col for col in base_required_columns if col not in scoreboard_df.columns]

    if missing_cols:
        print(f"Warning: Missing critical columns in scoreboard data - {missing_cols}")
        return pd.DataFrame()

    # Create a copy of relevant columns plus PTS for WL calculation
    game_data = scoreboard_df[base_required_columns + ['GAME_DATE']].copy()

    # Derive WL from PTS by comparing scores within each game
    # Group by game to compare scores
    game_results = []

    for game_id, game_group in scoreboard_df.groupby('GAME_ID'):
        if len(game_group) == 2:  # Ensure we have exactly 2 teams per game
            team1, team2 = game_group.iloc[0], game_group.iloc[1]

            # Determine winner and loser
            if team1['PTS'] > team2['PTS']:
                team1_wl, team2_wl = 'W', 'L'
            elif team1['PTS'] < team2['PTS']:
                team1_wl, team2_wl = 'L', 'W'
            else:
                team1_wl, team2_wl = 'T', 'T'  # Tie (rare in NBA)

            # Add results
            game_results.append({'GAME_ID': game_id, 'TEAM_ID': team1['TEAM_ID'], 'WL': team1_wl})
            game_results.append({'GAME_ID': game_id, 'TEAM_ID': team2['TEAM_ID'], 'WL': team2_wl})

    wl_df = pd.DataFrame(game_results)
    game_data = game_data.merge(wl_df, on=['GAME_ID', 'TEAM_ID'], how='left')

    if 'TOV' in scoreboard_df.columns:
        game_data['TOV'] = scoreboard_df['TOV']

    if 'FG3_PCT' in scoreboard_df.columns:
        game_data['FG3_PCT'] = scoreboard_df['FG3_PCT']

    if 'FT_PCT' in scoreboard_df.columns:
        game_data['FT_PCT'] = scoreboard_df['FT_PCT']

        # Add home/away indicator if available
    if 'IS_HOME' in scoreboard_df.columns:
        game_data['IS_HOME'] = scoreboard_df['IS_HOME']

    return game_data