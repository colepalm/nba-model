import json

import pandas as pd

from nba_api.stats.endpoints import scoreboardv2


def fetch_games_for_date(date):
    try:
        date_str = date.strftime('%m/%d/%Y')  # NBA API expects MM/DD/YYYY format
        print(f"Fetching data for {date_str}")

        # Try the normal API call first
        try:
            scoreboard = scoreboardv2.ScoreboardV2(
                game_date=date_str,
                league_id='00',
                day_offset=0
            )
            response = json.loads(scoreboard.get_json())

        except KeyError as e:
            if 'WinProbability' in str(e):
                print(f"WinProbability KeyError for {date_str} - this is expected for dates before April 10, 2025")
                # For historical dates, we need to use a different approach
                return fetch_games_historical_method(date, date_str)
            else:
                # Some other KeyError, re-raise it
                raise e

        # If we get here, the normal API call worked
        result_sets = response.get('resultSets', [])
        print(f"Available result sets: {[rs['name'] for rs in result_sets]}")

        line_score = next((rs for rs in result_sets if rs['name'] == 'LineScore'), None)
        game_header = next((rs for rs in result_sets if rs['name'] == 'GameHeader'), None)

        if not game_header or not line_score:
            print(f"Incomplete data for {date_str}")
            return pd.DataFrame()

        if not line_score['rowSet'] or not game_header['rowSet']:
            print(f"No games found for {date_str}")
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

        merged_df['IS_HOME'] = merged_df['TEAM_ID'] == merged_df['HOME_TEAM_ID']
        merged_df['GAME_DATE'] = pd.to_datetime(date.strftime('%Y-%m-%d'))

        print(f"Successfully fetched {len(merged_df)} rows for {date_str}")
        return merged_df

    except Exception as e:
        print(f"Error fetching {date_str}: {str(e)}")
        return pd.DataFrame()


def fetch_games_historical_method(date, date_str):
    """
    Alternative method for fetching historical games (pre-April 10, 2025)
    Uses direct HTTP requests to bypass the nba_api library's WinProbability requirement
    """
    try:
        import requests

        print(f"Using historical method for {date_str}")

        # Direct NBA API call
        url = "https://stats.nba.com/stats/scoreboardV2"

        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.nba.com/',
            'Connection': 'keep-alive',
        }

        params = {
            'GameDate': date_str,  # MM/DD/YYYY format
            'LeagueID': '00',
            'DayOffset': '0'
        }

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code != 200:
            print(f"HTTP error {response.status_code} for {date_str}")
            return pd.DataFrame()

        data = response.json()
        result_sets = data.get('resultSets', [])

        if not result_sets:
            print(f"No result sets found for {date_str}")
            return pd.DataFrame()

        line_score = next((rs for rs in result_sets if rs.get('name') == 'LineScore'), None)
        game_header = next((rs for rs in result_sets if rs.get('name') == 'GameHeader'), None)

        if not line_score or not game_header:
            print(f"Required result sets missing for {date_str}")
            return pd.DataFrame()

        if not line_score.get('rowSet') or not game_header.get('rowSet'):
            print(f"No games found for {date_str}")
            return pd.DataFrame()

        # Create DataFrames (same logic as before)
        game_header_df = pd.DataFrame(game_header['rowSet'], columns=game_header['headers'])
        line_score_df = pd.DataFrame(line_score['rowSet'], columns=line_score['headers'])

        merged_df = line_score_df.merge(
            game_header_df[['GAME_ID', 'HOME_TEAM_ID', 'VISITOR_TEAM_ID']],
            on='GAME_ID',
            how='left'
        )

        merged_df['IS_HOME'] = merged_df['TEAM_ID'] == merged_df['HOME_TEAM_ID']
        merged_df['GAME_DATE'] = pd.to_datetime(date.strftime('%Y-%m-%d'))

        print(f"Historical method: Successfully fetched {len(merged_df)} rows for {date_str}")
        return merged_df

    except Exception as e:
        print(f"Error in historical method for {date_str}: {e}")
        return pd.DataFrame()


def is_historical_date(date):
    """
    Check if a date is before the WinProbability field was added
    """
    cutoff_date = pd.Timestamp('2025-04-10')
    return pd.Timestamp(date) < cutoff_date


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