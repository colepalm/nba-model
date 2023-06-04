import pandas as pd
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import cumestatsteam, teamgamelog

from model.forest import Forest


def main():
    # Today's Score Board
    games = scoreboard.ScoreBoard()
    games_dict = games.get_dict()
    if len(games_dict['scoreboard']['games']) > 0:
        team1 = games_dict['scoreboard']['games'][0]['homeTeam']['teamId']
        team2 = games_dict['scoreboard']['games'][0]['awayTeam']['teamId']

        gameLogTeam1 = teamgamelog.TeamGameLog(
            season='2022-23',
            season_type_all_star='Regular Season',
            team_id=team1,
        )

        gameLogTeam2 = teamgamelog.TeamGameLog(
            season='2022-23',
            season_type_all_star='Regular Season',
            team_id=team2,
        )

        log_dict1 = gameLogTeam1.get_dict()
        log_dict2 = gameLogTeam2.get_dict()

        team1Games = []
        team1GamesWL = []
        team2Games = []
        team2GamesWL = []

        # Creates arrays for games and results
        for game in log_dict1['resultSets'][0]['rowSet']:
            team1Games.append(game[1])
            team1GamesWL.append(game[4])

        for game in log_dict2['resultSets'][0]['rowSet']:
            team2Games.append(game[1])
            team2GamesWL.append(game[4])

        statsTeam1 = cumestatsteam.CumeStatsTeam(
            game_ids=team1Games,
            league_id=0,
            season='2022-23',
            season_type_all_star='Regular Season',
            team_id=team1
        )

        statsTeam2 = cumestatsteam.CumeStatsTeam(
            game_ids=team2Games,
            league_id=0,
            season='2022-23',
            season_type_all_star='Regular Season',
            team_id=team2
        )

        forest1 = Forest(statsTeam1.get_dict())
        df1 = forest1.create_dataframe()

        forest2 = Forest(statsTeam2.get_dict())
        df2 = forest2.create_dataframe()

        selections = pd.concat([df1, df2], ignore_index=True, axis=1)
        selections = selections.sample(frac=1, random_state=42).reset_index(drop=True)

if __name__ == "__main__":
    main()
