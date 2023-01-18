import pandas as pd

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import cumestatsteam


# Today's Score Board
games = scoreboard.ScoreBoard()

# dictionary
games_dict = games.get_dict()
team1 = games_dict['scoreboard']['games'][0]['homeTeam']['teamId']
team2 = games_dict['scoreboard']['games'][0]['awayTeam']['teamId']

season = cumestatsteam.CumeStatsTeam()
print(season)


print(games_dict)
