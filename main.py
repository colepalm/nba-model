import pandas as pd

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import teamgamelog

# Today's Score Board
games = scoreboard.ScoreBoard()
games_dict = games.get_dict()
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


print(games_dict)
