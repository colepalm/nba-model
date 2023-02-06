import pandas as pd

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import cumestatsteam, leaguegamelog


# Today's Score Board
games = scoreboard.ScoreBoard()
gameLog = leaguegamelog.LeagueGameLog(
    counter=0,
    direction='ASC',
    league_id='00',
    player_or_team_abbreviation='T',
    season='2020-21',
    season_type_all_star='Regular Season',
    sorter='DATE'
)

log_dict = gameLog.get_dict()

games_dict = games.get_dict()
team1 = games_dict['scoreboard']['games'][0]['homeTeam']['teamId']
team2 = games_dict['scoreboard']['games'][0]['awayTeam']['teamId']





season = cumestatsteam.CumeStatsTeam()
print(season)


print(games_dict)
