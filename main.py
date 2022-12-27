import pandas as pd

from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints.team


# Today's Score Board
games = scoreboard.ScoreBoard()

# dictionary
games_dict = games.get_dict()

print(games_dict)
