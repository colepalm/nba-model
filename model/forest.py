import pandas as pd


class Forest:

    def __init__(self, log):
        self.log = log

    def create_dataframe(self):
        rowSet = self.log['resultSets'][0]['rowSet']
        df = pd.json_normalize(rowSet)
        return df
