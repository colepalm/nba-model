import pandas as pd


class Forest:

    def __init__(self, log):
        self.log = log

    def create_dataframe(self):
        results = self.log['resultSets'][0]
        df = pd.DataFrame(results['rowSet'], columns=results['headers'])
        return df
