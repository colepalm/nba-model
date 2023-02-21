import json

import pandas as pd


class Forest:

    def __init__(self, log):
        self.log = log

    def create_dataframe(self):
        df = pd.read_json(json.dumps(self.log))
        print(df)

