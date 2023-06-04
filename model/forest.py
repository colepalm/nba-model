import pandas as pd


# Averages the value in a dataframe
def create_average_dataframe(df):
    # Get the response as a dictionary
    data = df.get_normalized_dict()

    # Extract the relevant information
    headers = data['headers']
    row_set = data['rowSet']

    # Create a DataFrame using the headers and row set
    df = pd.DataFrame(row_set, columns=headers)

    # Calculate the averages for each column
    averages = df.mean()

    # Create a new DataFrame with the averages
    avg_df = pd.DataFrame(averages).T

    return avg_df


class Forest:

    def __init__(self, log):
        self.log = log

    def create_dataframe(self):
        results = self.log['resultSets'][1]
        df = pd.DataFrame(results['rowSet'], columns=results['headers'])
        avg_df = create_average_dataframe(df)
        return avg_df
