import pandas as pd

class DataLoader:
    @staticmethod
    def load_df():
        df = pd.read_csv('data/tweets_injected 3.csv')
        return df

