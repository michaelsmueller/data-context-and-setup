import os
import pandas as pd


class Olist:
    def get_data(self):
        """
        This function returns a Python dict.
        Its keys should be 'sellers', 'orders', 'order_items' etc...
        Its values should be pandas.DataFrames loaded from csv files
        """
        current_dir = os.path.dirname(__file__)
        raw_path = os.path.join(current_dir, "../data/csv")
        csv_path = os.path.abspath(raw_path)

        file_names = list(os.listdir(csv_path))
        key_names = [
            file.replace(".csv", "").replace("olist_", "").replace("_dataset", "")
            for file in file_names
        ]

        data = {}
        for key, file in zip(key_names, file_names):
            full_path = os.path.join(csv_path, file)
            data[key] = pd.read_csv(full_path)

        return data

    def ping(self):
        """
        You call ping I print pong.
        """
        print("pong")
