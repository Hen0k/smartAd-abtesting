import os
import pandas as pd


class DataLoader:
    def __init__(self, dir_name, file_name):
        self.dir_name = dir_name
        self.file_name = file_name
        self.cwd = os.getcwd()

    def read_csv(self):
        os.chdir(self.dir_name)
        df = pd.read_csv(self.file_name)
        os.chdir(self.cwd)
        return df

    def read_excel(self):
        os.chdir(self.dir_name)
        df = pd.read_excel(self.file_name, engine='openpyxl')
        os.chdir(self.cwd)
        return df
