from dataclasses import dataclass
from pathlib import Path

import pandas as pd

@dataclass
class ProcessData:
    path: Path
    df: pd.DataFrame

    def __post_init__(self):
        self.df = pd.read_csv(self.path, sep=';')


    @classmethod
    def filter_dates(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters df
        to keep only a maximum of 3 rows per product
        first, before last, last date
        :return: None
        """
        pass

    @classmethod
    def compute_variation(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        :param df: 
        :return: 
        """
        pass


    @classmethod
    def export_processed_df(cls, df) -> None:
        """
        :return:
        """
        pass

    def run(self):
        res_df = self.filter_dates(self.df)
        res_df = self.compute_variation(res_df)
        self.export_processed_df(res_df)



