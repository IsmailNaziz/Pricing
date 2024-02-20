from dataclasses import dataclass
from pathlib import Path

import pandas as pd

@dataclass
class ProcessData:
    input_path: Path
    output_path: Path
    df: pd.DataFrame = pd.DataFrame()

    def __post_init__(self):
        self.df = pd.read_csv(self.input_path, sep=';')


    @classmethod
    def filter_dates(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters df
        to keep only a maximum of 3 rows per product
        first, before last, last date
        :return: None
        """
        return pd.DataFrame()

    @classmethod
    def compute_variation(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        explain how it works
        """
        return pd.DataFrame()


    def run(self):
        res_df = self.filter_dates(self.df)
        res_df = self.compute_variation(res_df)
        res_df.to_csv(self.output_path, sep=';', index=False)



