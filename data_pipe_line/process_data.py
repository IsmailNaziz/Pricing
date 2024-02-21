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
        if df.empty:
            return df
        res_df = pd.DataFrame()
        for id_product, sub_df in df.groupby(by='product'):
            sub_df["rank"] = sub_df["activity_date"].rank(method='dense')
            # depending on the size of sub_df, we select indexes to keep: [-2,-1,0], [-1,0], [0]
            idx_rank_to_keep = [i for i in range(-min(len(sub_df), 2), 1)]
            sub_df = sub_df.iloc[idx_rank_to_keep]
            sub_df = sub_df.drop_duplicates(subset=['product', 'activity_date'], keep='first')
            res_df = pd.concat([res_df, sub_df], axis=1)
        return res_df

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



