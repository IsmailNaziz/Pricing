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
    def filter_rows_based_on_rank(cls, group):
        if len(group) < 2:
            return group
        first_row = group[group['rank'] == group['rank'].min()]
        last_row = group[group['rank'] == group['rank'].max()]
        before_last_row = group[group['rank'] == group['rank'].max() - 1]
        return pd.concat([first_row, before_last_row, last_row])

    @classmethod
    def filter_dates(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filters df
        to keep only a maximum of 3 rows per product
        first, before last, last date
        adds a column rank
        :return: None
        """
        if df.empty:
            return df

        df['asc_rank'] = df.groupby('product')['activity_date'].rank(ascending=True, method='dense')
        df['desc_rank'] = df.groupby('product')['activity_date'].rank(ascending=False, method='dense')
        df = df[(df['asc_rank'] == 1.0) | (df['desc_rank'] == 1.0) | (df['desc_rank'] == 2.0)]
        df = df.drop_duplicates(subset=['product', 'activity_date'], keep='first')
        df['rank'] = df.groupby('product')['activity_date'].rank()
        res_df = df.drop(columns=['asc_rank', 'desc_rank'])
        return res_df



    @classmethod
    def compute_variation(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        explain how it works
        """
        if df.empty:
            return df
        cols_to_keep = ['product', 'delta_type', 'absolute_variation', 'relative_variation']
        # case 3 rows
        three_records_auto_join_df = pd.merge(df[df['rank'] == 3], df[df['rank'] != 3],
                                              on='product',
                                              how='inner')

        # case 2 rows
        filtered_product_ids = df[df['rank'] == 3]['product']
        two_records_df = df[~df['product'].isin(filtered_product_ids)]
        two_records_auto_join_df = pd.merge(two_records_df[two_records_df['rank'] == 2],
                                            two_records_df[two_records_df['rank'] != 2],
                                            on='product',
                                            how='inner')

        auto_join_df = pd.concat([three_records_auto_join_df, two_records_auto_join_df], axis=0)
        auto_join_df['delta_type'] = auto_join_df['rank_y'].apply(lambda rank: 'delta_from_start' if rank == 1 else 'delta_latest')
        auto_join_df['absolute_variation'] = auto_join_df['price_x'] - auto_join_df['price_y']
        auto_join_df['relative_variation'] = round((auto_join_df['absolute_variation']*100)/auto_join_df['price_y'], 2)
        if auto_join_df.empty:
            return pd.DataFrame()

        return auto_join_df[cols_to_keep]

    def run(self):
        res_df = self.filter_dates(self.df)
        res_df = self.compute_variation(res_df)
        res_df.to_csv(self.output_path, sep=';', index=False)
