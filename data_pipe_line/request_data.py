from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
pd.options.mode.chained_assignment = None

from service.models import MetricsRequest, Variation


@dataclass
class RequestData:
    path: Path
    df: Optional[pd.DataFrame] = None

    def __post_init__(self):
        self.df = pd.read_csv(self.path, sep=';')
    
    @classmethod
    def filter_metric(cls, df: pd.DataFrame, variation_name: str, value: float) -> pd.DataFrame:
        col_to_keep = ['product', 'delta_type', 'variation_type', variation_name]
        df_res = df[df['absolute_variation'] >= value]
        df_res['variation_type'] = variation_name
        df_res = df_res[col_to_keep]
        df_res = df_res.rename(columns={variation_name: 'value'})
        return df_res
        
    def run(self, metric_request: MetricsRequest) -> list[Variation]:

        if not metric_request.absolute_variation and not metric_request.absolute_variation:
            raise ValueError("At least one attribute must be set")

        df_absolute = df_relative = pd.DataFrame()
        if metric_request.absolute_variation:
            df_absolute = self.filter_metric(df=self.df,
                                             variation_name='absolute_variation',
                                             value=metric_request.absolute_variation)

        if metric_request.relative_variation:
            df_relative = self.filter_metric(df=self.df,
                                             variation_name='relative_variation',
                                             value=metric_request.relative_variation)

        res_df = pd.concat([df_relative, df_absolute], axis=0)
        # choices are set here:
        # we keep absolute values in priority, we can choose at this point if business needs evolves
        # we keep the first appearance of delta of every product
        res_df = res_df.drop_duplicates(subset='product', keep='first')
        result = [Variation(**record) for record in res_df.to_dict(orient='records')]
        return result
