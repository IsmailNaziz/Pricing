from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from service.models import MetricsRequest, ProductsVariation, Variation


@dataclass
class RequestData:
    path: Path
    df: Optional[pd.DataFrame] = None

    def __post_init__(self):
        self.df = pd.read_csv(self.path, sep=';')


    def run(self, metric_request: MetricsRequest) -> ProductsVariation:

        if not metric_request.absolute_variation and not metric_request.absolute_variation:
            raise ValueError("At least one attribute must be set")

        df_absolute = df_relative = pd.DataFrame()
        col_to_keep = ['product', 'delta_type', 'variation_type', 'relative_variation']
        if metric_request.absolute_variation:
            df_absolute = self.df[self.df['absolute_variation'] >= metric_request.absolute_variation]
            df_absolute['variation_type'] = 'absolute_variation'
            df_absolute = df_absolute[col_to_keep]
            df_absolute = df_absolute.rename(columns={'absolute_variation': 'value'})

        if metric_request.relative_variation:
            df_relative = self.df[self.df['relative_variation'] >= metric_request.relative_variation]
            df_relative['variation_type'] = 'relative_variation'
            df_relative = df_relative[col_to_keep]
            df_relative = df_relative.rename(columns={'relative_variation': 'value'})

        res_df = pd.concat([df_relative, df_absolute], axis=0)
        # choice is set here we keep absolute values in priority, we can choose at this point if business needs evolves
        res_df = res_df.drop_duplicates(subset='product', keep='first')
        formatted_dict = {record['product']: {attribute: value for attribute, value in record.items() if attribute != 'product'}
                          for record in res_df.to_dict(orient='records')}
        converted_dic = {product: Variation(**variation_dic) for product, variation_dic in formatted_dict.items()}
        res = ProductsVariation(**converted_dic)
        return self.format_df_for_response(df)
