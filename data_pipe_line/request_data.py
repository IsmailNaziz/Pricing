from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from service.models import MetricsRequest, ProductsVariation


@dataclass
class RequestData:
    path: Path
    df: pd.DataFrame

    def __post_init__(self):
        self.df = pd.read_csv(self.path, sep=';')

    def format_df_for_response(self, df: pd.DataFrame) -> ProductsVariation:
        pass

    def filter_from_variation_and_value(self, variation_mode: str, value: float) -> pd.DataFrame:
        pass

    def run(self, metric_request: MetricsRequest) -> ProductsVariation:
        df_absolute = df_relative = pd.DataFrame()
        if metric_request.absolute_variation and metric_request.absolute_variation:
            raise ValueError("At least one attribute must be set")

        if metric_request.absolute_variation:
            df_absolute = self.filter_from_variation_and_value(variation_mode='absolute',
                                                               value=metric_request.absolute_variation)
        if metric_request.relative_variation:
            df_relative = self.filter_from_variation_and_value(variation_mode='relative',
                                                               value=metric_request.relative_variation)

        df = pd.concat([df_relative, df_absolute], axis=1)
        # choice is set here we keep absolute values in priority, we can choose at this point if business needs evolves
        df = df.drop_duplicates(subset='product', keep='first')
        return self.format_df_for_response(df)
