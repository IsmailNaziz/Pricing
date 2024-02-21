import os
import unittest
from pathlib import Path
from unittest import TestCase

import pandas as pd

from data_pipe_line.process_data import ProcessData

from datetime import datetime


class TestDataPipeLine(TestCase):
    
    def setUp(self) -> None:
        self.date_format = '%d/%m/%Y'

    @classmethod
    def post_process(cls, df: pd.DataFrame) -> pd.DataFrame:
        # post processed for comparison
        df['new_index'] = df['product'].astype(str) + '_' + df['activity_date'].astype(str)
        df = df.set_index('new_index')
        df = df.sort_index()
        return df

    def test_filter_dates(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10
            },
            {
                'activity_date': datetime.strptime('05/10/2021', self.date_format),
                'product': 1,
                'price': 12
            },
            {
                'activity_date': datetime.strptime('10/10/2021', self.date_format),
                'product': 1,
                'price': 8
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28
            }
        ]
        expected_data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28,
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16,
            },
        ]
        cols_to_compare = ['activity_date', 'product', 'price']
        expected_output_df = pd.DataFrame(expected_data)
        expected_output_df = self.post_process(expected_output_df)
        df = pd.DataFrame(data)
        current_df = ProcessData.filter_dates(df)
        current_df = self.post_process(current_df)
        current_df = current_df[cols_to_compare]
        self.assertTrue(expected_output_df.equals(current_df))


    def test_compute_variation_empty_data(self):
        input_df = expected_output_df = pd.DataFrame()
        current_df = ProcessData.compute_variation(input_df)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")

    def test_compute_variation_one_product(self):
        data = {
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16
            },
        }
        expected_data = {
            {
                'product': 1,
                'variation_type': 'absolute_variation',
                'delta_type': 'from_start_delta',
                'value': 6,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'from_start_delta',
                'value': 60,
            },
            {
                'product': 1,
                'variation_type': 'absolute_variation',
                'delta_type': 'latest_delta',
                'value': -12,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'latest_delta',
                'value': -75,
            },
        }

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")

    def test_compute_variation_two_products(self):
        data = {
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16
            },
            {
                'activity_date': datetime.strptime('12/10/2021', self.date_format),
                'product': 2,
                'price': 300
            },
            {
                'activity_date': datetime.strptime('24/1/2022', self.date_format),
                'product': 2,
                'price': 350
            },
            {
                'activity_date': datetime.strptime('1/6/2022', self.date_format),
                'product': 2,
                'price': 150
            },
        }

        expected_data = {
            {
                'product': 1,
                'variation_type': 'absolute_variation',
                'delta_type': 'from_start_delta',
                'value': 6,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'from_start_delta',
                'value': 60,
            },
            {
                'product': 1,
                'variation_type': 'absolute_variation',
                'delta_type': 'latest_delta',
                'value': -12,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'latest_delta',
                'value': -75,
            },
            {
                'product': 2,
                'variation_type': 'absolute_variation',
                'delta_type': 'from_start_delta',
                'value': -150,
            },
            {
                'product': 2,
                'variation_type': 'relative_variation',
                'delta_type': 'from_start_delta',
                'value': -50,
            },
            {
                'product': 2,
                'variation_type': 'absolute_variation',
                'delta_type': 'latest_delta',
                'value': -200,
            },
            {
                'product': 2,
                'variation_type': 'relative_variation',
                'delta_type': 'latest_delta',
                'value': -57.14,
            },
        }
        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")

    def test_compute_variation_two_rows(self):
        data = {
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': None
            },
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': None
            },
        }

        expected_data = {
            {
                'product': 1,
                'variation_type': 'absolute_variation',
                'delta_type': 'latest_delta',
                'value': 0,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'latest_delta',
                'value': 0,
            },
        }

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")

    def test_compute_variation_one_row(self):
        data = {
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': None
            },
        }

        input_df = pd.DataFrame(data)
        expected_output_df = pd.DataFrame()
        current_df = ProcessData.compute_variation(input_df)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")


    def test_process_data(self):
        input_path = Path(os.path.dirname(__file__)) / 'test_data/test_data.csv'
        output_path = Path(os.path.dirname(__file__)) / 'test_data/current_data.csv'
        expected_file_path = Path(os.path.dirname(__file__)) / 'test_data/test_processed_data.csv'
        process_data = ProcessData(input_path=input_path, output_path=output_path)
        process_data.run()
        self.assertTrue(Path(output_path).exists(), 'file successfully created')
        current_df = pd.read_csv(output_path, sep=';')
        expected_output_df = pd.read_csv(expected_file_path, sep=';')
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")


if __name__ == "__main__":
    unittest.main()
