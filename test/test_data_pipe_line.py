import os
import unittest
from pathlib import Path
from unittest import TestCase

import pandas as pd

from data_pipe_line.process_data import ProcessData

from datetime import datetime

from data_pipe_line.request_data import RequestData
from service.models import MetricsRequest


class TestDataPipeLine(TestCase):

    def setUp(self) -> None:
        self.date_format = '%d/%m/%Y'
        self.process_data_current = Path(os.path.dirname(__file__)) / 'test_data/current_data.csv'

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
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16,
                'rank': 3.0
            },
        ]
        expected_output_df = pd.DataFrame(expected_data)
        input_df = pd.DataFrame(data)
        current_df = ProcessData.filter_dates(input_df)
        expected_output_df = self.post_process(expected_output_df)
        current_df = self.post_process(current_df)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_empty_data(self):
        input_df = expected_output_df = pd.DataFrame()
        current_df = ProcessData.compute_variation(input_df)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_one_product(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16,
                'rank': 3.0
            },
        ]
        expected_data = [
            {'product': 1,
             'delta_type': 'from_start_delta',
             'absolute_variation': 6,
             'relative_variation': 60.0},
            {'product': 1,
             'delta_type': 'latest_delta',
             'absolute_variation': -12,
             'relative_variation': -42.86}
        ]

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_two_products(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('01/11/2021', self.date_format),
                'product': 1,
                'price': 28,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('12/2/2022', self.date_format),
                'product': 1,
                'price': 16,
                'rank': 3.0
            },
            {
                'activity_date': datetime.strptime('12/10/2021', self.date_format),
                'product': 2,
                'price': 300,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('24/1/2022', self.date_format),
                'product': 2,
                'price': 350,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('1/6/2022', self.date_format),
                'product': 2,
                'price': 150,
                'rank': 3.0
            },
        ]

        expected_data = [{'product': 1,
                          'delta_type': 'from_start_delta',
                          'absolute_variation': 6,
                          'relative_variation': 60.0},
                         {'product': 1,
                          'delta_type': 'latest_delta',
                          'absolute_variation': -12,
                          'relative_variation': -42.86},
                         {'product': 2,
                          'delta_type': 'from_start_delta',
                          'absolute_variation': -150,
                          'relative_variation': -50.0},
                         {'product': 2,
                          'delta_type': 'latest_delta',
                          'absolute_variation': -200,
                          'relative_variation': -57.14}]

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_two_rows(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('18/12/2021', self.date_format),
                'product': 1,
                'price': 16,
                'rank': 2.0
            },
        ]

        expected_data = [
            {'product': 1,
             'delta_type': 'from_start_delta',
             'absolute_variation': 6,
             'relative_variation': 60.0}]

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_two_rows_two_products(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('18/12/2021', self.date_format),
                'product': 1,
                'price': 16,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('12/10/2021', self.date_format),
                'product': 2,
                'price': 300,
                'rank': 1.0
            },
            {
                'activity_date': datetime.strptime('24/1/2022', self.date_format),
                'product': 2,
                'price': 350,
                'rank': 2.0
            },
            {
                'activity_date': datetime.strptime('1/6/2022', self.date_format),
                'product': 2,
                'price': 150,
                'rank': 3.0
            },
        ]

        expected_data = [
            {'product': 2,
             'delta_type': 'from_start_delta',
             'absolute_variation': -150,
             'relative_variation': -50.0},
            {'product': 2,
             'delta_type': 'latest_delta',
             'absolute_variation': -200,
             'relative_variation': -57.14},
            {'product': 1,
             'delta_type': 'from_start_delta',
             'absolute_variation': 6,
             'relative_variation': 60.0}]

        input_df = pd.DataFrame(data)
        current_df = ProcessData.compute_variation(input_df)
        expected_output_df = pd.DataFrame(expected_data)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_compute_variation_one_row(self):
        data = [
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': 10,
                'rank': 2.0
            },
        ]

        input_df = pd.DataFrame(data)
        expected_output_df = pd.DataFrame()
        current_df = ProcessData.compute_variation(input_df)
        self.assertTrue(expected_output_df.equals(current_df))

    def test_process_data(self):
        input_path = Path(os.path.dirname(__file__)) / 'test_data/test_data.csv'
        output_path = self.process_data_current
        expected_file_path = Path(os.path.dirname(__file__)) / 'test_data/test_processed_data.csv'
        process_data = ProcessData(input_path=input_path, output_path=output_path)
        process_data.run()
        self.assertTrue(Path(output_path).exists(), 'file successfully created')
        current_df = pd.read_csv(output_path, sep=';')
        expected_output_df = pd.read_csv(expected_file_path, sep=';')
        self.assertTrue(expected_output_df.equals(current_df))

    def test_format_df_for_response(self):
        input_path = Path(os.path.dirname(__file__)) / 'test_data/test_processed_data.csv'
        input_df = pd.read_csv(input_path, sep=';')
        metric_request = MetricsRequest(**{'absolute_variation': 5,
                                           'relative_variation': 1.8})
        request_data = RequestData(input_path)
        current_df = request_data.run(metric_request)
        expected_data = [
            {'product': 2,
             'delta_type': 'from_start_delta',
             'absolute_variation': -150,
             'relative_variation': -50.0},
            {'product': 2,
             'delta_type': 'latest_delta',
             'absolute_variation': -200,
             'relative_variation': -57.14},
            {'product': 1,
             'delta_type': 'from_start_delta',
             'absolute_variation': 6,
             'relative_variation': 60.0}]
        # self.assertTrue(expected_output_df.equals(current_df))

    def test_request_data(self):
        pass


if __name__ == "__main__":
    unittest.main()
