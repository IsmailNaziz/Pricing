import unittest
from unittest import TestCase

import pandas as pd

from data_pipe_line.process_data import ProcessData

from datetime import datetime


class TestDataPipeLine(TestCase):
    
    def setUp(self) -> None:
        self.date_format = '%d/%m/%Y'

    
    def test_filter_dates(self):
        data = {
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
        }
        expected_data = {
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

        expected_output_df = pd.DataFrame(expected_data)
        df = pd.DataFrame(data)
        current_df = ProcessData.filter_dates(df)
        self.assertTrue(expected_output_df.equals(current_df), "DataFrames are equal")


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
                'value': 0,
            },
            {
                'product': 1,
                'variation_type': 'relative_variation',
                'delta_type': 'from_start_delta',
                'value': 0,
            },
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

    def test_compute_variation_two_products(self):
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

        pass

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

        pass

    def test_compute_variation_one_row(self):
        data = {
            {
                'activity_date': datetime.strptime('01/10/2021', self.date_format),
                'product': 1,
                'price': None
            },
        }

        pass



    def test_export_processed_df(self):
        pass

    def test_process_data(self):
        pass


if __name__ == "__main__":
    unittest.main()
