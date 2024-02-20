import os
import unittest
from pathlib import Path
from unittest import TestCase

import pandas as pd
from starlette.testclient import TestClient
from service.app import app
from service.models import MetricsRequest
from unittest.mock import patch
from data_pipe_line.request_data import RequestData


def mock_sample_processed_data():
    return pd.read_csv(Path(os.path.dirname(__file__)) / 'test_data/test_sample_processed_data.csv')


def mock_processed_data():
    return pd.read_csv(Path(os.path.dirname(__file__)) / 'test_data/test_processed_data.csv')


class TestService(TestCase):

    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch.object(RequestData, 'df', side_effect=mock_sample_processed_data())
    def test_products_variation(self):

        metrics_request_data = {
            'absolute_variation': 5,
            'relative_variation': 25
        }
        metrics_request = MetricsRequest(**metrics_request_data)

        response = self.client.post("/products-variation", json=metrics_request.dict())

        self.assertEqual(response.status_code, 200)
        # choice is not specified
        expected_response_1 = {1: {'variation_type': 'absolute_variation', 'value': 5}}
        expected_response_2 = {1: {'variation_type': 'relative_variation', 'value': 25}}
        self.assertTrue(
                response.json() == expected_response_1 or
                response.json() == expected_response_2
        )


    @patch.object(RequestData, 'df', side_effect=mock_processed_data())
    def test_non_reg_products_variation(self):

        metrics_request_data = {
            'absolute_variation': 5,
            'relative_variation': 25
        }
        metrics_request = MetricsRequest(**metrics_request_data)

        response = self.client.post("/products-variation", json=metrics_request.dict())

        self.assertEqual(response.status_code, 200)
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()