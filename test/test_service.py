import json
import os
import unittest
from pathlib import Path
from unittest import TestCase

import pandas as pd
from starlette.testclient import TestClient

import service
from service.app import app
from service.models import MetricsRequest
from unittest.mock import patch


sample_data_path = Path(os.path.dirname(__file__)).parent / 'data/sample_processed_data.csv'
class TestService(TestCase):

    def setUp(self) -> None:
        self.client = TestClient(app)

    @patch('service.app_config.AppConfig')
    def test_products_variation(self, app_mock):
        app_mock.processed_data_path.return_value = sample_data_path
        metrics_request_data = {
            'absolute_variation': 5,
            'relative_variation': -1.5
        }
        metrics_request = MetricsRequest(**metrics_request_data)

        response = self.client.post("/products-variation", json=metrics_request.dict())
        current_result = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()