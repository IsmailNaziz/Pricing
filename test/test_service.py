import json
import os
import unittest
from pathlib import Path
from unittest import TestCase

from starlette.testclient import TestClient

from service.app import app, startup_preparation
from service.app_config import AppConfig
from service.models import MetricsRequest


class TestService(TestCase):

    def setUp(self) -> None:
        processed_data_path = Path(os.path.dirname(__file__)).parent / 'data/processed_data.csv'
        data_path = Path(os.path.dirname(__file__)).parent / 'data/sample_data.csv'
        app_config = AppConfig(processed_data_path=processed_data_path, data_path=data_path)
        startup_preparation(app_config)
        self.client = TestClient(app)


    def test_products_variation(self):
        metrics_request_data = {
            'absolute_variation': 5,
            'relative_variation': 25
        }
        metrics_request = MetricsRequest(**metrics_request_data)

        response = self.client.post("/products-variation", json=metrics_request.dict())
        current_result = response.json()
        expected_result = [{'product': 1, 'variation_type': 'absolute_variation', 'value': 15.0, 'delta_type': 'delta_from_start'}]
        self.assertEqual(response.status_code, 200)
        self.assertTrue(current_result==expected_result)


if __name__ == "__main__":
    unittest.main()