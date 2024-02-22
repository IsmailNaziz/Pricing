import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from data_pipe_line.process_data import ProcessData
from data_pipe_line.request_data import RequestData
from service.models import MetricsRequest, Variation
from service.app_config import AppConfig
app = FastAPI()

processed_data_path = Path(os.path.dirname(__file__)).parent / 'data/processed_data.csv'
data_path = Path(os.path.dirname(__file__)).parent / 'data/data.csv'
app_config = AppConfig(processed_data_path=processed_data_path, data_path=data_path)


def startup_preparation():
    process_data = ProcessData(input_path=app_config.data_path, output_path=app_config.processed_data_path)
    process_data.run()



@app.post("/products-variation")
def products_variation(metric_request: MetricsRequest) -> list[Variation]:
    request_data = RequestData(app_config.processed_data_path)
    return request_data.run(metric_request)


if __name__ == "__main__":
    startup_preparation()
    uvicorn.run(app, host="0.0.0.0", port=8080)
