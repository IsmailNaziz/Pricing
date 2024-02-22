import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from data_pipe_line.process_data import ProcessData
from data_pipe_line.request_data import RequestData
from models import MetricsRequest, Variation

app = FastAPI()
PROCESSED_DATA_PATH = Path(os.path.dirname(__file__)).parent / 'data/processed_data.csv'
DATA_PATH = Path(os.path.dirname(__file__)).parent / 'data/data.csv'


def startup_preparation():
    process_data = ProcessData(input_path=DATA_PATH, output_path=PROCESSED_DATA_PATH)
    process_data.run()



@app.post("/products-variation")
def products_variation(metric_request: MetricsRequest) -> list[Variation]:
    request_data = RequestData(PROCESSED_DATA_PATH)
    return request_data.run(metric_request)


if __name__ == "__main__":
    startup_preparation()
    uvicorn.run(app, host="0.0.0.0", port=8080)
