from fastapi import FastAPI

from models import MetricsRequest, ProductsVariation

app = FastAPI()

@app.get("/")
def index(data: MetricsRequest) -> ProductsVariation:
    pass