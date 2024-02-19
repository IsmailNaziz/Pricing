from fastapi import FastAPI

from models import MetricsRequest, ProductsVariation

app = FastAPI()

@app.post("/products-variation")
def index(data: MetricsRequest) -> ProductsVariation:
    pass