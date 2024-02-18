from fastapi import FastAPI

from models import Metrics, ProductsVariation

app = FastAPI()

@app.get("/")
def index(data: Metrics) -> ProductsVariation:
    pass