from fastapi import FastAPI

from models import MetricsRequest, Variation

app = FastAPI()

@app.post("/products-variation")
def index(data: MetricsRequest) -> list[Variation]:
    pass