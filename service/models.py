from enum import Enum
from typing import Optional
from pydantic import BaseModel

class VariationType(Enum):
    ABSOLUTE_VARIATION = 'absolute_variation'
    RELATIVE_VARIATION = 'relative_variation'

class MetricsRequest(BaseModel):
    absolute_variation: Optional[float]
    relative_variation: Optional[float]

    def __init__(self, **data):
        super().__init__(**data)
        if self.absolute_variation is None and self.relative_variation is None:
            raise ValueError("At least one attribute must be set")

class Variation(BaseModel):
    variation_type: VariationType
    value: float

class ProductsVariation(BaseModel):
    products_variation: dict[int, Variation]