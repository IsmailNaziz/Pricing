from pathlib import Path
from pydantic import BaseModel

class AppConfig(BaseModel):
    processed_data_path: Path
    data_path: Path
