from dataclasses import dataclass

@dataclass
class Config:
    api: str="https://api.covidtracking.com/v2"
    db: str="covid_data.db"
    timeout: int=32
    batch: int=200
    user_agent: str = "ETLPipeline/1.0 (https://example.com)"
