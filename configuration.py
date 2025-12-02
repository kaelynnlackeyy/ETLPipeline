from dataclasses import dataclass

@dataclass
class Config:
    csv_path: str = "all-states-history.csv"
    db: str = "covid_data.db"
    batch: int = 200
    timeout: int = 32
