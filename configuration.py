from dataclasses import dataclass

@dataclass
class Config:
    api: str = "https://api.covidtracking.com/v1"
    db: str = "covid_data.db"
    timeout: int = 32
    batch: int = 200
    user_agent: str = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
