class config:
    api_base: str="https://api.covidtracking.com/v2"
    db: str="covid_data.db"
    timeout: int=32
    batch: int=200