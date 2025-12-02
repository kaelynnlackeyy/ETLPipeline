from datetime import date
from typing import Optional
from dataclasses import dataclass

@dataclass
class covid_schema:
    state: str
    date: date
    cases_total: Optional[int] = None
    cases_confirmed: Optional[int] = None
    deaths_total: Optional[int] = None
    deaths_confirmed: Optional[int] = None
    deaths_probable: Optional[int] = None
    hospitalized_currently: Optional[int] = None
    hospitalized_cumulative: Optional[int] = None
    in_icu_currently: Optional[int] = None
    tests_total: Optional[int] = None

    def __post_init__(self):
        if self.state:
            self.state= self.state.upper()
        if not self.state or len(self.state) != 2:
            raise ValueError("state must be a 2-letter code")

        for field_name in [
            "cases_total",
            "cases_confirmed",
            "deaths_total",
            "deaths_confirmed",
            "deaths_probable",
            "hospitalized_currently",
            "hospitalized_cumulative",
            "in_icu_currently",
            "tests_total",
        ]:
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")