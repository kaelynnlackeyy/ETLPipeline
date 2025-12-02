from datetime import datetime, date
from typing import Dict, Any, List

from schema import covid_schema
import logging

logger = logging.getLogger(__name__)

class data_cleaner:
    @staticmethod
    def extract_nested_value(data: Dict[str, Any], path: str, default=None):
        if not path:
            return default
        keys = path.split(".")
        value: Any = data
        for key in keys:
            value = value.get(key)
        else:
            return default
        return value if value is not None else default

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        if isinstance(value, int):
            try:
                return datetime.strptime(str(value), "%Y%m%d").date()
            except ValueError:
                return None
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%Y%m%d"):
                try:
                    return datetime.strptime(value, fmt).date()
                except ValueError:
                    continue
        return None

    @staticmethod
    def normalize(record: Dict[str, Any], state_code: str, state_name: str) -> Dict[str, Any]:
        normalized = {
            "state_code": state_code,
            "state_name": state_name,
            "date": data_cleaner._parse_date(record.get("date")),
            "cases_total": data_cleaner.extract_nested_value(record, "cases.total", record.get("positive")),
            "cases_confirmed": data_cleaner.extract_nested_value(
                record, "cases.confirmed", record.get("positive")
            
            ),
            "deaths_total": data_cleaner.extract_nested_value(record, "outcomes.death.total", record.get("death")),
            "deaths_confirmed": data_cleaner.extract_nested_value(
                record, "outcomes.death.confirmed", record.get("deathConfirmed")
            ),
            "deaths_probable": data_cleaner.extract_nested_value(
                record, "outcomes.death.probable", record.get("deathProbable")
            ),
            "hospitalized_cumulative": data_cleaner.extract_nested_value(
                record, "outcomes.hospitalized.total", record.get("hospitalizedCumulative")
            ),
            "tests_total": data_cleaner.extract_nested_value(record, "tests.pcr.total", record.get("totalTestResults"))
             }
        return normalized

