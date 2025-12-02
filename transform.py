from datetime import datetime, date
from typing import Dict, Any, List
from typing import Optional

from schema import covid_schema
import logging

logger = logging.getLogger(__name__)

class data_cleaner:
    @staticmethod
    def _parse_int(value: Optional[str]) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_date(value: str):
        return datetime.strptime(value, "%Y-%m-%d").date()

    def normalize(self, record: Dict[str, Any], state: str) -> Dict[str, Any]:
        return {
            "state": state,
            "date": self._parse_date(record.get("date")),
            "cases_total": self._parse_int(record.get("positive")),
            "cases_confirmed": self._parse_int(record.get("positiveCasesViral")),
            "deaths_total": self._parse_int(record.get("death")),
            "deaths_confirmed": self._parse_int(record.get("deathConfirmed")),
            "deaths_probable": self._parse_int(record.get("deathProbable")),
            "hospitalized_currently": self._parse_int(record.get("hospitalizedCurrently")),
            "hospitalized_cumulative": self._parse_int(record.get("hospitalizedCumulative")),
            "in_icu_currently": self._parse_int(record.get("inIcuCurrently")),
            "tests_total": self._parse_int(record.get("totalTestResults")),
            }
    def clean_and_validate(self, records: List[Dict[str, Any]], state: str
    ) -> List[covid_schema]:
        cleaned: List[covid_schema] = []
        for record in records:
            try:
                normalized = self.normalize(record, state)
                cleaned.append(covid_schema(**normalized))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "Skipping invalid record for %s on %s: %s",
                    state,
                    record.get("date"),
                    exc,
                )
                continue
        return cleaned