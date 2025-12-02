# extracts data using a public covid api @ covidtracking
import logging
from configuration import Config
from typing import List, Dict, Any
import csv
from collections import defaultdict
import requests

logger = logging.getLogger(__name__)


class data_extraction:
    def __init__(self, config: Config):
        self.config = config
        self._data_by_state: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._load_data()

    def _parse_response_payload(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "data" in payload:
            return payload.get("data")
        return payload

    def _load_data(self) -> None:
        try:
            with open(self.config.csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    state = row.get("state")
                    if not state:
                        continue
                    # Normalize keys we care about; keep raw strings for cleaning
                    filtered_row = {
                        "date": row.get("date"),
                        "state": state,
                        "positive": row.get("positive"),
                        "positiveCasesViral": row.get("positiveCasesViral"),
                        "death": row.get("death"),
                        "deathConfirmed": row.get("deathConfirmed"),
                        "deathProbable": row.get("deathProbable"),
                        "hospitalizedCurrently": row.get("hospitalizedCurrently"),
                        "hospitalizedCumulative": row.get("hospitalizedCumulative"),
                        "inIcuCurrently": row.get("inIcuCurrently"),
                        "totalTestResults": row.get("totalTestResults"),
                    }
                    self._data_by_state[state].append(filtered_row)
            # Ensure rows are ordered newest-first for predictable inserts
            for state, records in self._data_by_state.items():
                self._data_by_state[state] = sorted(
                    records, key=lambda r: r.get("date", ""), reverse=True
                )
            logger.info(
                "Loaded %d states worth of data from %s",
                len(self._data_by_state),
                self.config.csv_path,
            )
        except FileNotFoundError:
            logger.error("CSV file not found at %s", self.config.csv_path)
            raise
        except Exception as exc:
            logger.error("Failed to load CSV data: %s", exc)
            raise

    def get_state_info(self) -> List[Dict[str, str]]:
        return [
            {"state": code, "state": code}
            for code in sorted(self._data_by_state.keys())
        ]

    def fetch_state_daily(self, state: str) -> List[Dict[str, Any]]:
        return list(self._data_by_state.get(state, []))
    
    def fetch_state_current(self, state: str) -> Dict[str, Any]:
        records = self._data_by_state.get(state, [])
        return records[0] if records else {}
    
    def fetch_us_daily(self) -> List[Dict[str, Any]]:
        url = f"{self.config.api}/us/daily.json"

        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            data = self._parse_response_payload(response.json())
            if data is None:
                data = []
            logger.info(f"fetched {len(data)} daily US records")
            return data
        except requests.RequestException as e:
            logger.error("failed to fetch US daily data: %s", e)
            raise
    
    def close(self):
        return None