# extracts data using a public covid api @ covidtracking
import logging
from configuration import Config
from typing import List, Dict, Any

import requests

logger = logging.getLogger(__name__)


class data_extraction:
    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config.user_agent})

    def _parse_response_payload(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "data" in payload:
            return payload.get("data")
        return payload

    def fetch_state_info(self, state_code: str) -> Dict[str, Any]:
        url = f"{self.config.api}/states/{state_code.lower()}/info.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            return self._parse_response_payload(response.json())
        except requests.RequestException as e:
            logger.error(f"failed to fetch state info for {state_code}: {e}")
            raise

    def fetch_state_daily(self, state_code: str) -> List[Dict[str, Any]]:
        url = f"{self.config.api}/states/{state_code.lower()}/info.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            print(response)
            response.raise_for_status()
            data = self._parse_response_payload(response.json())
            if data is None:
                data = []
            logger.info(f"fetched {len(data)} daily records for {state_code}")
            return data
        except requests.RequestException as e:
            logger.error("failed to fetch data for %s: %s", state_code, e)
            raise
    
    def fetch_state_current(self, state_code: str) -> Dict[str, Any]:
        url = f"{self.config.api}/states/{state_code.lower()}/current.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            return self._parse_response_payload(response.json())
        except requests.RequestException as e:
            logger.error("failed to fetch current data for %s: %s", state_code, e)
            raise
    
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
        self.session.close()