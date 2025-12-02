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

     def get_state_info(self) -> List[Dict[str, Any]]:
        url = f"{self.config.api}/states.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()
            states = data.get("data", [])
            logger.info("fetched info for %d states", len(states))
            return states
        except requests.RequestException as e:
            logger.error("failed to fetch states info: %s", e)
            raise

    def fetch_state_daily(self, state_code: str) -> List[Dict[str, Any]]:
        url = f"{self.config.api}/states/{state_code.lower()}/daily.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()
            daily_data = data.get("data", [])
            logger.info("fetched %d daily records for %s", len(daily_data), state_code)
            return daily_data
        except requests.RequestException as e:
            logger.error("failed to fetch data for %s: %s", state_code, e)
            raise
    
    def fetch_state_current(self, state_code: str) -> Dict[str, Any]:
        url = f"{self.config.api}/states/{state_code.lower()}.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error("failed to fetch current data for %s: %s", state_code, e)
            raise
    
    def fetch_us_daily(self) -> List[Dict[str, Any]]:
        url = f"{self.config.apiL}/us/daily.json"
        try:
            response = self.session.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()
            daily_data = data.get("data", [])
            logger.info("fetched %d daily US records", len(daily_data))
        except requests.RequestException as e:
            logger.error("failed to fetch US daily data: %s", e)
            raise
    
    def close(self):
        self.session.close()