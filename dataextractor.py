# extracts data using a public covid api @ covidtracking
import requests
import logging
from configuration import config
logger = logging.getLogger(__name__)


class data_extraction:
    def __init__(self, config: config):
        self.config=config
        self.session=requests.Session()

    def get_state_info(self):
        url=f"{self.config.API}/states.json" 
        try:
            response = self.session.get(url, timeout=self.config.REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            states = data.get('data', [])
            logger.info(f"Fetched info for {len(states)} states")
            return states
        except requests.RequestException as e:
            logger.error(f"Failed to fetch states info: {e}")
            raise