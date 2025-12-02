import logging 
from dataextractor import data_extraction
from transform import data_cleaner
from store import sqlstorage
from configuration import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class etl_pipeline:    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.extractor = data_extraction(self.config)
        self.transformer = data_cleaner()
        self.storage = sqlstorage(self.config)
    
    def run_for_state(self, state_code: str):
        try:
            logger.info(f"starting ETL pipeline for {state_code}")
            logger.info(f"extracting data for {state_code}")
            raw_data = self.extractor.fetch_state_daily(state_code.lower())
            if not state_name:
                try:
                    state_info = self.extractor.fetch_state_info(state_code.lower())
                    state_name = state_info.get("name") or state_info.get("state") or state_code
                except Exception:
                    state_name = state_code
            logger.info("cleaning data")
            cleaned_data = self.transformer.clean_and_validate(
                raw_data, state_code, state_name
            )
            logger.info("loading data into database")
            self.storage.insert_records(cleaned_data)
            logger.info(f"completed for {state_code}")
            return cleaned_data            
        except Exception as e:
            logger.error(f"pipeline failed for {state_code}: {e}")
            raise
    
    def run_for_all_states(self, state_code, limit: int = None):
        try:
            logger.info("starting pipeline for all states")
            states_info = self.extractor.fetch_state_info()
            
            if limit:
                states_info = states_info[:limit]
            total_records = 0
            for state in states_info:
                state_code = state.get('state') or state.get('state_code')
                state_name = state.get('name') or state.get('state')
                if not state_code:
                    continue
                
                try:
                    records = self.run_for_state(state_code)
                    total_records += len(records)
                except Exception as e:
                    logger.error(f"failed to process {state_code}: {e}")
                    continue
            
            logger.info(f"pipeline completed. Loaded {total_records} total records")
            return total_records
            
        except Exception as e:
            logger.error(f"pipeline failed: {e}")
            raise
        finally:
            self.extractor.close()
    
    def query_top_cases(self, limit: int = 10):
        return self.storage.get_top_states_by_cases(limit)
    
    def query_top_deaths(self, limit: int = 10):
        return self.storage.get_top_states_by_deaths(limit)
    
    def query_state(self, state_code: str):
        return self.storage.get_latest_by_state(state_code)
    
    def query_time_series(self, state_code: str, days: int = 30):
        return self.storage.get_time_series(state_code, days)
    
    def get_summary(self):
        return self.storage.get_summary_stats()

