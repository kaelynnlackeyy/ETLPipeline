from typing import List, Dict, Any
from datetime import datetime, date
from schema import CovidStateRecord
from pydantic import ValidationError
import logging

logger = logging.getLogger(__name__)

class data_cleaner:
    def extract_nested_values(data,path,default=None):
        keys=path.split('')
        value=data
        for key in keys:
            if isinstance(value, dict):
                value=value.get(key)
            else:
                return default
        return value
    
    def normalize(record: Dict[str, Any],
                state_code: str, 
                state_name: str) -> Dict[str, Any]:
        normalized=
