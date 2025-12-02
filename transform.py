from typing import Dict, Any
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
        normalized = {
            'state_code': state_code,
            'state_name': state_name,
            'date': record.get('date'),
            'cases_total': data_cleaner.extract_nested_value(
                record, 'cases.total'
            ),
            'cases_confirmed': data_cleaner.extract_nested_value(
                record, 'cases.confirmed'
            ),
            'deaths_total': data_cleaner.extract_nested_value(
                record, 'outcomes.death.total'
            ),
            'deaths_confirmed': data_cleaner.extract_nested_value(
                record, 'outcomes.death.confirmed'
            ),
            'deaths_probable':  data_cleaner.extract_nested_value(
                record, 'outcomes.death.probable'
            ),
            'hospitalized_currently': data_cleaner.extract_nested_value(
                record, 'hospitalization.hospitalized.currently'
            ),
            'hospitalized_cumulative':  data_cleaner.extract_nested_value(
                record, 'outcomes.hospitalized.total'
            ),
            'in_icu_currently':  data_cleaner.extract_nested_value(
                record, 'hospitalization.in_icu.currently'
            ),
            'tests_total':  data_cleaner.extract_nested_value(
                record, 'tests.pcr.total'
            )
        }
        return normalized
    

