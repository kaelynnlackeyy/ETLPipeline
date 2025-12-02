from configuration import config
from dataextractor import data_extraction
from schema import covid_schema

import sqlite3
from unittest.mock import Mock, patch
from datetime import datetime, date
import tempfile
import os

def get_temp_config():
    """Create temporary config for testing"""
    f = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    db= f.name
    f.close()
    return config(DB_PATH=db), db

def cleanup_temp_db(db):
    """Clean up temporary database"""
    if os.path.exists(db):
        os.remove(db)


def get_sample_api_response():
    """Sample API response data from COVID Tracking Project"""
    return [
        {
            'date': '2021-03-07',
            'cases': {
                'total': 3500000,
                'confirmed': 3400000
            },
            'outcomes': {
                'death': {
                    'total': 50000,
                    'confirmed': 48000,
                    'probable': 2000
                },
                'hospitalized': {
                    'total': 200000
                }
            },
            'hospitalization': {
                'hospitalized': {
                    'currently': 5000
                },
                'in_icu': {
                    'currently': 1200
                }
            },
            'tests': {
                'pcr': {
                    'total': 45000000
                }
            }
        },
        {
            'date': '2021-03-06',
            'cases': {
                'total': 3480000,
                'confirmed': 3380000
            },
            'outcomes': {
                'death': {
                    'total': 49800,
                    'confirmed': 47800,
                    'probable': 2000
                },
                'hospitalized': {
                    'total': 198000
                }
            },
            'hospitalization': {
                'hospitalized': {
                    'currently': 5100
                },
                'in_icu': {
                    'currently': 1250
                }
            },
            'tests': {
                'pcr': {
                    'total': 44500000
                }
            }
        }
    ]


def get_sample_states_info():
    """Sample states info response"""
    return [
        {
            'state_code': 'CA',
            'name': 'California'
        },
        {
            'state_code': 'TX',
            'name': 'Texas'
        }
    ]


def get_sample_records():
    """Sample validated records"""
    return [
        covid_schema(
            state_code='CA',
            state_name='California',
            date=date(2021, 3, 7),
            cases_total=3500000,
            cases_confirmed=3400000,
            deaths_total=50000,
            deaths_confirmed=48000,
            hospitalized_currently=5000,
            tests_total=45000000
        ),
        covid_schema(
            state_code='CA',
            state_name='California',
            date=date(2021, 3, 6),
            cases_total=3480000,
            cases_confirmed=3380000,
            deaths_total=49800,
            deaths_confirmed=47800,
            hospitalized_currently=5100,
            tests_total=44500000
        )
    ]

def test_get_state_info():
    """Test successful states info fetch"""
    sample_states_info = get_sample_states_info()
    config = config()
    extractor = data_extraction(config)
    
    with patch.object(extractor.session, 'get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = {'data': sample_states_info}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        result = extractor.get_state_info()
        assert len(result) == 2, f"Expected 2 states, got {len(result)}"
        assert result[0]['state_code'] == 'CA', f"Expected CA, got {result[0]['state_code']}"
        mock_get.assert_called_once()
    
    print("test_fetch_states_info_success passed")


test_get_state_info()
     