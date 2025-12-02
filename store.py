import sqlite3
from typing import List, Optional, Dict
from contextlib import contextmanager
from datetime import date
from schema import covid_schema
from configuration import config

import logging

logger = logging.getLogger(__name__)

class sqlstorage:    
    def __init__(self, config: config):
        self.db= config.db
        self._init_db()
    
    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS covid_states (
                    state_code TEXT NOT NULL,
                    state_name TEXT NOT NULL,
                    date DATE NOT NULL,
                    cases_total INTEGER,
                    cases_confirmed INTEGER,
                    deaths_total INTEGER,
                    deaths_confirmed INTEGER,
                    deaths_probable INTEGER,
                    hospitalized_currently INTEGER,
                    hospitalized_cumulative INTEGER,
                    in_icu_currently INTEGER,
                    tests_total INTEGER,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (state_code, date)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state_code 
                ON covid_states(state_code)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_date 
                ON covid_states(date DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cases 
                ON covid_states(cases_total DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_deaths 
                ON covid_states(deaths_total DESC)
            """)
            
            conn.commit()
            logger.info("Database initialized")
    
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def insert_records(self, records: List[covid_schema]):
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO covid_states 
                (state_code, state_name, date, cases_total, cases_confirmed,
                 deaths_total, deaths_confirmed, deaths_probable,
                 hospitalized_currently, hospitalized_cumulative,
                 in_icu_currently, tests_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (r.state_code, r.state_name, r.date, r.cases_total, 
                 r.cases_confirmed, r.deaths_total, r.deaths_confirmed,
                 r.deaths_probable, r.hospitalized_currently,
                 r.hospitalized_cumulative, r.in_icu_currently, r.tests_total)
                for r in records
            ])
            conn.commit()
            logger.info(f"Inserted {len(records)} records")
    
    def get_latest_by_state(self, state_code: str) -> Optional[dict]:
        """Get most recent data for a state"""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM covid_states 
                WHERE state_code = ?
                ORDER BY date DESC
                LIMIT 1
            """, (state_code,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def get_top_states_by_cases(self, limit: int = 10, 
                               as_of_date: date = None) -> List[dict]:
        """Get states with highest total cases"""
        with self._get_connection() as conn:
            if as_of_date:
                cursor = conn.execute("""
                    SELECT * FROM covid_states 
                    WHERE date = ?
                    ORDER BY cases_total DESC
                    LIMIT ?
                """, (as_of_date, limit))
            else:
                cursor = conn.execute("""
                    SELECT cs.* FROM covid_states cs
                    INNER JOIN (
                        SELECT state_code, MAX(date) as max_date
                        FROM covid_states
                        GROUP BY state_code
                    ) latest ON cs.state_code = latest.state_code 
                           AND cs.date = latest.max_date
                    ORDER BY cs.cases_total DESC
                    LIMIT ?
                """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_top_states_by_deaths(self, limit: int = 10) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT cs.* FROM covid_states cs
                INNER JOIN (
                    SELECT state_code, MAX(date) as max_date
                    FROM covid_states
                    GROUP BY state_code
                ) latest ON cs.state_code = latest.state_code 
                       AND cs.date = latest.max_date
                ORDER BY cs.deaths_total DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_time_series(self, state_code: str, 
                       days: int = 30) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM covid_states
                WHERE state_code = ?
                ORDER BY date DESC
                LIMIT ?
            """, (state_code, days))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_summary_stats(self) -> dict:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT 
                    COUNT(DISTINCT state_code) as total_states,
                    SUM(cases_total) as total_cases,
                    SUM(deaths_total) as total_deaths,
                    SUM(hospitalized_currently) as total_hospitalized,
                    AVG(cases_total) as avg_cases_per_state,
                    MAX(date) as latest_date
                FROM covid_states cs
                INNER JOIN (
                    SELECT state_code, MAX(date) as max_date
                    FROM covid_states
                    GROUP BY state_code
                ) latest ON cs.state_code = latest.state_code 
                       AND cs.date = latest.max_date
            """)
            return dict(cursor.fetchone())

