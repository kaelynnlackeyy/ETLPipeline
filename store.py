import sqlite3
from typing import List, Optional, Dict
from contextlib import contextmanager
from datetime import date
from schema import covid_schema
from configuration import Config

import logging

logger = logging.getLogger(__name__)

class sqlstorage:    
    def __init__(self, config: Config):
        self.db= config.db
        self._init_db()
    
    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS covid_states (
                    state TEXT NOT NULL,
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
                    PRIMARY KEY (state, date)
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_state 
                ON covid_states(state)
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
            logger.info("database initialized")

    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    
    def insert_records(self, records: List[covid_schema]):
        with self._get_connection() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO covid_states 
                (state, date, cases_total, cases_confirmed,
                deaths_total, deaths_confirmed, deaths_probable,
                hospitalized_currently, hospitalized_cumulative,
                in_icu_currently, tests_total)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                (r.state, r.date, r.cases_total,
                 r.cases_confirmed, r.deaths_total, r.deaths_confirmed,
                 r.deaths_probable, r.hospitalized_currently,
                 r.hospitalized_cumulative, r.in_icu_currently, r.tests_total)
                for r in records
            ])
            conn.commit()
            logger.info(f"Inserted {len(records)} records")
    
    def get_latest_by_state(self, state: str) -> Optional[dict]:
        """Get most recent data for a state"""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM covid_states 
                WHERE state = ?
                ORDER BY date DESC
                LIMIT 1
            """, (state,))
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
                        SELECT state, MAX(date) as max_date
                        FROM covid_states
                        GROUP BY state
                    ) latest ON cs.state = latest.state 
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
                    SELECT state, MAX(date) as max_date
                    FROM covid_states
                    GROUP BY state
                ) latest ON cs.state = latest.state 
                       AND cs.date = latest.max_date
                ORDER BY cs.deaths_total DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_time_series(self, state: str,days: int = 30) -> List[dict]:
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM covid_states
                WHERE state = ?
                ORDER BY date DESC
                LIMIT ?
            """, (state.upper(), days))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_summary_stats(self) -> dict:
        with self._get_connection() as conn:
            
            cursor = conn.execute("""
                SELECT
                    COUNT(DISTINCT cs.state) as total_states,
                    SUM(cs.cases_total) as total_cases,
                    SUM(cs.deaths_total) as total_deaths,
                    SUM(cs.hospitalized_currently) as total_hospitalized,
                    AVG(cs.cases_total) as avg_cases_per_state,
                    MAX(cs.date) as latest_date
                FROM covid_states cs
                INNER JOIN (
                    SELECT state, MAX(date) as max_date
                    FROM covid_states
                    GROUP BY state
                    ) latest ON cs.state = latest.state
                       AND cs.date = latest.max_date
            """)
        
            return dict(cursor.fetchone())

