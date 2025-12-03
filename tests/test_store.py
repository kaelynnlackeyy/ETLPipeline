import os
import tempfile
import unittest
from datetime import date

from configuration import Config
from schema import covid_schema
from store import sqlstorage


class SQLStorageTests(unittest.TestCase):
    def setUp(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        self.db_path = path
        self.storage = sqlstorage(Config(db=self.db_path))
    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _record(self, state: str, record_date: date, cases: int, deaths: int = 0):
        return covid_schema(
            state=state,
            date=record_date,
            cases_total=cases,
            deaths_total=deaths,
        )

    def test_insert_and_get_latest_by_state(self):
        records = [
            self._record("CA", date(2021, 3, 6), 10, 1),
            self._record("CA", date(2021, 3, 7), 20, 2),
        ]
        self.storage.insert_records(records)
        latest = self.storage.get_latest_by_state("CA")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["date"], "2021-03-07")
        self.assertEqual(latest["cases_total"], 20)

    def test_get_top_states_by_cases_returns_sorted(self):
        records = [
            self._record("TX", date(2021, 3,7), 100, 10),
            self._record("CA", date(2021, 3,7), 200, 20),
            self._record("NY", date(2021, 3,7), 150, 15),
        ]
        self.storage.insert_records(records)

        top_states = self.storage.get_top_states_by_cases(limit=2)
        print(top_states)
        print([s["state"] for s in top_states], ["CA", "NY"])
        self.assertEqual(len(top_states), 2)
        self.assertEqual([s["state"] for s in top_states], ["CA", "NY"])

    def test_get_time_series_limits_results(self):
        records = [
            self._record("ny",date(2021,3,5), 10, 1),
            self._record("WA", date(2021, 3,6), 12, 1),
            self._record("wa", date(2021,3,7), 15, 2),
        ]
        self.storage.insert_records(records)

        series = self.storage.get_time_series("WA", days=2)
        self.assertEqual(len(series), 2)
        self.assertEqual([r["date"] for r in series],["2021-03-07", "2021-03-06"])


if __name__ == "__main__":
    unittest.main()