import unittest
from datetime import datetime

from configuration import Config
from dataextractor import data_extraction

class DataExtractionTests(unittest.TestCase):
    def setUp(self):
        self.extractor = data_extraction(Config())

    def tearDown(self):
        self.extractor.close()

    def test_get_state_info_returns_codes(self):
        states = self.extractor.get_state_info()
        self.assertGreater(len(states), 0)
        for item in states:
            self.assertIn("state", item)
            self.assertEqual(len(item["state"]), 2)
            self.assertEqual(item["state"].upper(), item["state"])

    def test_get_state_info_returns_codes_incorrect_state(self):
        states = self.extractor.get_state_info()
        self.assertGreater(len(states), 0)
        for item in states:
            self.assertIn("state", item)
            self.assertEqual(len(item["state"]), 2)
            self.assertEqual(item["state"].upper(), item["state"])

    def test_fetch_state_daily_sorted_by_date(self):
        state_code = self.extractor.get_state_info()[0]["state"]
        records = self.extractor.fetch_state_daily(state_code)
        self.assertGreater(len(records), 1)
        parsed_dates = [datetime.fromisoformat(r["date"]) for r in records]
        self.assertEqual(parsed_dates, sorted(parsed_dates, reverse=True))


if __name__ == "__main__":
    unittest.main()