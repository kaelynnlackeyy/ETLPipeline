# COVID-19 CSV ETL Pipeline
## Challenge #1: Data Engineering - Open Source Data Pipeline

This project runs a simple ETL pipeline that loads state-level COVID-19 history from the provided `all-states-history.csv` file (sourced from the public Covid Tracking Project dataset), stores it in a local SQLite database, and exposes a small CLI for exploring the data.

## Prerequisites
- Python 3.9+
- `pip install -r requirements.txt` (installs CLI + test dependencies).

## Quick start
1. Ensure `all-states-history.csv` is present in the repository root (it is included by default).
2. Run one of the ETL commands:
   - Load a single state:
     ```bash
     python app.py fetch --state CA
     ```
   - Load all states (optionally limit how many to process):
     ```bash
     python app.py fetch --all-states --limit 5
     ```
3. Explore the stored data:
   - Top states by cases or deaths:
     ```bash
     python app.py top --metric cases --limit 10
     python app.py top --metric deaths --limit 10
     ```
   - Latest record for a state:
     ```bash
     python app.py state NY
     ```
   - Recent timeline for a state:
     ```bash
     python app.py timeline TX --days 14
     ```
  - Generate a quick visualization for a state (saves a PNG chart):
     ```bash
     python app.py visualize CA --metric cases --days 30 --output ca_cases.png
     ```
   - Summary statistics across all loaded states:
     ```bash
     python app.py summary
     ```

## Schema and transformations
- **Schema**: the table `covid_states` stores `state`, `date`, totals for cases and deaths, confirmed/probable breakdowns, hospitalization counts, ICU counts, and total tests.
- **Transformations**: the transformer parses dates (`YYYY-MM-DD`) and coerces numeric fields to integers. Missing/blank/invalid numeric values become `NULL` and negative numbers are rejected. Invalid rows are skipped with a warning.
- **Storage**: records are persisted to SQLite (default `covid_data.db`) with indexes for fast queries.

## Testing
- Install dev dependencies: `pip install -r requirements.txt`
- Run the unit tests (five total, covering extractor, transformer, and pipeline behaviors):
  ```bash
  python -m pytest
  python -m unittest discover -s tests
  ```

## Implementation notes
- All data comes exclusively from the local CSV—no external API calls are made.
- Missing or blank values in the CSV are treated as `null` and ignored by validation when appropriate.
- Records are validated with lightweight dataclass checks before being inserted into SQLite. Invalid rows are skipped with a warning.
- The database file defaults to `covid_data.db` in the project root; delete it to reload from scratch.
