# Changelog
## Unreleased
- Historical odds backfill (`nba/NBAodds.py`): The Odds API historical events and event-odds endpoints into parquet by season / pull_sequence, with an events manifest, resumable task table, credit floor and per-run cap, open-line search, Pikkit bet-time snapshots, player/team alias tables, pilot report and live-lines comparison
- Tests for the backfill helpers (`tests/test_odds_hist.py`)
- Fixed the Fanatics bookmaker key (`fanatcis` -> `fanatics`) so live pulls return Fanatics prices
- `NAME_MAP`: Kenyon Martin Jr, Moe Wagner, Vincent Williams Jr
- `duckdb` added to the environment

## v0.1.0 - 2026-04-15
- Pytest suite with 6 tests, fixtures and integeration markers
- Maekfile with test, test-all and test integeration targets
- Structured error handling on get_open_shot_allowed
- Logging wired into insert_data and derive_opp_data_table
- standardize_names duplication resolved
- Run script started
- Repo cleanup: gitignore expanded and moved to root, bytecode and build artifacts untracked
