# Changelog
## Unreleased
- Historical odds backfill (`nba/NBAodds.py`; books include the Kalshi and Novig exchanges and BetRivers): The Odds API historical events and event-odds endpoints into parquet by season / pull_sequence, with an events manifest, resumable task table, credit floor and per-run cap, open-line search, Pikkit bet-time snapshots, player/team alias tables, pilot report and live-lines comparison
- `--extra`: first basket, first team basket and Q1 points into `nba/data/odds_hist_extra/`, reusing the core run's events and open times; markets the API rejects are dropped before the run
- Tests for the backfill helpers (`tests/test_odds_hist.py`)
- Fixed the Fanatics bookmaker key (`fanatcis` -> `fanatics`) so live pulls return Fanatics prices
- `NAME_MAP`: Kenyon Martin Jr, Moe Wagner, Vincent Williams Jr
- `duckdb` added to the environment
- Live odds pulled as append-only parquet snapshots (nba/data/odds_live): open after the morning run, midday at 14:00 ET, close at tip - 20 min, manual from the app's Refresh Lines button
- 10 books, the same as the backfill (FanDuel, DraftKings, BetMGM, Fanatics, theScore, Caesars, Pinnacle, Kalshi, BetRivers, Novig) and 9 markets (threes, points, PRA, main and alternate; first basket, first team basket, Q1 points)
- Prices no longer averaged across main and alternate markets (pivot_table mean): the better payout is used
- Slate window computed at call time and ends 6am ET
- App reads lines from snapshots (no API call on rerun); best book is FanDuel, DraftKings or BetMGM; Fanatics shown when it beats them; theScore is information only; wager follows the best book
- Daily bankroll: settled from box scores per user, 80% of profits kept (20% of each new profit high set aside)
## v0.1.0 - 2026-04-15
- Pytest suite with 6 tests, fixtures and integeration markers
- Maekfile with test, test-all and test integeration targets
- Structured error handling on get_open_shot_allowed
- Logging wired into insert_data and derive_opp_data_table
- standardize_names duplication resolved
- Run script started
- Repo cleanup: gitignore expanded and moved to root, bytecode and build artifacts untracked
