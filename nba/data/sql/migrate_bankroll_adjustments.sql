-- Bankroll money that isn't a model play: live bets, H2H, parlays. Filled by `python -m betting.pikkit <export> --apply`.
-- One row per settled Pikkit bet (bet_id), so re-running overlapping exports doesn't double count. amount = profit (loss negative).
CREATE TABLE IF NOT EXISTS bankroll_adjustments (
     bet_id   TEXT PRIMARY KEY,
     date     TEXT,
     user     TEXT,
     source   TEXT,   -- live / h2h / parlay / other
     amount   REAL,
     note     TEXT
     );
