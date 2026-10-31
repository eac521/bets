-- bets v2: boost + free_bet columns, and a primary key that includes book and user.
-- The old key (player_id, date, over_under, number) let a second book or the other user's bet silently replace the first ("insert or replace").
-- Run once against the live DB when nothing is writing, together with the matching app.py / funcs.py release:
--   sqlite3 nba/data/database/nba.db ".backup nba_pre_bets_v2.db"
--   sqlite3 nba/data/database/nba.db < nba/data/sql/migrate_bets_v2.sql
BEGIN;
CREATE TABLE bets_v2 (
     player_id       TEXT,
     date            TEXT,
     market          TEXT,
     over_under      TEXT,
     number          REAL,
     bet_book        TEXT,
     final_line      REAL,
     bet_amount      REAL,
     user            TEXT,
     boost           REAL DEFAULT 0,     -- profit boost as a fraction: .25 = 25%
     free_bet        INTEGER DEFAULT 0,  -- 1 = stake wasn't ours; a loss costs 0
     PRIMARY KEY (player_id, date, over_under, number, bet_book, user)
     );
INSERT INTO bets_v2 (player_id, date, market, over_under, number, bet_book, final_line, bet_amount, user)
SELECT player_id, date, market, over_under, number, bet_book, final_line, bet_amount, user FROM bets;
DROP TABLE bets;
ALTER TABLE bets_v2 RENAME TO bets;
COMMIT;
