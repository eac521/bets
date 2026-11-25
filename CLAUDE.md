# CLAUDE.md — NBA betting models

Live NBA player-prop betting system. Core model: MNLogit (statsmodels) predicting a full PMF of threes made (0 to cap) per player-game, priced against FanDuel / DraftKings / BetMGM, sized with 1/8 Kelly on a compounding bankroll. Streamlit app + a 5:30am / 11:30am pipeline run on the Mac mini. Points model and a second "who's playing" layer are in development on `dev`.

This system makes real-money decisions every day. When unsure, stop and ask.

## Machines, folders, branches

| Machine | Folder | Branch | Role |
|---|---|---|---|
| Mac mini | `~/code/bets` | `main` | **Prod.** What Streamlit and the pipeline run. Changes only by merging `dev` (or a documented hotfix). |
| Mac mini | `~/code/bets-dev` | `dev` | App edits, pipeline/ETL fixes, database work, long jobs (odds backfill). |
| MacBook | `~/code/bets-dev` | `dev` | Analysis only: EDA, notebooks, pricing research, offseason retrain. |

- Check which machine and folder you're in before editing (`hostname`, `pwd`, `git branch --show-current`).
- On the MacBook: never edit `app.py`, `betting/`, or the pipeline; never check out `main`; no hotfixes.
- Never edit files in `~/code/bets` (prod) unless I explicitly ask for a hotfix.

## Database rules

- Prod DB: `~/code/bets/nba/data/database/nba.db` (WAL mode). **Never write to it** — no migrations, fills, reconcile `--apply`, or DDL — unless I ask in this session. Read-only queries are fine.
- Mini dev/tests use `~/nba_copy.db`. MacBook uses `~/data/nba.db` (`NBA_DB_PATH`), restored from the nightly dump.
- Never open, write, or create anything inside the Dropbox folder (it would sync over the backup).
- Before any write I've approved to the prod DB, back it up first: `sqlite3 <db> ".backup <db_dir>/nba_pre_<change>_<date>.db"`.
- Enforce uniformity at the source: consistent column names and types in the tables (game-keyed tables use `game_date`). Don't add casts or defensive hacks in queries to paper over inconsistencies; flag the source instead.
- Pikkit is the final source of truth for bet results and the bankroll.

## Git

- `git pull` at the start of a session. Ask before committing, and always ask before pushing.
- Stage files by name (`git add <file>`), never `git add .` / `git add -A`.
- New files under `nba/data/` are gitignored; model files need `git add -f`.
- Release (mini only, when I ask):
  ```
  cd ~/code/bets-dev && git pull && make test
  cd ~/code/bets && git pull && git merge --no-ff dev && make test
  git push origin main dev
  ```
- After a release: `app.py`-only changes take effect on page refresh; anything else needs a Streamlit restart (`lsof -ti tcp:8501 | xargs kill`; the boot job restarts it).
- If `git log --oneline dev..main` lists anything, `git merge main` in `bets-dev` before new work.

## Code style

- Python unless impossible (SQL lives in `.sql` files). Use f-strings.
- Make the smallest change that does the job: fix the line, don't add functions or classes unless I ask, don't refactor neighbouring code.
- Run `make test` after changes on the mini and show me the result.
- `odds_featured.py` uses paths relative to the repo root — run from the root (notebooks must `os.chdir` there).
- `odds_hist_featured/team_lines_wide.parquet` is derived and not auto-refreshed; rebuild after a featured pull (`python -m nba.odds_featured --season <season> --run --build-wide`).
- Environments: `nba_mini_env` (mini), `nba_env` (MacBook), Python 3.11, editable install (`pip install -e .`; rerun after moving a clone).

## Modeling guardrails

- Don't change the live model, its features, scaler, binning, or outcome cap during the season. Experiments run as shadow models, with predictions saved as parquet in the same format as the predictions table.
- One change at a time, validated on holdout Brier **and** per-line / per-class calibration (tail lines like Over 6.5 matter more than total Brier).
- Watch for leakage: rolling windows must not include the current game; no `ROWS UNBOUNDED PRECEDING` season stats that cause train/serve skew; check numerator/denominator windows match.
- EDA findings are hypotheses, not conclusions.
- Modeling decisions (features, windows, halflives, cutoffs, sizing rules) are mine. Propose them with pros/cons; don't implement them by default.
- Regular season only for calibration and odds backfills (exclude playoffs, play-in, NBA Cup final).

## Working with me

- Be concise. Lead with the change or the answer; flag design tradeoffs afterward.
- Show the diff and say what you ran.
- For analysis, go one step at a time (one function or check, review the output, then the next). Give me code to run rather than running whole analyses on your own.
- Push back when you think I'm wrong, with reasons.
