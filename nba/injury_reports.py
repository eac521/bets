'''
Official NBA injury reports through nbainjuries: one parsed parquet per report snapshot, plus a manifest of which
report each target time used.

Targets per game date (ET): 05:30, 09:30, 14:00, 17:00, and tip - 90 min for every game.
Each target uses the latest report published at or before it. The NBA's report cadence changed:
- before 2025-12-22 09:00: hourly files, named by the hour and stamped hh:30 (Injury-Report_2025-11-30_01PM.pdf = 1:30 PM)
- from 2025-12-22 09:00: every 15 minutes (Injury-Report_2025-12-22_09_15AM.pdf)
If the latest slot is missing (gaps, overnight), the search steps back up to LOOKBACK.

Rows whose reason is "NOT YET SUBMITTED" are team placeholders (no player): they're kept, as they show when each
team files.

Usage (from the repo root; nbainjuries needs Java on PATH):
python -m nba.injury_reports --season 2025-26 --plan                       # targets and distinct reports, no calls
python -m nba.injury_reports --season 2025-26 --run                        # resumable: reports already on disk are skipped
python -m nba.injury_reports --season 2025-26 --run --start 2025-11-01 --end 2025-11-30
python -m nba.injury_reports --season 2025-26 --check                      # validation summary of the parsed files

Output: nba/data/injury/{season}/reports/{YYYY-MM-DD_HHMM}.parquet (one per report, write-once)
        nba/data/injury/{season}/manifest.parquet (one row per target: game_date, target, game_id, tip_et, target_ts,
        report_ts, result)
'''
import argparse
import logging
import os
import random
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from .constants import _BASE_DIR

LOG = logging.getLogger('injury_reports')
OUT_ROOT = os.path.join(_BASE_DIR, 'data', 'injury')
FIXED_TARGETS = [('0530', 5, 30), ('0930', 9, 30), ('1400', 14, 0), ('1700', 17, 0)]
TIP_OFFSET = timedelta(minutes=90)
NEW_FORMAT_START = datetime(2025, 12, 22, 9, 0)              # 15-minute reports from here on
LEGACY_GAP = (datetime(2025, 12, 19, 15, 30), datetime(2025, 12, 19, 16, 45))   # nbainjuries can't build URLs inside it
LOOKBACK = timedelta(hours=8)
STATUSES = {'Out', 'Doubtful', 'Questionable', 'Probable', 'Available'}
COLUMNS = {'Game Date': 'game_date', 'Game Time': 'game_time', 'Matchup': 'matchup', 'Team': 'team',
           'Player Name': 'player_name', 'Current Status': 'status', 'Reason': 'reason'}


def report_candidates(target):
    '''Report timestamps at or before target, newest first, back to LOOKBACK.
    Legacy (hourly) reports are stamped hh:30; new ones fall on :00/:15/:30/:45.'''
    out, floor = [], target - LOOKBACK
    if target >= NEW_FORMAT_START:
        t = target.replace(minute=target.minute - target.minute % 15, second=0, microsecond=0)
        while t >= max(floor, NEW_FORMAT_START):
            out.append(t)
            t -= timedelta(minutes=15)
        if t >= floor:                                    # lookback crosses into the hourly era
            target = min(t, NEW_FORMAT_START - timedelta(minutes=1))
        else:
            return out
    t = target.replace(minute=30, second=0, microsecond=0)
    if t > target:
        t -= timedelta(hours=1)
    while t >= floor:
        if not (LEGACY_GAP[0] < t < LEGACY_GAP[1]):
            out.append(t)
        t -= timedelta(hours=1)
    return out


def plan_targets(schedule):
    '''One row per target. schedule: one row per game with game_date (YYYY-MM-DD), game_id, tip_et (datetime, ET).'''
    rows = []
    for game_date, games in schedule.groupby('game_date'):
        day = datetime.strptime(game_date, '%Y-%m-%d')
        for label, hh, mm in FIXED_TARGETS:
            rows.append({'game_date': game_date, 'target': label, 'game_id': None, 'tip_et': pd.NaT,
                         'target_ts': day.replace(hour=hh, minute=mm)})
        for g in games.itertuples():
            rows.append({'game_date': game_date, 'target': 'tip-90', 'game_id': g.game_id, 'tip_et': g.tip_et,
                         'target_ts': pd.Timestamp(g.tip_et).to_pydatetime() - TIP_OFFSET})
    return pd.DataFrame(rows)


def clean_report(df, report_ts):
    '''nbainjuries output -> snake-case columns, ISO game date, report timestamp.'''
    out = df.rename(columns=COLUMNS)[list(COLUMNS.values())].copy()
    out['game_date'] = pd.to_datetime(out.game_date, format='%m/%d/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
    out['unsubmitted'] = out.reason.astype(str).str.strip().str.casefold() == 'not yet submitted'
    out.insert(0, 'report_ts', pd.Timestamp(report_ts))
    return out


def validate(df):
    '''Flags for a parsed report; a nonzero count means the parse is suspect and the report should be re-pulled.'''
    players = df[~df.unsubmitted]
    return {'n_rows': len(df), 'n_players': len(players), 'n_unsubmitted': int(df.unsubmitted.sum()),
            'bad_status': int((~players.status.isin(STATUSES)).sum()),
            'null_player': int(players.player_name.isna().sum()),
            'null_game_date': int(df.game_date.isna().sum())}


def report_path(season, report_ts):
    return os.path.join(OUT_ROOT, season, 'reports', f'{report_ts:%Y-%m-%d_%H%M}.parquet')


def is_missing(err):
    '''True when the report file doesn't exist (403/404), as opposed to a connection problem.'''
    reason = getattr(err, 'reason', None)
    resp = getattr(reason, 'response', None)
    return resp is not None and resp.status_code in (403, 404)


def fetch(report_ts, tries=3):
    '''Parsed report, or None if it doesn't exist. Raises on repeated connection errors or a failed parse.'''
    from nbainjuries import injury
    from nbainjuries._exceptions import URLRetrievalError
    for attempt in range(tries):
        try:
            return injury.get_reportdata(report_ts, return_df=True)
        except URLRetrievalError as e:
            if is_missing(e):
                return None
            LOG.warning(f'{report_ts} connection error ({e}); retry {attempt + 1}')
            time.sleep(5 * 2 ** attempt)
    raise RuntimeError(f'{report_ts}: connection failed {tries} times')


def resolve(season, target_ts, known, pause=(1.0, 2.0)):
    '''Latest available report at or before target_ts. known caches report_ts -> 'ok' / 'missing' / 'error'.'''
    for ts in report_candidates(target_ts):
        state = known.get(ts)
        if state is None:
            if os.path.exists(report_path(season, ts)):
                state = 'ok'
            else:
                try:
                    df = fetch(ts)
                except Exception as e:                    # a report that exists but won't parse: stop, don't fall back
                    LOG.error(f'{ts} failed: {e}')
                    known[ts] = 'error'
                    return ts, 'error'
                time.sleep(random.uniform(*pause))
                if df is None:
                    state = 'missing'
                else:
                    out = clean_report(df, ts)
                    os.makedirs(os.path.dirname(report_path(season, ts)), exist_ok=True)
                    out.to_parquet(report_path(season, ts), index=False)
                    flags = validate(out)
                    if flags['bad_status'] or flags['null_player'] or flags['null_game_date']:
                        LOG.warning(f'{ts} suspect parse: {flags}')
                    state = 'ok'
            known[ts] = state
        if state in ('ok', 'error'):
            return ts, state
    return None, 'none_in_lookback'


def load_schedule(season):
    from .NBAetl import etl
    s = etl.get_schedule(None, season)                    # no DB needed: skip etl() (it opens nba.db on init)
    if 'tip_et' not in s.columns or s.tip_et.isna().all():
        raise RuntimeError('get_schedule returned no tip_et: check gameDateTimeEst in ScheduleLeagueV2')
    return s.drop_duplicates('game_id')[['game_date', 'game_id', 'tip_et']]


def run(season, start=None, end=None):
    sched = load_schedule(season)
    today = datetime.now().strftime('%Y-%m-%d')
    sched = sched[(sched.game_date < today) & (sched.game_date >= (start or '0')) & (sched.game_date <= (end or '9'))]
    plan = plan_targets(sched)
    known, results, streak = {}, [], 0
    for i, r in enumerate(plan.itertuples(index=False)):
        report_ts, result = resolve(season, r.target_ts, known)
        results.append({**r._asdict(), 'report_ts': report_ts, 'result': result})
        streak = streak + 1 if result == 'error' else 0
        if streak >= 3:                                   # same failure every time: setup problem (Java, network), not data
            LOG.error('3 errors in a row: stopping. Check the first error above, fix it, and rerun.')
            break
        if i % 50 == 0:
            LOG.info(f'{i + 1}/{len(plan)} targets | {sum(v == "ok" for v in known.values())} reports')
    man = pd.DataFrame(results)
    path = os.path.join(OUT_ROOT, season, 'manifest.parquet')
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):                              # keep earlier runs' rows for targets not re-run
        old = pd.read_parquet(path)
        key = ['game_date', 'target', 'game_id']
        man = pd.concat([old, man]).drop_duplicates(subset=key, keep='last')
    man.sort_values(['game_date', 'target_ts']).to_parquet(path, index=False)
    LOG.info(f"done: {man.result.value_counts().to_dict()}")
    return man


def check(season):
    folder = os.path.join(OUT_ROOT, season, 'reports')
    if not os.path.isdir(folder):
        print(f'no reports saved yet for {season}')
        return pd.DataFrame()
    rows = []
    for f in sorted(os.listdir(folder)):
        df = pd.read_parquet(os.path.join(folder, f))
        rows.append({'file': f, **validate(df)})
    out = pd.DataFrame(rows)
    bad = out[(out.bad_status > 0) | (out.null_player > 0) | (out.null_game_date > 0)]
    print(f'{len(out):,} reports | {len(bad):,} suspect')
    print(out[['n_rows', 'n_players', 'n_unsubmitted']].describe().round(1).to_string())
    if len(bad):
        print(bad.to_string(index=False))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--season', required=True)
    ap.add_argument('--plan', action='store_true')
    ap.add_argument('--run', action='store_true')
    ap.add_argument('--check', action='store_true')
    ap.add_argument('--start')
    ap.add_argument('--end')
    a = ap.parse_args()
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.FileHandler('logs/injury_reports.log'), logging.StreamHandler()])
    if a.plan:
        plan = plan_targets(load_schedule(a.season))
        distinct = {report_candidates(t)[0] for t in plan.target_ts if report_candidates(t)}
        print(f'{len(plan):,} targets over {plan.game_date.nunique()} dates | ~{len(distinct):,} distinct reports to pull')
    if a.run:
        run(a.season, a.start, a.end)
    if a.check:
        check(a.season)


if __name__ == '__main__':
    main()
