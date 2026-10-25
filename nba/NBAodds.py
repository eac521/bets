'''
Historical odds backfill from The Odds API (historical endpoints; paid plans only).

Layout (spec: project doc claude/odds-backfill-spec.md)
    nba/data/odds_hist/odds_hist.db                  events manifest, task progress, alias tables (SQLite)
    nba/data/odds_hist/{season}/{pull_sequence}/     raw odds, one parquet file per game_date per run

Raw odds rows are exactly what the API returns (team and player names as the books list them),
one row per game x pull x book x market x player x side x line. Ids are attached at query time
through player_alias / team_alias, never written into the parquet.

Usage
    python -m nba.NBAodds --season 2026-27 --estimate
    python -m nba.NBAodds --season 2026-27 --run --max-credits 25000
    python -m nba.NBAodds --season 2026-27 --alias
    python -m nba.NBAodds --season 2026-27 --report
'''
import os
import re
import json
import time
import glob
import sqlite3
import difflib
import logging
import argparse
import unicodedata
import datetime as dt
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from nba_api.stats.static import teams as static_teams

from .NBAbase import base
from .NBAdata import data
from .constants import _BASE_DIR

logger = logging.getLogger(__name__)

ET = ZoneInfo('America/New_York')
API = 'https://api.the-odds-api.com/v4/historical/sports/basketball_nba'
CONFIG_PATH = os.path.join(os.path.dirname(_BASE_DIR), 'betting', 'config.json')
ODDS_DIR = os.path.join(_BASE_DIR, 'data', 'odds_hist')

# Up to 10 bookmakers count as one region; an 11th doubles the cost of every call.
# Exchanges (us_ex): prices are contract prices before exchange fees (Kalshi taker fee = 0.07 * P * (1 - P) per contract).
# Historical coverage starts when The Odds API added each book, so exchanges will be missing from older seasons.
HIST_BOOKS = ['fanduel', 'draftkings', 'betmgm', 'espnbet', 'pinnacle',
              'williamhill_us', 'fanatics',
              'kalshi', 'novig', 'betrivers']
assert len(HIST_BOOKS) <= 10, 'more than 10 bookmakers is billed as a second region'
HIST_MARKETS = ['player_threes', 'player_threes_alternate',
                'player_points', 'player_points_alternate',
                'player_points_rebounds_assists', 'player_points_rebounds_assists_alternate']
OPEN_PROBE_MARKET = 'player_threes'
PROP_PATTERN = r'Three|3-Point|three-point|Points|Rebounds|Assists|Pts'

CORE_PULLS = ['open', 'morning', 'midday', 'close']
FIXED_PULLS = {'morning': dt.time(10, 0), 'midday': dt.time(14, 0)}
CLOSE_OFFSET = pd.Timedelta(minutes=20)
OPEN_LOOKBACK = pd.Timedelta(hours=36)
STALE_AFTER = pd.Timedelta(minutes=60)
SNAP_GRID = pd.Timedelta(minutes=5)

ODDS_COLUMNS = ['game_date', 'commence_time_et', 'home_team', 'away_team', 'pull_sequence', 'snapshot_ts',
                'book', 'market', 'player_name', 'side', 'line', 'price', 'last_update', 'is_stale']

# Team nickname keys (last word of the full name) - unique across the league and stable across book formats
TEAM_KEY_OVERRIDES = {'Trail Blazers': 'Blazers'}


class BudgetStop(Exception):
    '''Raised when the credit floor or the per-run credit cap is reached.'''


# ---------------------------------------------------------------- helpers (pure, unit tested)

def to_et(ts):
    '''UTC ISO string or Timestamp -> naive America/New_York Timestamp'''
    t = pd.Timestamp(ts)
    if t.tzinfo is None:
        t = t.tz_localize('UTC')
    return t.tz_convert(ET).tz_localize(None)


def et_to_utc_iso(ts_et):
    '''Naive ET Timestamp -> UTC ISO string for the API'''
    t = pd.Timestamp(ts_et)
    if t.tzinfo is None:
        t = t.tz_localize(ET, ambiguous=True, nonexistent='shift_forward')
    return t.tz_convert('UTC').strftime('%Y-%m-%dT%H:%M:%SZ')


def team_key(name):
    '''Nickname key used to match teams across the API, Pikkit and the teams table'''
    for long_name, key in TEAM_KEY_OVERRIDES.items():
        if long_name in name:
            return key
    return name.strip().split(' ')[-1]


def teams_in_text(text, keys):
    '''Team keys mentioned in free text (word boundaries, so Nets does not match Hornets)'''
    return {k for k in keys if re.search(r'\b{}\b'.format(re.escape(k)), text, flags=re.IGNORECASE)}


def snapshot_targets(game_date, commence_et, pulls=CORE_PULLS, hourly=False):
    '''
    Target snapshot times (ET) for one game, excluding the open (found by search).
    Morning/midday are skipped when they fall at or after the close (early tips).
    Output: list of (pull_sequence, target Timestamp)
    '''
    close = pd.Timestamp(commence_et) - CLOSE_OFFSET
    day = pd.Timestamp(game_date)
    out = []
    for p in pulls:
        if p == 'close':
            out.append(('close', close))
        elif p in FIXED_PULLS:
            t = day + pd.Timedelta(hours=FIXED_PULLS[p].hour, minutes=FIXED_PULLS[p].minute)
            if t < close:
                out.append((p, t))
    if hourly:
        fixed_hours = {v.hour for v in FIXED_PULLS.values()}
        t = day + pd.Timedelta(hours=11)
        while t <= close - pd.Timedelta(hours=1):
            if t.hour not in fixed_hours:
                out.append(('hourly', t))
            t += pd.Timedelta(hours=1)
    return out


def parse_snapshot(js, pull_sequence):
    '''
    Historical event-odds response -> long-format odds rows (ODDS_COLUMNS).
    Player props: outcome name is Over/Under, description is the player, point is the line.
    '''
    ev = (js or {}).get('data') or {}
    if not ev or not ev.get('bookmakers'):
        return pd.DataFrame(columns=ODDS_COLUMNS)
    snap = to_et(js['timestamp'])
    commence = to_et(ev['commence_time'])
    rows = []
    for bk in ev.get('bookmakers', []):
        for mk in bk.get('markets', []):
            upd = to_et(mk.get('last_update') or bk.get('last_update'))
            for oc in mk.get('outcomes', []):
                rows.append([commence.date(), commence, ev.get('home_team'), ev.get('away_team'), pull_sequence, snap,
                             bk.get('key'), mk.get('key'), oc.get('description'), oc.get('name'),
                             oc.get('point'), oc.get('price'), upd])
    df = pd.DataFrame(rows, columns=ODDS_COLUMNS[:-1])
    df['line'] = df.line.astype(float)
    df['price'] = pd.to_numeric(df.price).round().astype('Int64')
    df['is_stale'] = (df.snapshot_ts - df.last_update) > STALE_AFTER
    return df


def has_market(js, market):
    '''True when any bookmaker in the response carries outcomes for the market'''
    ev = (js or {}).get('data') or {}
    return any(mk.get('key') == market and mk.get('outcomes')
               for bk in ev.get('bookmakers', []) for mk in bk.get('markets', []))


def normalize_name(name):
    '''Lowercase, strip accents, punctuation and generational suffixes'''
    s = unicodedata.normalize('NFKD', str(name)).encode('ascii', 'ignore').decode()
    s = re.sub(r"[.'`]", '', s.lower())
    s = re.sub(r'\b(jr|sr|ii|iii|iv)\b', '', s)
    return re.sub(r'\s+', ' ', s).strip()


def season_window(season):
    '''2024-25 -> (2024-08-01, 2025-07-31)'''
    y = int(season[:4])
    return '{}-08-01'.format(y), '{}-07-31'.format(y + 1)


# ---------------------------------------------------------------- backfill

class oddsHistory(base):

    def __init__(self, api_key=None, odds_dir=ODDS_DIR, min_remaining=25000, sleep=0.25):
        '''
        api_key: The Odds API key with historical access. Falls back to env ODDS_HIST_KEY,
                 then oddsApi.historical in betting/config.json
        min_remaining: stop the run when x-requests-remaining drops below this
        '''
        super().__init__()
        self.api_key = api_key or os.environ.get('ODDS_HIST_KEY') or self._config_key()
        self.odds_dir = odds_dir
        self.min_remaining = min_remaining
        self.sleep = sleep
        self.remaining = None
        self.credits_run = 0
        self.max_credits = None
        os.makedirs(odds_dir, exist_ok=True)
        self.hconn = sqlite3.connect(os.path.join(odds_dir, 'odds_hist.db'), check_same_thread=False)
        self._create_tables()
        # nba_api's static list, not the teams table: teams.teamName has typos and MIL mislabeled as OKC
        self.team_keys = {team_key(t['full_name']): str(t['id']) for t in static_teams.get_teams()}

    @staticmethod
    def _config_key():
        if not os.path.exists(CONFIG_PATH):
            return None
        dct = json.loads(open(CONFIG_PATH, 'r').read())
        return (dct.get('oddsApi') or {}).get('historical')

    def _create_tables(self):
        self.hconn.executescript('''
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY, game_id TEXT, season TEXT, game_date TEXT,
                commence_time_et TEXT, home_team TEXT, away_team TEXT);
            CREATE TABLE IF NOT EXISTS event_dates (
                season TEXT, game_date TEXT, n_events INT, n_matched INT, PRIMARY KEY (season, game_date));
            CREATE TABLE IF NOT EXISTS tasks (
                event_id TEXT, pull_sequence TEXT, target_ts TEXT, status TEXT DEFAULT 'pending',
                snapshot_ts TEXT, credits INT, n_rows INT, file TEXT, note TEXT, updated_at TEXT,
                PRIMARY KEY (event_id, pull_sequence, target_ts));
            CREATE TABLE IF NOT EXISTS player_alias (
                player_name TEXT PRIMARY KEY, player_id TEXT, source TEXT);
            CREATE TABLE IF NOT EXISTS team_alias (
                team_name TEXT PRIMARY KEY, team_id TEXT);
        ''')
        self.hconn.commit()

    # ------------------------------------------------------------ API

    def _get(self, url, params, allow_empty=(404, 422)):
        '''
        GET with retries. Tracks credits from x-requests-last and enforces the budget floor.
        Returns parsed JSON, or None for statuses in allow_empty (event or snapshot not available).
        '''
        if self.max_credits is not None and self.credits_run >= self.max_credits:
            raise BudgetStop('max credits for this run reached ({:,})'.format(self.credits_run))
        if self.remaining is not None and self.remaining < self.min_remaining:
            raise BudgetStop('credit floor reached: {:,} remaining'.format(self.remaining))
        params = dict(params, apiKey=self.api_key)
        for attempt in range(5):
            r = requests.get(url, params=params, timeout=60)
            if r.headers.get('x-requests-remaining') is not None:
                self.remaining = int(float(r.headers['x-requests-remaining']))
            self.credits_run += int(float(r.headers.get('x-requests-last', 0) or 0))
            if r.status_code == 200:
                time.sleep(self.sleep)
                return r.json()
            if r.status_code in allow_empty:
                return None
            if r.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning('429 from the API, sleeping {}s'.format(wait))
                time.sleep(wait)
                continue
            if r.status_code in (401, 403):
                raise BudgetStop('API refused the key ({}): {}'.format(r.status_code, r.text[:200]))
            if r.status_code >= 500:
                time.sleep(2 ** attempt * 5)
                continue
            r.raise_for_status()
        raise RuntimeError('API request failed after retries: {} {}'.format(url, params.get('date')))

    def fetch_snapshot(self, event_id, when_et, pull_sequence, markets=HIST_MARKETS):
        '''One event at one snapshot time -> (rows, raw json)'''
        js = self._get('{}/events/{}/odds'.format(API, event_id), {
            'bookmakers': ','.join(HIST_BOOKS), 'markets': ','.join(markets),
            'oddsFormat': 'american', 'dateFormat': 'iso', 'date': et_to_utc_iso(when_et)})
        return parse_snapshot(js, pull_sequence), js

    def find_open(self, event_id, commence_et):
        '''
        First snapshot carrying props, by binary search between commence - 36h and the close,
        probing player_threes only. Empty snapshots cost nothing, so only hits are paid.
        Output: snapshot Timestamp (ET) or None when the game never had props
        '''
        def probe(t):
            _, js = self.fetch_snapshot(event_id, t, 'open', markets=[OPEN_PROBE_MARKET])
            return to_et(js['timestamp']) if js and has_market(js, OPEN_PROBE_MARKET) else None

        close = pd.Timestamp(commence_et) - CLOSE_OFFSET
        hi = probe(close)
        if hi is None:
            return None
        lo = close - OPEN_LOOKBACK
        lo_hit = probe(lo)
        if lo_hit is not None:
            return lo_hit
        while hi - lo > SNAP_GRID:
            mid = lo + (hi - lo) / 2
            s = probe(mid)
            if s is None:
                lo = mid
            else:
                hi = s if s > lo else mid
        return hi

    # ------------------------------------------------------------ schedule and manifest

    def season_dates(self, season, end=None):
        '''Regular-season dates already played, from plyrLogs (game_id prefix 002)'''
        start, stop = season_window(season)
        end = min(stop, end or (dt.datetime.now(ET) - dt.timedelta(days=1)).strftime('%Y-%m-%d'))
        q = '''select distinct game_date from plyrLogs
               where game_id like '002%' and min is not null and game_date between ? and ? order by game_date'''
        return pd.read_sql(q, self.conn, params=(start, end)).game_date.str[:10].tolist()

    def game_map(self, season):
        '''(game_date, frozenset(team_ids)) -> game_id for regular-season games'''
        start, stop = season_window(season)
        g = pd.read_sql('''select distinct game_id, substr(game_date,1,10) as game_date, team_id from plyrLogs
                           where game_id like '002%' and game_date between ? and ?''', self.conn, params=(start, stop))
        return {(d, frozenset(grp.team_id.astype(str))): gid for (gid, d), grp in g.groupby(['game_id', 'game_date'])}

    def build_events(self, season, end=None):
        '''
        One historical events call per game date (1 credit each) -> events manifest.
        Only events matching a regular-season game_id are kept (drops playoffs, play-in, Cup final).
        '''
        gmap = self.game_map(season)
        done = set(pd.read_sql('select game_date from event_dates where season = ?', self.hconn, params=(season,)).game_date)
        dates = [d for d in self.season_dates(season, end) if d not in done]
        logger.info('{}: building events for {} dates'.format(season, len(dates)))
        for d in dates:
            day = pd.Timestamp(d)
            js = self._get('{}/events'.format(API), {
                'dateFormat': 'iso', 'date': et_to_utc_iso(day + pd.Timedelta(hours=9)),
                'commenceTimeFrom': et_to_utc_iso(day), 'commenceTimeTo': et_to_utc_iso(day + pd.Timedelta(hours=23, minutes=59))})
            evs = (js or {}).get('data') or []
            rows = []
            for e in evs:
                commence = to_et(e['commence_time'])
                gd = commence.strftime('%Y-%m-%d')
                ids = frozenset(self.team_keys.get(team_key(t)) for t in (e['home_team'], e['away_team']))
                gid = gmap.get((gd, ids))
                if gid is None:
                    logger.warning('{}: no regular-season game for {} @ {}'.format(gd, e['away_team'], e['home_team']))
                    continue
                rows.append((e['id'], gid, season, gd, str(commence), e['home_team'], e['away_team']))
            self.hconn.executemany('insert or replace into events values (?,?,?,?,?,?,?)', rows)
            self.hconn.execute('insert or replace into event_dates values (?,?,?,?)', (season, d, len(evs), len(rows)))
            self.hconn.commit()
        self.build_team_alias()

    def build_team_alias(self):
        names = pd.read_sql('select distinct home_team as n from events union select distinct away_team from events', self.hconn).n
        rows = [(n, self.team_keys.get(team_key(n))) for n in names]
        self.hconn.executemany('insert or replace into team_alias values (?,?)', rows)
        self.hconn.commit()

    def events(self, season):
        return pd.read_sql('select * from events where season = ? order by commence_time_et', self.hconn, params=(season,))

    def plan_tasks(self, season, pulls=CORE_PULLS, hourly=False):
        '''Insert pending tasks for every event (existing tasks are left alone, so this is safe to re-run)'''
        rows = []
        for e in self.events(season).itertuples():
            if 'open' in pulls:
                rows.append((e.event_id, 'open', 'search'))
            for p, t in snapshot_targets(e.game_date, e.commence_time_et, pulls, hourly):
                rows.append((e.event_id, p, str(t)))
        self.hconn.executemany("insert or ignore into tasks (event_id, pull_sequence, target_ts) values (?,?,?)", rows)
        self.hconn.commit()
        return len(rows)

    def plan_bet_tasks(self, season, pikkit_path=None):
        '''
        One snapshot per (event, bet time floored to 5 min) for every Pikkit NBA bet placed before the close.
        Bets are matched to events by the two team nicknames in bet_info and a commence time within 36h.
        '''
        pikkit_path = pikkit_path or os.path.join(_BASE_DIR, 'data', 'pikkit', 'transactions.csv')
        if not os.path.exists(pikkit_path):
            logger.warning('No Pikkit export at {}'.format(pikkit_path))
            return 0
        pk = pd.read_csv(pikkit_path, usecols=['bet_info', 'leagues', 'time_placed_iso'])
        pk = pk[pk.leagues.fillna('').str.contains('NBA')].dropna(subset=['bet_info', 'time_placed_iso'])
        pk = pk[pk.bet_info.str.contains(PROP_PATTERN, case=False)]
        pk['placed_et'] = pk.time_placed_iso.apply(to_et).dt.floor('5min')
        ev = self.events(season)
        if ev.empty:
            return 0
        ev['commence'] = pd.to_datetime(ev.commence_time_et)
        ev['keys'] = [frozenset((team_key(h), team_key(a))) for h, a in zip(ev.home_team, ev.away_team)]
        keys = set(self.team_keys)
        rows = set()
        for b in pk.itertuples():
            mentioned = teams_in_text(b.bet_info, keys)
            if len(mentioned) < 2:
                continue
            cand = ev[(ev.commence - CLOSE_OFFSET > b.placed_et) & (ev.commence - b.placed_et < OPEN_LOOKBACK)]
            for e in cand.itertuples():
                if e.keys <= mentioned:
                    rows.add((e.event_id, 'bet', str(b.placed_et)))
        self.hconn.executemany("insert or ignore into tasks (event_id, pull_sequence, target_ts) values (?,?,?)", sorted(rows))
        self.hconn.commit()
        return len(rows)

    # ------------------------------------------------------------ run

    def pending(self, season, pulls=None):
        q = '''select t.*, e.game_date, e.commence_time_et from tasks t join events e using(event_id)
               where e.season = ? and t.status = 'pending' '''
        df = pd.read_sql(q, self.hconn, params=(season,))
        if pulls:
            df = df[df.pull_sequence.isin(pulls)]
        return df.sort_values(['game_date', 'pull_sequence', 'commence_time_et'])

    def run(self, season, pulls=CORE_PULLS, hourly=False, bets=True, max_credits=None, end=None):
        '''
        Full backfill for a season: manifest -> tasks -> pulls. Resumable: only pending tasks run,
        and each (game_date, pull_sequence) batch is written to parquet before its tasks are marked done.
        '''
        self.max_credits = max_credits
        self.credits_run = 0
        try:
            self.build_events(season, end)
            self.plan_tasks(season, pulls, hourly)
            if bets:
                logger.info('{} bet-time tasks planned'.format(self.plan_bet_tasks(season)))
            todo = self.pending(season, list(pulls) + (['hourly'] if hourly else []) + (['bet'] if bets else []))
        except BudgetStop as e:
            logger.warning('Stopped while planning: {}'.format(e))
            return self.credits_run
        logger.info('{}: {} pending tasks'.format(season, len(todo)))
        for (game_date, pull), grp in todo.groupby(['game_date', 'pull_sequence'], sort=True):
            stop = self._run_batch(season, game_date, pull, grp)
            logger.info('{} {} done; credits this run {:,}; remaining {}'.format(game_date, pull, self.credits_run, self.remaining))
            if stop:
                logger.warning('Run stopped: {}'.format(stop))
                break
        return self.credits_run

    def _run_batch(self, season, game_date, pull, grp):
        frames, results, stop = [], [], None
        for t in grp.itertuples():
            before = self.credits_run
            try:
                if pull == 'open':
                    snap = self.find_open(t.event_id, t.commence_time_et)
                    df = self.fetch_snapshot(t.event_id, snap, 'open')[0] if snap is not None else pd.DataFrame(columns=ODDS_COLUMNS)
                else:
                    df = self.fetch_snapshot(t.event_id, pd.Timestamp(t.target_ts), pull)[0]
                status, note = ('done' if len(df) else 'empty'), None
            except BudgetStop as e:
                stop = str(e)
                break
            except Exception as e:
                df, status, note = pd.DataFrame(columns=ODDS_COLUMNS), 'error', str(e)[:300]
                logger.error('{} {} {}: {}'.format(game_date, pull, t.event_id, note))
            if len(df):
                frames.append(df)
            snap_ts = str(df.snapshot_ts.iloc[0]) if len(df) else None
            results.append((t.event_id, t.pull_sequence, t.target_ts, status, snap_ts, self.credits_run - before, len(df), note))
        path = self._write(season, game_date, pull, frames)
        now = dt.datetime.now().isoformat(timespec='seconds')
        self.hconn.executemany('''update tasks set status=?, snapshot_ts=?, credits=?, n_rows=?, file=?, note=?, updated_at=?
                                  where event_id=? and pull_sequence=? and target_ts=?''',
                               [(s, ts, c, n, path, note, now, e, p, tt) for e, p, tt, s, ts, c, n, note in results])
        self.hconn.commit()
        return stop

    def _write(self, season, game_date, pull, frames):
        if not frames:
            return None
        out = os.path.join(self.odds_dir, season, pull)
        os.makedirs(out, exist_ok=True)
        path = os.path.join(out, '{}_{}.parquet'.format(game_date, dt.datetime.now().strftime('%Y%m%d%H%M%S')))
        df = pd.concat(frames, ignore_index=True)
        df.to_parquet(path, index=False, compression='snappy')
        return path

    # ------------------------------------------------------------ estimate and checks

    def estimate(self, season, pulls=CORE_PULLS, hourly=False, n_markets=len(HIST_MARKETS), open_probes=5, end=None):
        '''
        Upper-bound credit estimate from the schedule, without calling the API.
        Excludes bet-time snapshots: ~2,900 per season from Pikkit, about 60 credits each (~175k per season).
        '''
        dates = self.season_dates(season, end)
        games = len({gid for (d, _), gid in self.game_map(season).items() if d in set(dates)})
        fixed = len([p for p in pulls if p != 'open'])
        per_game = 10 * n_markets * fixed
        if 'open' in pulls:
            per_game += 10 * n_markets + 10 * open_probes
        if hourly:
            per_game += 10 * n_markets * 6
        est = {'season': season, 'game_dates': len(dates), 'games': games, 'events_calls': len(dates),
               'credits_per_game': per_game, 'credits_total': len(dates) + games * per_game}
        logger.info(est)
        return est

    def _odds_glob(self, season):
        return os.path.join(self.odds_dir, season, '*', '*.parquet')

    def _query(self, sql):
        import duckdb
        return duckdb.sql(sql).df()

    def build_player_alias(self, season, min_coverage=0.98, fuzzy_cutoff=0.85):
        '''
        Map book player names -> player_id using each game's roster (plyrLogs), in order:
        existing alias rows, exact, standardize_names (NAME_MAP), normalized, fuzzy within the roster.
        Writes player_alias and returns (coverage, unmatched DataFrame, fuzzy matches to review).
        Raises when coverage is below min_coverage.
        '''
        names = self._query('''select distinct cast(game_date as varchar) as game_date, home_team, away_team, player_name
                               from read_parquet('{}') where player_name is not null'''.format(self._odds_glob(season)))
        ev = self.events(season)[['game_id', 'game_date', 'home_team', 'away_team']]
        names = names.merge(ev, on=['game_date', 'home_team', 'away_team'], how='left')
        if names.game_id.dropna().empty:
            raise ValueError('No odds rows matched to events for {}; run the backfill first'.format(season))
        roster = pd.read_sql('''select distinct l.game_id, l.player_id, p.name from plyrLogs l
                                join players p using(player_id) where l.game_id in ({})'''.format(
            ','.join("'{}'".format(g) for g in names.game_id.dropna().unique())), self.conn)
        known = dict(self.hconn.execute('select player_name, player_id from player_alias').fetchall())
        std = data.standardize_names
        roster['std'] = std(roster.name)
        roster['norm'] = roster.name.apply(normalize_name)

        rows, unmatched, fuzzy = {}, [], []
        for n in names.itertuples():
            if n.player_name in known or n.player_name in rows:
                continue
            r = roster[roster.game_id == n.game_id]
            hit, source = None, None
            if (r.name == n.player_name).any():
                hit, source = r.loc[r.name == n.player_name, 'player_id'].iloc[0], 'exact'
            elif (r['std'] == std(pd.Series([n.player_name])).iloc[0]).any():
                hit, source = r.loc[r['std'] == std(pd.Series([n.player_name])).iloc[0], 'player_id'].iloc[0], 'name_map'
            elif (r.norm == normalize_name(n.player_name)).any():
                hit, source = r.loc[r.norm == normalize_name(n.player_name), 'player_id'].iloc[0], 'normalized'
            else:
                close = difflib.get_close_matches(normalize_name(n.player_name), r.norm.tolist(), n=1, cutoff=fuzzy_cutoff)
                if close:
                    hit, source = r.loc[r.norm == close[0], 'player_id'].iloc[0], 'fuzzy'
                    fuzzy.append((n.player_name, r.loc[r.norm == close[0], 'name'].iloc[0], n.game_date))
            if hit is None:
                unmatched.append((n.player_name, n.game_date, n.away_team, n.home_team))
            else:
                rows[n.player_name] = (str(hit), source)
        self.hconn.executemany('insert or replace into player_alias values (?,?,?)',
                               [(k, v[0], v[1]) for k, v in rows.items()])
        self.hconn.commit()

        all_names = set(names.player_name)
        alias = set(known) | set(rows)
        coverage = len(all_names & alias) / max(len(all_names), 1)
        unmatched = pd.DataFrame(unmatched, columns=['player_name', 'game_date', 'away_team', 'home_team'])
        unmatched = unmatched[~unmatched.player_name.isin(alias)].drop_duplicates('player_name')
        fuzzy = pd.DataFrame(fuzzy, columns=['book_name', 'matched_to', 'game_date'])
        logger.info('{}: player alias coverage {:.2%} ({} unmatched, {} fuzzy to review)'.format(
            season, coverage, len(unmatched), len(fuzzy)))
        if coverage < min_coverage:
            raise ValueError('Player alias coverage {:.2%} below {:.0%}; add aliases for:\n{}'.format(
                coverage, min_coverage, unmatched.to_string(index=False)))
        return coverage, unmatched, fuzzy

    def pilot_report(self, season):
        '''Credits vs estimate, task status, book coverage (incl. Pinnacle), stale share at the close'''
        tasks = pd.read_sql('''select t.pull_sequence, t.status, t.credits, t.n_rows from tasks t
                               join events e using(event_id) where e.season = ?''', self.hconn, params=(season,))
        by_pull = tasks.groupby('pull_sequence').agg(
            tasks=('status', 'size'), done=('status', lambda s: (s == 'done').sum()),
            empty=('status', lambda s: (s == 'empty').sum()), error=('status', lambda s: (s == 'error').sum()),
            credits=('credits', 'sum'), avg_credits=('credits', 'mean'), rows=('n_rows', 'sum'))
        g = self._odds_glob(season)
        books = self._query('''select book, market, count(*) as n_rows, count(distinct player_name) as players,
                               avg(case when pull_sequence = 'close' then is_stale::int end) as stale_share_close
                               from read_parquet('{}') group by 1, 2 order by 1, 2'''.format(g))
        markets = self._query('''select market, count(distinct book) as books from read_parquet('{}') group by 1'''.format(g))
        return {'by_pull': by_pull, 'books': books, 'markets': markets, 'estimate': self.estimate(season)}

    def compare_live_lines(self, season):
        '''
        Agreement between historical snapshots and the live lines table (threes, same player/date/side/line/book),
        by pull_sequence. Live lines keep the last pull of the day, so the best-agreeing pull shows when that was.
        '''
        hist = self._query('''select cast(game_date as varchar) as date, player_name, side as over_under, line as number,
                              book, price, pull_sequence from read_parquet('{}')
                              where market in ('player_threes', 'player_threes_alternate')
                              and book in ('fanduel', 'draftkings', 'espnbet')'''.format(self._odds_glob(season)))
        alias = pd.read_sql('select player_name, player_id from player_alias', self.hconn)
        hist = hist.merge(alias, on='player_name')
        live = pd.read_sql('''select player_id, date, over_under, number, FanDuel as fanduel, DraftKings as draftkings,
                              theScore_Bet as espnbet from lines where date between ? and ?''',
                           self.conn, params=season_window(season))
        live = live.melt(id_vars=['player_id', 'date', 'over_under', 'number'], var_name='book', value_name='live_price').dropna()
        m = hist.merge(live, on=['player_id', 'date', 'over_under', 'number', 'book'])
        m['match'] = m.price == m.live_price
        return m.groupby(['pull_sequence', 'book']).agg(n=('match', 'size'), share_equal=('match', 'mean')).reset_index()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='Historical odds backfill (The Odds API)')
    ap.add_argument('--season', required=True, help='e.g. 2026-27')
    ap.add_argument('--estimate', action='store_true', help='credit estimate from the schedule, no API calls')
    ap.add_argument('--run', action='store_true', help='run the backfill (resumable)')
    ap.add_argument('--pulls', default=','.join(CORE_PULLS), help='comma list from open,morning,midday,close')
    ap.add_argument('--hourly', action='store_true', help='add hourly snapshots between morning and close')
    ap.add_argument('--no-bets', action='store_true', help='skip Pikkit bet-time snapshots')
    ap.add_argument('--max-credits', type=int, default=None, help='stop this run after this many credits')
    ap.add_argument('--min-remaining', type=int, default=25000, help='stop when the account drops below this')
    ap.add_argument('--end', default=None, help='last game_date to include (YYYY-MM-DD)')
    ap.add_argument('--alias', action='store_true', help='build player_alias and print the match report')
    ap.add_argument('--report', action='store_true', help='pilot report')
    args = ap.parse_args()

    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('logs/odds_backfill.log'), logging.StreamHandler()])
    oh = oddsHistory(min_remaining=args.min_remaining)
    pulls = [p for p in args.pulls.split(',') if p]
    if args.estimate:
        print(oh.estimate(args.season, pulls, args.hourly, end=args.end))
    if args.run:
        if not oh.api_key:
            raise SystemExit('No API key: set ODDS_HIST_KEY or oddsApi.historical in betting/config.json')
        used = oh.run(args.season, pulls, args.hourly, not args.no_bets, args.max_credits, args.end)
        print('Credits used this run: {:,}; remaining: {}'.format(used, oh.remaining))
    if args.alias:
        cov, unmatched, fuzzy = oh.build_player_alias(args.season)
        print('Coverage {:.2%}'.format(cov))
        print(unmatched.to_string(index=False))
        print(fuzzy.to_string(index=False))
    if args.report:
        for k, v in oh.pilot_report(args.season).items():
            print('\n== {}'.format(k))
            print(v.to_string() if hasattr(v, 'to_string') else v)
