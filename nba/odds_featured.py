'''
Historical game lines (spreads, totals) from The Odds API, for the zero-threes diagnostics.

Uses the bulk historical endpoint (/v4/historical/sports/basketball_nba/odds): one call returns every NBA game
on the slate at that timestamp. Cost = 10 credits x markets x regions, so spreads + totals across the same 10
books as the props backfill = 20 credits per call, regardless of how many games are on the slate.

Snapshots per game date (ET, same times as the props backfill):
    morning  10:00
    midday   14:00
    close    each distinct tip time - 20 min (optional; one call per tip time)
Use morning (fall back to midday for late-posted games) as the pre-game feature. The close is for diagnostics
only: it can postdate the bet.

The game list comes from the props backfill (nba/data/odds_hist/odds_hist.db: events, team_alias), so no events
calls are needed and only regular-season games are kept. Writes only to nba/data/odds_hist_featured/.

Usage (repo root; key from env ODDS_HIST_KEY, or "historical" under "oddsApi" in betting/config.json):
    python -m nba.odds_featured --season 2023-24 2024-25 2025-26 2026-27 --estimate
    python -m nba.odds_featured --season 2025-26 --run --max-credits 2000 --end 2025-11-05      # pilot
    python -m nba.odds_featured --season 2023-24 2024-25 2025-26 2026-27 --run
    python -m nba.odds_featured --season 2023-24 2024-25 2025-26 2026-27 --run --pulls close    # optional
    python -m nba.odds_featured --season 2025-26 --report
    python -m nba.odds_featured --season 2025-26 --build-wide     # rewrite the wide file (all seasons pulled)

Storage:
    raw (source of record, write-once): nba/data/odds_hist_featured/{season}/{pull}/{game_date}_{run ts}.parquet,
        one row per game x pull x book x market x outcome
    wide (derived, rebuilt in full by --build-wide): nba/data/odds_hist_featured/team_lines_wide.parquet,
        one row per game_id x team_id x pull_sequence, a column per book: {book}_spread, {book}_spread_price,
        {book}_total, {book}_over_price, {book}_under_price. No consensus columns: pick the book at analysis time.
        Joins to teamLog / plyrLogs on (team_id, game_id). Pick the snapshot with pull_sequence.

In code:
    from nba.odds_featured import team_lines, book_lines
    tl = team_lines(['2025-26'])                      # pre-game row per game_id x team_id, first of FD / DK / MGM
    w = pd.read_parquet('nba/data/odds_hist_featured/team_lines_wide.parquet')
    bl = book_lines(bets_joined_to_w, bets_joined_to_w.book)   # the line from the bet's own book
'''
import argparse
import json
import logging
import math
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

try:
    from betting.constants import books as _BOOKS
    BOOKS = list(_BOOKS)
except ImportError:
    BOOKS = ['draftkings', 'fanduel', 'betmgm', 'fanatics', 'espnbet', 'williamhill_us', 'pinnacle', 'kalshi',
             'betrivers', 'novig']

URL = 'https://api.the-odds-api.com/v4/historical/sports/basketball_nba/odds'
HIST_DB = 'nba/data/odds_hist/odds_hist.db'
OUT_DIR = 'nba/data/odds_hist_featured'
MARKETS = ['spreads', 'totals']
PULL_TIMES = {'morning': (10, 0), 'midday': (14, 0)}
CLOSE_MIN_BEFORE_TIP = 20
STALE_MIN = 60
ET = ZoneInfo('America/New_York')
UTC = timezone.utc

log = logging.getLogger('odds_featured')


def credits_per_call():
    return 10 * len(MARKETS) * math.ceil(len(BOOKS) / 10)


def to_utc(ts_et):
    '''Naive ET timestamp -> aware UTC (DST-safe).'''
    return pd.Timestamp(ts_et).tz_localize(ET).tz_convert(UTC)


def iso(ts):
    return pd.Timestamp(ts).tz_convert(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def utc_to_et(s):
    '''API UTC string -> naive ET timestamp.'''
    if s is None:
        return pd.NaT
    return pd.Timestamp(s).tz_convert(ET).tz_localize(None)


def api_key():
    key = os.environ.get('ODDS_HIST_KEY')
    if key:
        return key
    with open('betting/config.json') as f:
        return json.load(f)['oddsApi']['historical']


class FeaturedHistory:
    def __init__(self, hist_db=HIST_DB, out_dir=OUT_DIR, session=None, key=None):
        self.hist_db = hist_db
        self.out_dir = out_dir
        self.session = session or requests.Session()
        self._key = key
        os.makedirs(out_dir, exist_ok=True)
        self.db = sqlite3.connect(os.path.join(out_dir, 'featured.db'))
        self.db.execute('''create table if not exists tasks (
            season text, game_date text, pull_sequence text, target_et text, tip_et text,
            status text default 'pending', snapshot_ts text, credits int, n_events int, n_rows int,
            file text, note text, updated_at text,
            primary key (game_date, pull_sequence, target_et))''')
        self.db.commit()
        self.remaining = None
        self.spent = 0

    @property
    def key(self):
        if self._key is None:
            self._key = api_key()
        return self._key

    # ---------------------------------------------------------------- planning
    def events(self, seasons, start=None, end=None):
        con = sqlite3.connect('file:{}?mode=ro'.format(self.hist_db), uri=True)
        q = 'select event_id, game_id, season, game_date, commence_time_et, home_team, away_team from events where season in ({})'
        ev = pd.read_sql(q.format(','.join('?' * len(seasons))), con, params=list(seasons))
        ev['commence_time_et'] = pd.to_datetime(ev.commence_time_et)
        if start:
            ev = ev[ev.game_date >= start]
        if end:
            ev = ev[ev.game_date <= end]
        return ev

    def plan(self, seasons, pulls, start=None, end=None):
        ev = self.events(seasons, start, end)
        rows = []
        for (season, gd), g in ev.groupby(['season', 'game_date']):
            last_tip = g.commence_time_et.max()
            for p in pulls:
                if p in PULL_TIMES:
                    hh, mm = PULL_TIMES[p]
                    target = pd.Timestamp(gd) + pd.Timedelta(hours=hh, minutes=mm)
                    if target >= last_tip - pd.Timedelta(minutes=CLOSE_MIN_BEFORE_TIP):
                        continue                      # every game is at or past its close: nothing pre-game left
                    rows.append((season, gd, p, str(target), None))
                elif p == 'close':
                    for tip in sorted(g.commence_time_et.unique()):
                        tip = pd.Timestamp(tip)
                        rows.append((season, gd, p, str(tip - pd.Timedelta(minutes=CLOSE_MIN_BEFORE_TIP)), str(tip)))
                else:
                    raise ValueError('unknown pull {}'.format(p))
        self.db.executemany('insert or ignore into tasks (season, game_date, pull_sequence, target_et, tip_et) '
                            'values (?, ?, ?, ?, ?)', rows)
        self.db.commit()
        return len(rows)

    def estimate(self, seasons, pulls, start=None, end=None):
        self.plan(seasons, pulls, start, end)
        t = self.tasks(seasons, pulls, start, end)
        t = t[t.status == 'pending'].groupby(['season', 'pull_sequence']).size().rename('calls').reset_index()
        t['credits_max'] = t.calls * credits_per_call()
        return t

    def tasks(self, seasons, pulls, start=None, end=None):
        t = pd.read_sql('select * from tasks', self.db)
        t = t[t.season.isin(seasons) & t.pull_sequence.isin(pulls)]
        if start:
            t = t[t.game_date >= start]
        if end:
            t = t[t.game_date <= end]
        return t.sort_values(['game_date', 'target_et'])

    # ---------------------------------------------------------------- pulling
    def _get(self, params, tries=6):
        wait = 2
        for _ in range(tries):
            try:
                r = self.session.get(URL, params=params, timeout=60)
            except requests.RequestException as e:
                log.warning('request failed (%s); retrying in %ss', e, wait)
                time.sleep(wait)
                wait *= 2
                continue
            rem = r.headers.get('x-requests-remaining')
            if rem is not None:
                self.remaining = float(rem)
            last = float(r.headers.get('x-requests-last', 0) or 0)
            if r.status_code == 200:
                return r.json(), last, 'ok'
            if r.status_code in (404, 422):
                return None, last, 'empty'
            if r.status_code in (401, 403):
                return None, last, 'refused'
            if r.status_code == 429 or r.status_code >= 500:
                log.warning('HTTP %s; retrying in %ss', r.status_code, wait)
                time.sleep(wait)
                wait *= 2
                continue
            return None, last, 'error {}'.format(r.status_code)
        return None, 0, 'error retries'

    def _params(self, task):
        target = to_utc(task.target_et)
        if task.pull_sequence == 'close':
            tip = to_utc(task.tip_et)
            frm, to = tip, tip
        else:
            frm = target                                                   # only games not yet tipped
            to = to_utc(pd.Timestamp(task.game_date) + pd.Timedelta(hours=30))   # through 6am ET next day
        return {'apiKey': self.key, 'bookmakers': ','.join(BOOKS), 'markets': ','.join(MARKETS),
                'oddsFormat': 'american', 'dateFormat': 'iso', 'date': iso(target),
                'commenceTimeFrom': iso(frm), 'commenceTimeTo': iso(to)}

    def parse(self, payload, task, ev):
        snap = utc_to_et(payload.get('timestamp'))
        rows = []
        for e in payload.get('data', []):
            for bk in e.get('bookmakers', []):
                for mk in bk.get('markets', []):
                    lu = utc_to_et(mk.get('last_update') or bk.get('last_update'))
                    for oc in mk.get('outcomes', []):
                        rows.append({'event_id': e['id'], 'api_commence_et': utc_to_et(e.get('commence_time')),
                                     'home_team': e.get('home_team'), 'away_team': e.get('away_team'),
                                     'book': bk['key'], 'market': mk['key'], 'name': oc.get('name'),
                                     'point': oc.get('point'), 'price': oc.get('price'), 'last_update': lu})
        df = pd.DataFrame(rows)
        if df.empty:
            return df, 0
        n_api = df.event_id.nunique()
        df = df.merge(ev[['event_id', 'game_id', 'season', 'game_date', 'commence_time_et']], on='event_id')
        dropped = n_api - df.event_id.nunique()
        if dropped:
            log.info('%s %s: %s events not in the regular-season manifest, dropped', task.game_date,
                     task.pull_sequence, dropped)
        df = df[df.game_date == task.game_date]
        df['pull_sequence'] = task.pull_sequence
        df['target_ts'] = pd.Timestamp(task.target_et)
        df['snapshot_ts'] = snap
        df['point'] = df.point.astype(float)
        df['price'] = df.price.astype('Int64')
        df['is_stale'] = (df.snapshot_ts - df.last_update) > pd.Timedelta(minutes=STALE_MIN)
        cols = ['season', 'game_date', 'game_id', 'event_id', 'commence_time_et', 'home_team', 'away_team',
                'pull_sequence', 'target_ts', 'snapshot_ts', 'book', 'market', 'name', 'point', 'price',
                'last_update', 'is_stale']
        return df[cols], dropped

    def _write(self, df, task):
        d = os.path.join(self.out_dir, task.season, task.pull_sequence)
        os.makedirs(d, exist_ok=True)
        tag = '' if task.pull_sequence != 'close' else '_' + pd.Timestamp(task.tip_et).strftime('%H%M')
        path = os.path.join(d, '{}{}_{}.parquet'.format(task.game_date, tag, datetime.now().strftime('%Y%m%d%H%M%S')))
        df.to_parquet(path, index=False, compression='snappy')
        return path

    def _mark(self, task, status, **kw):
        kw.update({'status': status, 'updated_at': datetime.now().isoformat(timespec='seconds')})
        sets = ', '.join('{} = ?'.format(k) for k in kw)
        self.db.execute('update tasks set {} where game_date = ? and pull_sequence = ? and target_et = ?'.format(sets),
                        list(kw.values()) + [task.game_date, task.pull_sequence, task.target_et])
        self.db.commit()

    def run(self, seasons, pulls, start=None, end=None, max_credits=None, min_remaining=25000):
        self.plan(seasons, pulls, start, end)
        ev = self.events(seasons, start, end)
        todo = self.tasks(seasons, pulls, start, end)
        todo = todo[todo.status == 'pending']
        log.info('%s pending calls (~%s credits max)', len(todo), len(todo) * credits_per_call())
        done = 0
        for task in todo.itertuples(index=False):
            if max_credits is not None and self.spent + credits_per_call() > max_credits:
                log.info('stopping: per-run cap of %s credits reached', max_credits)
                break
            if self.remaining is not None and self.remaining - credits_per_call() < min_remaining:
                log.info('stopping: %s credits left, floor %s', self.remaining, min_remaining)
                break
            payload, cost, status = self._get(self._params(task))
            self.spent += cost
            if status == 'refused':
                log.error('key refused (401/403); stopping')
                break
            if status != 'ok':
                self._mark(task, 'empty' if status == 'empty' else 'error', credits=cost, note=status)
                continue
            df, dropped = self.parse(payload, task, ev)
            if df.empty:
                self._mark(task, 'empty', credits=cost, snapshot_ts=str(utc_to_et(payload.get('timestamp'))),
                           n_events=0, n_rows=0)
                continue
            path = self._write(df, task)
            self._mark(task, 'done', credits=cost, snapshot_ts=str(df.snapshot_ts.iloc[0]),
                       n_events=int(df.event_id.nunique()), n_rows=len(df), file=path,
                       note='dropped {}'.format(dropped) if dropped else None)
            done += 1
            if done % 25 == 0:
                log.info('%s calls done, %s credits this run, %s left', done, self.spent, self.remaining)
        log.info('run finished: %s calls, %s credits, %s left', done, self.spent, self.remaining)
        return done

    def report(self, seasons, pulls):
        t = self.tasks(seasons, pulls)
        st = t.groupby(['season', 'pull_sequence', 'status']).size().unstack(fill_value=0)
        st['credits'] = t.groupby(['season', 'pull_sequence']).credits.sum()
        ev = self.events(seasons)
        g = read_featured(seasons, pulls, self.out_dir)
        if g.empty:
            return st, pd.DataFrame()
        sp = g[g.market == 'spreads']
        games = ev.groupby('season').game_id.nunique().rename('games')
        cov = (sp.groupby(['season', 'pull_sequence']).game_id.nunique().rename('games_with_spread').reset_index()
                 .merge(games.reset_index(), on='season'))
        cov['share'] = cov.games_with_spread / cov.games
        cov['median_books'] = cov.apply(lambda r: sp[(sp.season == r.season) & (sp.pull_sequence == r.pull_sequence)]
                                        .groupby('game_id').book.nunique().median(), axis=1)
        cov['stale_share'] = cov.apply(lambda r: sp[(sp.season == r.season) & (sp.pull_sequence == r.pull_sequence)]
                                       .is_stale.mean(), axis=1)
        return st, cov


# ---------------------------------------------------------------- reading
def read_featured(seasons, pulls=('morning', 'midday'), out_dir=OUT_DIR):
    files = [os.path.join(out_dir, s, p, '*.parquet') for s in seasons for p in pulls
             if os.path.isdir(os.path.join(out_dir, s, p)) and os.listdir(os.path.join(out_dir, s, p))]
    if not files:
        return pd.DataFrame()
    import duckdb
    df = duckdb.connect().execute('select * from read_parquet({}, union_by_name=true)'.format(files)).df()
    # a re-run adds files; keep the latest file's rows per game, pull and book
    df = df.sort_values('snapshot_ts').drop_duplicates(['game_id', 'pull_sequence', 'target_ts', 'book', 'market', 'name'],
                                                       keep='last')
    return df


WIDE_PATH = os.path.join(OUT_DIR, 'team_lines_wide.parquet')
BOOK_FIELDS = ['spread', 'spread_price', 'total', 'over_price', 'under_price']


def build_wide(seasons, pulls=('morning', 'midday', 'close'), out_dir=OUT_DIR, hist_db=HIST_DB, path=None):
    """Wide game lines: one row per game_id x team_id x pull_sequence, a column per book and field. No consensus:
    every number is one book's own quote, so sharpness and pricing curves can be fit per book.

    Per book: {book}_spread (that team's spread, negative = favored), {book}_spread_price, {book}_total,
    {book}_over_price, {book}_under_price. Plus ids: opp_id, home, target_ts, snapshot_ts.

    Built from the raw long files (the source of record, never edited) and rewritten in full each time, like the
    other derived tables. Writes `path` (default nba/data/odds_hist_featured/team_lines_wide.parquet) if given."""
    df = read_featured(seasons, pulls, out_dir)
    if df.empty:
        return pd.DataFrame()
    con = sqlite3.connect('file:{}?mode=ro'.format(hist_db), uri=True)
    ta = pd.read_sql('select team_name, team_id from team_alias', con)
    ta['team_id'] = ta.team_id.astype(str)
    ev = pd.read_sql('select game_id, home_team, away_team from events', con)
    ev = (ev.merge(ta.rename(columns={'team_name': 'home_team', 'team_id': 'home_id'}), on='home_team')
            .merge(ta.rename(columns={'team_name': 'away_team', 'team_id': 'away_id'}), on='away_team'))
    keys = ['season', 'game_date', 'game_id', 'pull_sequence']
    snap = df.groupby(['game_id', 'pull_sequence']).agg(snapshot_ts=('snapshot_ts', 'max'), target_ts=('target_ts', 'max'))

    sp = df[(df.market == 'spreads') & df.point.notna()]
    unmatched = sorted(set(sp.name) - set(ta.team_name))
    if unmatched:
        log.warning('spread outcomes with no team_alias row, dropped: %s', unmatched)
    sp = sp.merge(ta, left_on='name', right_on='team_name')
    if sp.duplicated(keys + ['team_id', 'book']).any():
        raise ValueError('duplicate spread rows per book; check read_featured de-duplication')
    sw = sp.pivot(index=keys + ['team_id'], columns='book', values=['point', 'price'])
    sw.columns = ['{}_{}'.format(b, 'spread' if f == 'point' else 'spread_price') for f, b in sw.columns]
    sw = sw.reset_index()

    to = df[(df.market == 'totals') & df.point.notna()]
    if to.duplicated(keys + ['book', 'name']).any():
        raise ValueError('duplicate total rows per book; check read_featured de-duplication')
    tp = to[to.name == 'Over'].pivot(index=keys, columns='book', values=['point', 'price'])
    tp.columns = ['{}_{}'.format(b, 'total' if f == 'point' else 'over_price') for f, b in tp.columns]
    tu = to[to.name == 'Under'].pivot(index=keys, columns='book', values='price')
    tu.columns = ['{}_under_price'.format(b) for b in tu.columns]
    tw = tp.join(tu, how='outer').reset_index()

    w = sw.merge(tw, on=keys, how='left').merge(snap.reset_index(), on=['game_id', 'pull_sequence'], how='left')
    w = w.merge(ev[['game_id', 'home_id', 'away_id']], on='game_id', how='left')
    w['home'] = (w.team_id == w.home_id).astype(int)
    w['opp_id'] = np.where(w.home == 1, w.away_id, w.home_id)

    for b in BOOKS:                       # fixed schema: every book's columns exist even if it never posted
        for f in BOOK_FIELDS:
            c = '{}_{}'.format(b, f)
            if c not in w:
                w[c] = np.nan
    lead = ['season', 'game_date', 'game_id', 'team_id', 'opp_id', 'home', 'pull_sequence', 'target_ts', 'snapshot_ts']
    book_cols = ['{}_{}'.format(b, f) for b in BOOKS for f in BOOK_FIELDS]
    w = w[lead + book_cols].sort_values(['game_date', 'game_id', 'pull_sequence', 'team_id']).reset_index(drop=True)
    for c in book_cols:
        w[c] = w[c].astype('Int64') if c.endswith('_price') else w[c].astype(float)
    if path:
        w.to_parquet(path, index=False, compression='snappy')
    return w


def book_lines(w, book):
    """Pull one book's columns out of the wide frame under generic names. `book` is a key ('fanduel') or a
    per-row Series of keys (e.g. the book the bet or the threes line came from). Adds lines_book and the
    per-book derived fields: is_favorite (0.5 at a pick'em), fav_margin, dog_margin, implied_total."""
    out = w.copy()
    bk = book if isinstance(book, pd.Series) else pd.Series(book, index=w.index)
    out['lines_book'] = bk
    for f in BOOK_FIELDS:
        vals = np.full(len(w), np.nan)
        for b in bk.dropna().unique():
            col = '{}_{}'.format(b, f)
            if col in w:
                m = (bk == b).values
                vals[m] = w.loc[m, col].astype(float).values
        out[f] = vals
    out['is_favorite'] = np.select([out.spread < 0, out.spread > 0], [1.0, 0.0], np.where(out.spread.isna(), np.nan, .5))
    out['fav_margin'] = np.maximum(-out.spread, 0)
    out['dog_margin'] = np.maximum(out.spread, 0)
    out['implied_total'] = out.total / 2 - out.spread / 2
    return out


def team_lines(seasons, pulls=('morning', 'midday'), books=('fanduel', 'draftkings', 'betmgm'), out_dir=OUT_DIR,
               hist_db=HIST_DB):
    """One row per game_id x team_id: the first pull in `pulls` that has the game, then the first book in `books`
    with a spread at that pull. No averaging across books; `lines_book` says which book every row came from.
    For bets, use book_lines(w, bet_book) on the wide frame instead, so the line comes from the bet's book."""
    path = os.path.join(out_dir, 'team_lines_wide.parquet')
    if os.path.exists(path):
        w = pd.read_parquet(path)
        w = w[w.season.isin(seasons)]
    else:
        w = build_wide(seasons, pulls, out_dir, hist_db)
    cols = ['game_id', 'team_id', 'lines_pull', 'lines_book', 'lines_snapshot_ts', 'spread', 'spread_price', 'total',
            'over_price', 'under_price', 'implied_total', 'is_favorite', 'fav_margin', 'dog_margin']
    if w.empty:
        return pd.DataFrame(columns=cols)
    w = w[w.pull_sequence.isin(pulls)].copy()
    pick = pd.Series(np.nan, index=w.index, dtype=object)
    for b in reversed(books):             # first listed book wins
        pick = pick.where(w['{}_spread'.format(b)].isna(), b)
    w = book_lines(w[pick.notna()], pick[pick.notna()])
    order = {p: i for i, p in enumerate(pulls)}
    w['rank'] = w.pull_sequence.map(order)
    w = w.sort_values('rank').drop_duplicates(['game_id', 'team_id'])
    w = w.rename(columns={'pull_sequence': 'lines_pull', 'snapshot_ts': 'lines_snapshot_ts'})
    return w[cols].reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--season', nargs='+', required=True)
    ap.add_argument('--pulls', default='morning,midday', help='comma list of morning, midday, close')
    ap.add_argument('--start')
    ap.add_argument('--end')
    ap.add_argument('--estimate', action='store_true')
    ap.add_argument('--run', action='store_true')
    ap.add_argument('--report', action='store_true')
    ap.add_argument('--build-wide', action='store_true', help='rewrite team_lines_wide.parquet from the raw files')
    ap.add_argument('--max-credits', type=int)
    ap.add_argument('--min-remaining', type=int, default=25000)
    a = ap.parse_args()

    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                        handlers=[logging.FileHandler('logs/odds_featured.log'), logging.StreamHandler()])
    pulls = [p.strip() for p in a.pulls.split(',')]
    fh = FeaturedHistory()
    pd.set_option('display.width', 200)
    if a.estimate:
        est = fh.estimate(a.season, pulls, a.start, a.end)
        print(est.to_string(index=False))
        print('total: {} calls, at most {:,} credits ({} per call)'.format(est.calls.sum(), est.credits_max.sum(),
                                                                         credits_per_call()))
    if a.run:
        fh.run(a.season, pulls, a.start, a.end, a.max_credits, a.min_remaining)
    if a.report:
        st, cov = fh.report(a.season, pulls)
        print(st.to_string())
        print(cov.round(3).to_string(index=False))
    if a.build_wide:
        all_seasons = sorted(set(pd.read_sql('select distinct season from tasks', fh.db).season))
        w = build_wide(all_seasons, ('morning', 'midday', 'close'), path=WIDE_PATH)
        print('wrote {} ({:,} rows: {})'.format(WIDE_PATH, len(w), w.groupby(['season', 'pull_sequence']).size().to_dict()))


if __name__ == '__main__':
    main()
