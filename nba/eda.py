'''
Shared loaders for the monthly EDA notebooks (Notebooks/eda/YYYY-MM.ipynb), so every question uses the same definitions.

    from nba import eda
    con = eda.connect()                      # read-only; NBA_DB_PATH or nba/data/database/nba.db
    b = eda.bets(con)                        # bets with model p, book p, outcome, P&L, role, attempts bucket
    o = eda.model_vs_books(con)              # every two-way player_threes line in the odds snapshots, de-vigged, with model p and outcome
    eda.summary(b, ['side', 'role'])         # expected vs actual wins, date-clustered z, P&L

Definitions
- model p: P(over line) summed from the stored PMF in `predictions` (the day's last run, so after 11:30 it may differ from bet time)
- outcome: threes made = lc + rc + abv FGM in `plyrLogs`; a player who logged no minutes is dropped (books void DNPs)
- role: starter if he started >= half of his previous 10 games (this one excluded)
- bucket: season-to-date 3PA per game before tonight: 0-2, 2-5, 5-7, 7+, or '1st game'
- book p on bets: implied from the price taken, vig included (= break-even); in the odds snapshots: proportional de-vig of the two-way price
- z: clustered by game date (bets on one night share news, pace and lineups)
Odds snapshots: ODDS_LIVE_DIR or nba/data/odds_live (on the MacBook, rsync them from the mini). Pikkit: nba/data/pikkit.
'''
import glob
import os
import sqlite3

import numpy as np
import pandas as pd

from .constants import _BASE_DIR

DB = os.environ.get('NBA_DB_PATH', os.path.join(_BASE_DIR, 'data', 'database', 'nba.db'))
ODDS_LIVE = os.environ.get('ODDS_LIVE_DIR', os.path.join(_BASE_DIR, 'data', 'odds_live'))
PIKKIT = os.path.join(_BASE_DIR, 'data', 'pikkit')
SEASON = '2026-27'
SEASON_START = '2026-10-20'
BUCKETS = ([0, 2, 5, 7, 99], ['0-2', '2-5', '5-7', '7+'])


def connect(db=DB):
    '''Read-only connection, so a notebook can never write to the database.'''
    return sqlite3.connect(f'file:{db}?mode=ro', uri=True)


def _date_col(con, table):
    '''`date` or `game_date` (the dev rename of bets / predictions / lines).'''
    cols = [r[1] for r in con.execute(f'PRAGMA table_info({table})')]
    return 'game_date' if 'game_date' in cols else 'date'


def implied(american):
    a = np.asarray(american, dtype=float)
    return np.where(a > 0, 100 / (a + 100), -a / (100 - a))


def decimal(american):
    a = np.asarray(american, dtype=float)
    return np.where(a > 0, 1 + a / 100, 1 + 100 / -a)


# ── Model ──
def pmf(con, start=SEASON_START, end='9999'):
    '''Stored PMFs, one row per (player_id, date), one column per make count.'''
    d = _date_col(con, 'predictions')
    p = pd.read_sql(f"SELECT player_id, {d} AS date, number AS k, model_prob AS p FROM predictions "
                    f"WHERE market = 'threes' AND {d} BETWEEN ? AND ?", con, params=[start, end])
    p['player_id'] = p.player_id.astype(str)
    w = p.pivot_table(index=['player_id', 'date'], columns='k', values='p').fillna(0)
    w.columns = w.columns.astype(float)
    return w


def p_over(wide, player_id, date, line):
    '''Model P(over line) for aligned arrays of player_id, date, line (rows missing from the PMF -> NaN).'''
    ks = wide.columns.values
    cum = wide.values[:, ::-1].cumsum(axis=1)[:, ::-1]          # cum[:, j] = P(X >= ks[j])
    idx = wide.index.get_indexer(pd.MultiIndex.from_arrays([np.asarray(player_id, str), np.asarray(date)]))
    j = np.searchsorted(ks, np.asarray(line, float), side='right')    # first k > line
    out = np.where((idx >= 0) & (j < len(ks)), cum[np.clip(idx, 0, None), np.clip(j, 0, len(ks) - 1)], 0.0)
    return np.where(idx >= 0, out, np.nan)


def model_means(wide):
    '''Predicted mean threes and P(0) per (player_id, date).'''
    ks = wide.columns.values
    return pd.DataFrame({'mean_pred': wide.values @ ks, 'p0': wide[0.0].values}, index=wide.index).reset_index()


# ── Outcomes and roles ──
def outcomes(con, start=SEASON_START, end='9999', history_days=400):
    '''
    One row per player-game played (min > 0): threes, attempts, minutes, team, and pregame context:
    start_rate (last 10 games), min_avg (last 10), avg3pa (season to date) and bucket.
    '''
    lo = (pd.Timestamp(start) - pd.Timedelta(days=history_days)).strftime('%Y-%m-%d')
    L = pd.read_sql("""SELECT player_id, team_id, game_id, game_date AS date, min, Starter AS started,
                              lc_fga + rc_fga + abv_fga AS fg3a, lc_fgm + rc_fgm + abv_fgm AS fg3m
                       FROM plyrLogs WHERE game_date BETWEEN ? AND ? AND game_id LIKE '002%' AND min > 0""",
                    con, params=[lo, end])
    L[['player_id', 'team_id']] = L[['player_id', 'team_id']].astype(str)
    L = L.sort_values(['player_id', 'date'])
    g = L.groupby('player_id')
    L['start_rate'] = g.started.transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
    L['min_avg'] = g['min'].transform(lambda s: s.shift(1).rolling(10, min_periods=3).mean())
    L['season'] = np.where(L.date.str[5:7].astype(int) >= 8, L.date.str[:4], (L.date.str[:4].astype(int) - 1).astype(str))
    L['avg3pa'] = L.groupby(['player_id', 'season']).fg3a.transform(lambda s: s.shift(1).expanding().mean())
    L['bucket'] = pd.cut(L.avg3pa, *BUCKETS[:1], right=False, labels=BUCKETS[1]).cat.add_categories('1st game').fillna('1st game')
    L['role'] = np.where(L.start_rate >= 0.5, 'starter', np.where(L.start_rate.notna(), 'bench', 'unknown'))
    return L[L.date >= start].reset_index(drop=True)


def starters_out(con, start=SEASON_START, end='9999'):
    '''
    Per team-game: teammates who started >= half of the team's previous 10 games and logged no minutes tonight
    (box score, so diagnostic only). Departures (next game for another team) are dropped.
    Output: team_id, date, n_out, out_share (their 3PA share over those 10 games), any_first (one of them played the previous game).
    '''
    L = pd.read_sql("""SELECT player_id, team_id, game_id, game_date AS date, min, Starter AS started, lc_fga + rc_fga + abv_fga AS fg3a
                       FROM plyrLogs WHERE game_date BETWEEN ? AND ? AND game_id LIKE '002%'""",
                    con, params=[(pd.Timestamp(start) - pd.Timedelta(days=60)).strftime('%Y-%m-%d'), end])
    L[['player_id', 'team_id']] = L[['player_id', 'team_id']].astype(str)
    L = L[L['min'] > 0]
    tg = L[['team_id', 'game_id', 'date']].drop_duplicates(['team_id', 'game_id']).sort_values(['team_id', 'date'])
    tg['n'] = tg.groupby('team_id').cumcount()
    L = L.merge(tg[['team_id', 'game_id', 'n']], on=['team_id', 'game_id'])
    team3pa = L.groupby(['team_id', 'n']).fg3a.sum()
    nxt = L.sort_values('date')[['player_id', 'team_id', 'date']]
    rows = []
    for team, g in L.groupby('team_id'):
        for n in range(1, g.n.max() + 1):
            w = g[(g.n < n) & (g.n >= n - 10)]
            games = w.n.nunique()
            if games < 3:
                continue
            st = w.groupby('player_id').agg(starts=('started', 'sum'), fg3a=('fg3a', 'sum'))
            st = st[st.starts >= 0.5 * games]
            tonight, last = set(g[g.n == n].player_id), set(g[g.n == n - 1].player_id)
            for pid, r in st.iterrows():
                if pid not in tonight:
                    rows.append({'team_id': team, 'n': n, 'out_pid': pid, 'first': pid in last,
                                 'share': r.fg3a / team3pa.loc[team].loc[w.n.unique()].sum()})
    out = pd.DataFrame(rows).merge(tg[['team_id', 'n', 'date']], on=['team_id', 'n'])
    later = out.merge(nxt, left_on='out_pid', right_on='player_id', suffixes=('', '_next'))
    later = later[later.date_next > later.date].sort_values('date_next').drop_duplicates(['team_id', 'n', 'out_pid'])
    traded = set(map(tuple, later[later.team_id_next != later.team_id][['team_id', 'n', 'out_pid']].values))
    out = out[[(t, n, p) not in traded for t, n, p in out[['team_id', 'n', 'out_pid']].values]]
    out = out.groupby(['team_id', 'date']).agg(n_out=('out_pid', 'size'), out_share=('share', 'sum'), any_first=('first', 'max')).reset_index()
    return out[out.date >= start]


# ── Bets ──
def bets(con, start=SEASON_START, end='9999', user='EAC'):
    '''Model bets (threes) with model p, break-even book p, outcome, P&L and the outcomes() context. DNPs dropped.'''
    d = _date_col(con, 'bets')
    b = pd.read_sql(f"""SELECT player_id, {d} AS date, over_under AS side, number AS line, bet_book AS book,
                               final_line AS price, bet_amount AS stake, boost, free_bet
                        FROM bets WHERE market = 'threes' AND user = ? AND {d} BETWEEN ? AND ?""",
                    con, params=[user, start, end])
    b['player_id'] = b.player_id.astype(str)
    w = pmf(con, start, end)
    po = p_over(w, b.player_id, b.date, b.line)
    b['model_p'] = np.where(b.side == 'Over', po, 1 - po)
    b = b.merge(outcomes(con, start, end), on=['player_id', 'date'], how='inner')
    b['won'] = np.where(b.side == 'Over', b.fg3m > b.line, b.fg3m < b.line).astype(int)
    b['dec'] = decimal(b.price)
    b['book_p'] = 1 / b.dec
    b['pnl'] = np.where(b.won == 1, b.stake * (b.dec - 1) * (1 + b.boost.fillna(0)), np.where(b.free_bet == 1, 0, -b.stake))
    b['claimed'] = b.model_p - b.book_p
    return b


def clustered_z(resid, dates):
    '''Sum of residuals over its date-clustered SE.'''
    se = np.sqrt((pd.Series(resid).groupby(np.asarray(dates)).sum() ** 2).sum())
    return pd.Series(resid).sum() / se if se > 0 else np.nan


def summary(b, by):
    '''Bets by group: expected wins (model, break-even), actual wins, z of actual vs model, stake, P&L, ROI.'''
    def f(x):
        return pd.Series({'bets': len(x), 'model_exp': x.model_p.sum(), 'book_exp': x.book_p.sum(), 'wins': x.won.sum(),
                          'z_model': clustered_z(x.won.values - x.model_p.values, x.date.values),
                          'stake': x.stake.sum(), 'pnl': x.pnl.sum(), 'roi%': 100 * x.pnl.sum() / x.stake.sum()})
    return b.groupby(by, observed=True).apply(f).round(2)


# ── Odds snapshots ──
def odds(snaps=('open', 'midday', 'close'), market='player_threes', season=SEASON, con=None):
    '''
    Two-way prices from the live snapshots, one row per snap, date, book, player and line: book_p (proportional de-vig),
    hold, player_id (names matched through the Pikkit normalizer; unmatched rows dropped).
    '''
    from betting.pikkit import norm
    con = con or connect()
    frames = []
    for snap in snaps:
        for f in sorted(glob.glob(os.path.join(ODDS_LIVE, season, snap, '*.parquet'))):
            x = pd.read_parquet(f, columns=['game_date', 'snapshot_ts', 'book', 'market', 'player_name', 'side', 'line', 'price', 'is_stale'])
            frames.append(x[(x.market == market) & ~x.is_stale].assign(snap=snap))
    o = pd.concat(frames).sort_values('snapshot_ts')
    o = o.drop_duplicates(['snap', 'game_date', 'book', 'player_name', 'side', 'line'], keep='last')
    w = o.pivot_table(index=['snap', 'game_date', 'book', 'player_name', 'line'], columns='side', values='price', aggfunc='last')
    w = w.dropna(subset=['Over', 'Under']).reset_index().rename(columns={'game_date': 'date'})
    po, pu = implied(w.Over), implied(w.Under)
    w['book_p'], w['hold'] = po / (po + pu), po + pu - 1
    pl = pd.read_sql('SELECT player_id, name FROM players', con)
    pl = pl.assign(nkey=pl.name.map(norm), player_id=pl.player_id.astype(str)).drop_duplicates('nkey')
    w['nkey'] = w.player_name.map(norm)
    return w.merge(pl[['nkey', 'player_id']], on='nkey')


def model_vs_books(con, snaps=('open', 'midday', 'close'), start=SEASON_START, end='9999'):
    '''odds() for players who played, with model_p (same line) and y = went over.'''
    o = odds(snaps, con=con)
    o = o[o.date.between(start, end)]
    oc = outcomes(con, start, end)
    o = o.merge(oc, on=['player_id', 'date'])
    o['model_p'] = p_over(pmf(con, start, end), o.player_id, o.date, o.line)
    o['y'] = (o.fg3m > o.line).astype(int)
    return o.dropna(subset=['model_p'])


def consensus(o, snap='midday', min_books=2, exclude=('novig',)):
    '''Mean de-vigged p across books per player-date-line in one snapshot (Novig's alternate ladders excluded by default).'''
    x = o[(o.snap == snap) & ~o.book.isin(exclude)]
    keep = [c for c in ('bucket', 'role', 'start_rate', 'avg3pa', 'team_id') if c in x.columns]
    c = x.groupby(['date', 'player_id', 'line']).agg(book_p=('book_p', 'mean'), model_p=('model_p', 'first'), y=('y', 'first'),
                                                     nb=('book', 'nunique'), **{k: (k, 'first') for k in keep}).reset_index()
    return c[c.nb >= min_books]


def compare(x):
    '''Model vs book on the same lines: mean p, Brier and log loss difference (+ = model worse), date-clustered z.'''
    eps = 1e-4
    ll = lambda p, y: -(y * np.log(np.clip(p, eps, 1 - eps)) + (1 - y) * np.log(1 - np.clip(p, eps, 1 - eps)))
    db = (x.model_p - x.y) ** 2 - (x.book_p - x.y) ** 2
    dl = ll(x.model_p, x.y) - ll(x.book_p, x.y)
    return pd.Series({'n': len(x), 'actual': x.y.mean(), 'model': x.model_p.mean(), 'book': x.book_p.mean(),
                      'dBrier_x1000': 1000 * db.mean(), 'z_brier': clustered_z(db.values, x.date.values),
                      'dLogLoss_x1000': 1000 * dl.mean(), 'z_ll': clustered_z(dl.values, x.date.values)})


def blend(c):
    '''Logistic recalibration of the outcome on logit(model) and logit(book), SEs clustered by date.'''
    import statsmodels.api as sm
    lg = lambda p: np.log(np.clip(p, 1e-4, 1 - 1e-4) / (1 - np.clip(p, 1e-4, 1 - 1e-4)))
    X = sm.add_constant(pd.DataFrame({'model': lg(c.model_p.values), 'book': lg(c.book_p.values)}))
    f = sm.GLM(c.y.values, X, family=sm.families.Binomial()).fit(cov_type='cluster', cov_kwds={'groups': pd.factorize(c.date)[0]})
    return pd.DataFrame({'coef': f.params, 'se': f.bse}).round(3)


# ── Pikkit ──
def pikkit_model_plays(con):
    '''Settled model plays (threes straights) from every Pikkit export, with season, book p, closing p, CLV and role.'''
    from betting.pikkit import classify, norm
    files = sorted(glob.glob(os.path.join(PIKKIT, '*.csv')))
    pk = pd.concat([pd.read_csv(f, dtype={'bet_id': str}) for f in files]).drop_duplicates('bet_id', keep='last')
    m = classify(pk)
    m = m[(m.kind == 'model') & m.status.isin(['SETTLED_WIN', 'SETTLED_LOSS'])].copy()
    m['won'] = (m.status == 'SETTLED_WIN').astype(int)
    m['dec'] = m.odds.astype(float)
    m['book_p'], m['close_p'] = 1 / m.dec, 1 / m.closing_line.astype(float)
    m['clv'] = m.close_p - m.book_p
    m['pl'], m['amount'] = m.profit.astype(float), m.amount.astype(float)
    m['amer'] = np.where(m.dec >= 2, (m.dec - 1) * 100, -100 / (m.dec - 1))
    y = m.date.str[:4].astype(int)
    m['season'] = np.where(m.date.str[5:7].astype(int) >= 8, y, y - 1)
    m['season'] = m.season.astype(str) + '-' + (m.season + 1).astype(str).str[2:]
    pl = pd.read_sql('SELECT player_id, name FROM players', con)
    pl = pl.assign(nkey=pl.name.map(norm), player_id=pl.player_id.astype(str)).drop_duplicates('nkey')
    m = m.merge(pl[['nkey', 'player_id']], on='nkey', how='left')
    oc = outcomes(con, '2024-10-01')[['player_id', 'date', 'role', 'bucket']]
    return m.merge(oc, on=['player_id', 'date'], how='left')
