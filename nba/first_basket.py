'''
First basket / first team basket: is there an edge?  Spec: project doc claude/first-basket-edge-test.md

The question is answered in order, cheapest first, and each step can stop the project:
    step 0  market   Book overround, de-vigged calibration and flat ROI by price bucket (no model).
    step 1  labels   Walk-forward share model (who scores his team's first FG) scored on labels only, back to 2022-23.
    step 2  edge     Model vs de-vigged book on the backfilled close: paired log loss, the conditional-logit blend
                     (does the model add information the price doesn't have?) and a walk-forward betting replay.

Model
    P(player i scores the game's first FG) = P(team T scores first) x P(i is T's first scorer)
    share: conditional logit over each team's starters on log-share features, all strictly before the game date
           (shift(1) rolling windows); P(bench player scores first) is a constant from the training window.
    team:  logistic on home + the difference in the two teams' prior rates of scoring the first FG.
    The factorization assumes who scores first for a team doesn't depend on whether the team scores first;
    factorization_check() tests it.

Grading
    plyrLogs.game_first / team_first = first made FIELD GOAL (PlayByPlayV3 'Made Shot'). DraftKings and BetMGM grade
    on that. FanDuel grades first basket on the first score including free throws (~9% of games differ), so it is
    left out unless asked for (--books).

Usage (from the repo root; reads NBA_DB_PATH and nba/data/odds_hist_extra/)
    python -m nba.first_basket --step labels --start 2023-10-01 --end 2025-04-15
    python -m nba.first_basket --step market --market team --seasons 2025-26
    python -m nba.first_basket --step edge --market game --seasons 2024-25
    python -m nba.first_basket --step all --market team --seasons 2025-26,2026-27 --out logs/first_basket
'''
import os
import glob
import sqlite3
import logging
import argparse

import numpy as np
import pandas as pd
from scipy.optimize import brentq
import statsmodels.api as sm

from .constants import _BASE_DIR

logger = logging.getLogger(__name__)

ODDS_DIR = os.path.join(_BASE_DIR, 'data', 'odds_hist')
EXTRA_DIR = os.path.join(_BASE_DIR, 'data', 'odds_hist_extra')
DB_PATH = os.environ.get('NBA_DB_PATH', os.path.join(_BASE_DIR, 'data', 'database', 'nba.db'))

MARKETS = {'game': 'player_first_basket', 'team': 'player_first_team_basket'}
LABEL = {'game': 'game_first', 'team': 'team_first'}
FG_GRADED = ['draftkings', 'betmgm']            # grade on the first made field goal = plyrLogs flags
BET_BOOKS = ['draftkings', 'betmgm', 'fanduel']  # accounts held; fanduel only when asked (first score incl. FTs)
TRAIN_START = '2022-08-01'

SHARE_FEATURES = ['log_q1_share', 'log_fga_share', 'log_tf_share', 'fg_pct_c']
HEURISTIC = {'log_q1_share': 1.0}               # p proportional to prior Q1 field-goal points: the no-fit baseline
FEATURE_SETS = {
    'heuristic_q1': None,
    'q1': ['log_q1_share'],
    'q1_fga': ['log_q1_share', 'log_fga_share'],
    'q1_fga_tf': ['log_q1_share', 'log_fga_share', 'log_tf_share'],
    'full': SHARE_FEATURES,
}
TEAM_FEATURES = ['d_rate']   # allowed-first = 1 - scored-first in every game, so an allow term is collinear

PLAYER_WINDOW = 20       # games, rate features (sum over sum)
TF_WINDOW = 120          # games, own first-basket history
TEAM_WINDOW = 82         # games, team scoring / allowing first
TF_PRIOR_N = 20          # pseudo-starts pulling a player's own team-first rate toward his Q1 share
LEAGUE_FG = 0.47
FILL = {'q1_fgpts_pg': 2.0, 'q1_min_pg': 8.0, 'fga_pm': 0.35, 'fg_pct': LEAGUE_FG}   # when no starter has history

PRICE_BUCKETS = [-np.inf, 300, 500, 800, 1200, 2000, np.inf]
PRICE_LABELS = ['< +300', '+300/+500', '+500/+800', '+800/+1200', '+1200/+2000', '+2000+']


def season_window(season):
    '''2024-25 -> (2024-08-01, 2025-07-31)'''
    y = int(season[:4])
    return '{}-08-01'.format(y), '{}-07-31'.format(y + 1)


def american_to_decimal(a):
    a = np.asarray(a, float)
    return np.where(a > 0, 1 + a / 100, 1 + 100 / np.abs(a))


# ---------------------------------------------------------------- conditional logit

def _groups(groups):
    '''Sort order, contiguous group codes (sorted) and group start offsets'''
    codes = pd.factorize(np.asarray(groups))[0]
    order = np.argsort(codes, kind='stable')
    g = codes[order]
    new = np.r_[True, g[1:] != g[:-1]]
    return order, np.cumsum(new) - 1, np.flatnonzero(new)


def softmax_within(eta, groups):
    '''Softmax of eta within each group, returned in the input order'''
    eta = np.asarray(eta, float)
    order, g, starts = _groups(groups)
    e = eta[order]
    e = np.exp(e - np.maximum.reduceat(e, starts)[g])
    p = np.empty_like(eta)
    p[order] = e / np.add.reduceat(e, starts)[g]
    return p


class Clogit:
    '''
    Conditional (McFadden) logit: one chosen row per group, P(row) = softmax(X beta) within the group, no intercept.
    Newton-Raphson. Standard errors are the sandwich clustered on `clusters` (game date), because games on one
    night share books, refs and pace. Groups without exactly one chosen row are dropped (counted in n_dropped).
    '''

    def __init__(self, ridge=1e-6):
        self.ridge = ridge

    def fit(self, X, y, groups, clusters=None, names=None, maxiter=50, tol=1e-9):
        X = np.asarray(X, float)
        y = np.asarray(y, float)
        names = names or ['x{}'.format(i) for i in range(X.shape[1])]
        order, g, starts = _groups(groups)
        X, y = X[order], y[order]
        cl = np.asarray(clusters)[order] if clusters is not None else g
        wins = np.add.reduceat(y, starts)
        keep_g = wins == 1
        self.n_dropped = int((~keep_g).sum())
        keep = keep_g[g]
        X, y, cl = X[keep], y[keep], cl[keep]
        _, g, starts = _groups(g[keep])
        if len(starts) == 0:
            raise ValueError('No groups with exactly one chosen row')
        k = X.shape[1]
        beta = np.zeros(k)
        ll_old = -np.inf
        for _ in range(maxiter):
            ll, score_g, H = self._parts(X, y, g, starts, beta)
            step = np.linalg.solve(H - self.ridge * np.eye(k), score_g.sum(0))
            t = 1.0
            while t > 1e-4:
                cand = beta - t * step
                if self._parts(X, y, g, starts, cand)[0] >= ll - 1e-12:
                    break
                t /= 2
            beta = cand
            if abs(ll - ll_old) < tol:
                break
            ll_old = ll
        ll, score_g, H = self._parts(X, y, g, starts, beta)
        Hinv = np.linalg.inv(H - self.ridge * np.eye(k))
        ccodes = pd.factorize(cl[starts])[0]
        S = np.zeros((ccodes.max() + 1, k))
        np.add.at(S, ccodes, score_g)
        V = Hinv @ (S.T @ S) @ Hinv
        nc = S.shape[0]
        V *= nc / max(nc - 1, 1)
        self.params = pd.Series(beta, index=names)
        self.bse = pd.Series(np.sqrt(np.diag(V)), index=names)
        self.cov = pd.DataFrame(V, index=names, columns=names)
        self.llf = ll
        self.n_groups = len(starts)
        self.n_clusters = nc
        return self

    @staticmethod
    def _parts(X, y, g, starts, beta):
        eta = X @ beta
        mx = np.maximum.reduceat(eta, starts)
        e = np.exp(eta - mx[g])
        den = np.add.reduceat(e, starts)
        p = e / den[g]
        xbar = np.add.reduceat(p[:, None] * X, starts)
        xwin = np.add.reduceat(y[:, None] * X, starts)
        ll = np.sum(np.add.reduceat(y * (eta - mx[g]), starts) - np.log(den))
        d = X - xbar[g]
        H = -(p[:, None] * d).T @ d
        return ll, xwin - xbar, H

    def summary(self):
        return pd.DataFrame({'coef': self.params, 'se_clustered': self.bse, 'z': self.params / self.bse})

    def predict(self, X, groups):
        return softmax_within(np.asarray(X, float) @ self.params.values, groups)


# ---------------------------------------------------------------- outcomes and features

def load_outcomes(conn, start=TRAIN_START, end='2100-01-01'):
    '''
    Player-games with minutes, the first-FG flags, starter flag (plyrLogs.Starter, repaired Nov 8),
    home / opponent from teamLog and Q1 box score. Validity:
        valid_game: exactly one game_first, and that player has team_first
        valid_team: exactly one team_first on the team (the team made a field goal and the flags loaded)
    '''
    q = '''
        select l.player_id, l.team_id, l.game_id, substr(l.game_date, 1, 10) as game_date, l.min,
               coalesce(l.Starter, 0) as starter, coalesce(l.team_first, 0) as team_first,
               coalesce(l.game_first, 0) as game_first, l.usagePercentage as usg,
               coalesce(l.ra_fga, 0) + coalesce(l.paint_fga, 0) + coalesce(l.mid_fga, 0)
                 + coalesce(l.lc_fga, 0) + coalesce(l.rc_fga, 0) + coalesce(l.abv_fga, 0) as fga,
               coalesce(l.ra_fgm, 0) + coalesce(l.paint_fgm, 0) + coalesce(l.mid_fgm, 0)
                 + coalesce(l.lc_fgm, 0) + coalesce(l.rc_fgm, 0) + coalesce(l.abv_fgm, 0) as fgm,
               t.opp_id, t.home, q.min as q1_min, q.pts - coalesce(q.ftm, 0) as q1_fgpts
        from plyrLogs l
        join teamLog t on t.game_id = l.game_id and cast(t.team_id as text) = cast(l.team_id as text)
        left join plyrQ1Logs q on q.game_id = l.game_id and q.player_id = l.player_id
        where l.game_id like '002%' and l.min > 0 and substr(l.game_date, 1, 10) between ? and ?'''
    df = pd.read_sql(q, conn, params=(start, end))
    for c in ['player_id', 'team_id', 'game_id', 'opp_id']:
        df[c] = df[c].astype(str)
    df['game_date'] = pd.to_datetime(df.game_date)
    df['home'] = df.home.astype(int)

    g = df.groupby('game_id')
    n_gf = g.game_first.transform('sum')
    gf_tf = (df.game_first * df.team_first).groupby(df.game_id).transform('sum')
    df['valid_game'] = (n_gf == 1) & (gf_tf == 1)
    df['valid_team'] = df.groupby(['game_id', 'team_id']).team_first.transform('sum') == 1
    n_teams = g.team_id.transform('nunique')
    df['valid_game'] &= n_teams == 2
    return df.sort_values(['player_id', 'game_date', 'game_id']).reset_index(drop=True)


def _prior_sum(df, col, window, by='player_id'):
    '''Sum over the previous `window` games, strictly before the current row (shift(1) is the leak boundary)'''
    return df.groupby(by)[col].transform(lambda s: s.shift(1).rolling(window, min_periods=1).sum())


def build_features(out):
    '''
    Player features (strictly prior games) and their within-team-game shares among starters.
        q1_fgpts_pg  Q1 field-goal points per game (sum over sum, games with a Q1 row)
        fga_pm, q1_min_pg  FGA per minute and Q1 minutes -> expected early FGA
        tf_eb        own team-first rate as a starter, shrunk toward his Q1 share with TF_PRIOR_N pseudo-starts
        fg_pct_c     FG% minus league average
    Players with fewer than 5 prior games get the starters' median (flag new_player).
    '''
    df = out.copy()
    df['has_q1'] = df.q1_min.notna().astype(float)
    df['q1_fgpts0'] = df.q1_fgpts.fillna(0)
    df['q1_min0'] = df.q1_min.fillna(0)
    df['tf_start'] = df.team_first * df.starter * df.valid_team
    df['start_valid'] = df.starter * df.valid_team
    s = {c: _prior_sum(df, c, PLAYER_WINDOW) for c in ['q1_fgpts0', 'q1_min0', 'has_q1', 'fga', 'fgm', 'min']}
    df['n_prior'] = df.groupby('player_id').cumcount()
    df['q1_fgpts_pg'] = s['q1_fgpts0'] / s['has_q1'].replace(0, np.nan)
    df['q1_min_pg'] = s['q1_min0'] / s['has_q1'].replace(0, np.nan)
    df['fga_pm'] = s['fga'] / s['min'].replace(0, np.nan)
    df['fg_pct'] = s['fgm'] / s['fga'].replace(0, np.nan)
    df['tf_k'] = _prior_sum(df, 'tf_start', TF_WINDOW)
    df['tf_n'] = _prior_sum(df, 'start_valid', TF_WINDOW)
    df['new_player'] = (df.n_prior < 5).astype(int)
    df.loc[df.new_player == 1, ['q1_fgpts_pg', 'q1_min_pg', 'fga_pm', 'fg_pct']] = np.nan

    st = df[df.starter == 1].copy()
    grp = ['game_id', 'team_id']
    for c in ['q1_fgpts_pg', 'q1_min_pg', 'fga_pm', 'fg_pct']:
        med = st.groupby(grp)[c].transform('median')
        st[c] = st[c].fillna(med).fillna(FILL[c])   # fixed defaults: a frame-wide median would leak future games
    q1raw = st.q1_fgpts_pg.clip(lower=0.25)
    st['q1_share'] = q1raw / q1raw.groupby([st.game_id, st.team_id]).transform('sum')
    fraw = (st.fga_pm * st.q1_min_pg).clip(lower=0.05)
    st['fga_share'] = fraw / fraw.groupby([st.game_id, st.team_id]).transform('sum')
    tf_eb = (st.tf_k.fillna(0) + TF_PRIOR_N * st.q1_share) / (st.tf_n.fillna(0) + TF_PRIOR_N)
    st['tf_share'] = tf_eb / tf_eb.groupby([st.game_id, st.team_id]).transform('sum')
    for c in ['q1', 'fga', 'tf']:
        st['log_{}_share'.format(c)] = np.log(st['{}_share'.format(c)])
    st['fg_pct_c'] = (st.fg_pct - LEAGUE_FG).clip(-.15, .15)
    st['n_starters'] = st.groupby(grp).player_id.transform('size')
    return st.reset_index(drop=True)


def build_team_frame(out):
    '''One row per team-game: scored_first (label), prior rates of scoring and allowing the first FG'''
    v = out[out.valid_game]
    tg = (out.groupby(['game_id', 'game_date', 'team_id', 'opp_id', 'home'], as_index=False)
          .agg(valid_game=('valid_game', 'max')))
    sf = v.groupby(['game_id', 'team_id']).game_first.max().rename('scored_first').reset_index()
    tg = tg.merge(sf, on=['game_id', 'team_id'], how='left')
    opp = tg[['game_id', 'team_id', 'scored_first']].rename(columns={'team_id': 'opp_id', 'scored_first': 'allowed_first'})
    tg = tg.merge(opp, on=['game_id', 'opp_id'], how='left').sort_values(['team_id', 'game_date', 'game_id'])
    for c, new in [('scored_first', 'rate'), ('allowed_first', 'allow')]:
        tg[new] = tg.groupby('team_id')[c].transform(
            lambda s: s.shift(1).rolling(TEAM_WINDOW, min_periods=10).mean()).fillna(.5)
    o = tg[['game_id', 'team_id', 'rate', 'allow']].rename(
        columns={'team_id': 'opp_id', 'rate': 'opp_rate', 'allow': 'opp_allow'})
    tg = tg.merge(o, on=['game_id', 'opp_id'], how='left')
    tg['d_rate'] = tg.rate - tg.opp_rate
    tg['d_allow'] = tg.opp_allow - tg.allow
    return tg.reset_index(drop=True)


# ---------------------------------------------------------------- models

class ShareModel:
    '''P(starter i is his team's first scorer). fixed: dict of coefficients to use without fitting (heuristic).'''

    def __init__(self, features=SHARE_FEATURES, fixed=None):
        self.fixed = fixed
        self.features = list(fixed) if fixed else list(features)

    def fit(self, train):
        t = train[train.valid_team & (train.starter == 1)]
        grp = t.game_id + '_' + t.team_id
        won = t.team_first.groupby(grp).sum()
        self.bench_rate = float(1 - won.mean()) if len(won) else 0.0
        if self.fixed:
            self.clogit = None
            self.params = pd.Series(self.fixed)
            return self
        self.clogit = Clogit().fit(t[self.features].values, t.team_first.values, grp.values,
                                   clusters=t.game_date.values, names=self.features)
        self.params = self.clogit.params
        return self

    def predict(self, df):
        eta = df[self.features].values @ self.params[self.features].values
        return softmax_within(eta, (df.game_id + '_' + df.team_id).values) * (1 - self.bench_rate)


class TeamFirstModel:
    '''P(team scores the first FG): logistic from the home team's side, so the two teams sum to 1'''

    def fit(self, teams):
        h = teams[(teams.home == 1) & teams.valid_game & teams.scored_first.notna()]
        X = sm.add_constant(h[TEAM_FEATURES], has_constant='add')
        self.res = sm.Logit(h.scored_first.astype(float), X).fit(disp=0)
        return self

    def predict(self, teams):
        X = sm.add_constant(teams[TEAM_FEATURES], has_constant='add')
        X.loc[teams.home == 0, TEAM_FEATURES] *= -1   # same game from the away side
        eta = X.values @ self.res.params.values
        p_home_view = 1 / (1 + np.exp(-eta))
        return np.where(teams.home == 1, p_home_view, 1 - p_home_view)


def walk_forward(feat, teams, start, end, train_start=TRAIN_START, features=SHARE_FEATURES, fixed=None):
    '''
    Monthly refits on an expanding window (train_start to the first of the month), predictions for that month.
    Output: one row per starter-game with p_share (team first), p_team, p_game (= p_team x p_share).
    '''
    rows = []
    for m in pd.period_range(start, end, freq='M'):
        lo, hi = m.start_time, m.end_time
        test = feat[(feat.game_date >= lo) & (feat.game_date <= hi)]
        if test.empty:
            continue
        train = feat[(feat.game_date >= train_start) & (feat.game_date < lo)]
        tt = teams[(teams.game_date >= train_start) & (teams.game_date < lo)]
        if train.valid_team.sum() < 500 or tt.valid_game.sum() < 200:
            logger.info('{}: training window too small, skipped'.format(m))
            continue
        smod = ShareModel(features, fixed).fit(train)
        tmod = TeamFirstModel().fit(tt)
        p = test[['game_id', 'game_date', 'team_id', 'opp_id', 'home', 'player_id', 'min', 'team_first',
                  'game_first', 'valid_team', 'valid_game', 'new_player', 'n_starters']].copy()
        p['p_share'] = smod.predict(test)
        tm = teams[(teams.game_date >= lo) & (teams.game_date <= hi)].copy()
        tm['p_team'] = tmod.predict(tm)
        p = p.merge(tm[['game_id', 'team_id', 'p_team']], on=['game_id', 'team_id'], how='left')
        p['p_game'] = p.p_team * p.p_share
        p['fit_month'] = str(m)
        p['bench_rate'] = smod.bench_rate
        rows.append(p)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def clustered_mean(x, clusters):
    '''Mean of x and its standard error clustered on `clusters`'''
    d = pd.DataFrame({'x': np.asarray(x, float), 'c': np.asarray(clusters)}).dropna()
    n = len(d)
    m = d.x.mean()
    s = d.groupby('c').x.agg(['sum', 'size'])
    se = np.sqrt(((s['sum'] - s['size'] * m) ** 2).sum() * len(s) / max(len(s) - 1, 1)) / n
    return m, se


def group_log_loss(pred, pcol, label, grp):
    '''
    Log loss per group of the actual first scorer. A non-starter winner (or a group where the winner isn't listed)
    scores the leftover mass 1 - sum(p over starters), floored at 1e-4.
    '''
    t = pred.assign(_w=pred[label] * pred[pcol])
    a = t.groupby(grp).agg(game_date=('game_date', 'first'), won=('_w', 'sum'), sw=(label, 'sum'), tot=(pcol, 'sum'))
    p = np.where(a.sw > 0, a.won.clip(lower=1e-6), (1 - a.tot).clip(lower=1e-4))
    res = a[['game_date']].copy()
    res['ll'] = -np.log(p)
    res['starter_won'] = (a.sw > 0).values
    return res.reset_index()


def compare_share_models(feat, teams, start, end, sets=None, train_start=TRAIN_START):
    '''
    Step 1, labels only: walk-forward team-share log loss for each feature set, paired against the
    no-fit heuristic (p proportional to prior Q1 FG points). Negative delta = better than the heuristic.
    '''
    sets = sets or FEATURE_SETS
    scored = {}
    coefs = {}
    for name, fs in sets.items():
        fixed = HEURISTIC if fs is None else None
        wf = walk_forward(feat, teams, start, end, train_start, fs or SHARE_FEATURES, fixed)
        v = wf[wf.valid_team]
        scored[name] = group_log_loss(v, 'p_share', 'team_first', ['game_id', 'team_id'])
        if fs is not None:
            last = ShareModel(fs).fit(feat[(feat.game_date >= train_start) & (feat.game_date < pd.Timestamp(end))])
            coefs[name] = last.clogit.summary()
    base = scored['heuristic_q1']
    rows = []
    for name, s in scored.items():
        m = base.merge(s, on=['game_id', 'team_id', 'game_date'], suffixes=('_base', ''))
        d, se = clustered_mean(m.ll - m.ll_base, m.game_date)
        uniform = -np.log(1 / 5)
        rows.append({'model': name, 'team_games': len(m), 'log_loss': m.ll.mean(),
                     'skill_vs_uniform': 1 - m.ll.mean() / uniform,
                     'delta_vs_heuristic_x1000': 1000 * d, 'se_x1000': 1000 * se, 'z': d / se if se else np.nan})
    return pd.DataFrame(rows), coefs


def factorization_check(feat, teams):
    '''
    Is a team's first scorer independent of whether the team scores first?  Among team-first scorers, regress
    game_first on log Q1 share (centers who win the tip and score on possession one would show a positive slope)
    with the team's own P(score first) features as controls. Slope ~ 0 -> p_game = p_team x p_share holds.
    '''
    t = feat[(feat.team_first == 1) & feat.valid_game].merge(
        teams[['game_id', 'team_id'] + TEAM_FEATURES], on=['game_id', 'team_id'])
    X = sm.add_constant(t[['log_q1_share', 'home'] + TEAM_FEATURES].astype(float))
    res = sm.Logit(t.game_first.astype(float), X).fit(disp=0, cov_type='cluster',
                                                     cov_kwds={'groups': pd.factorize(t.game_date)[0]})
    return pd.DataFrame({'coef': res.params, 'se_clustered': res.bse, 'p': res.pvalues})


# ---------------------------------------------------------------- market

def devig(q, method='power'):
    '''
    Implied probabilities of one multi-runner market -> fair probabilities summing to 1.
    mult: q / sum(q). power: q**k with k solved so they sum to 1 (takes more vig from longshots).
    shin: Shin (1993) insider-trading model, also longshot-heavy.
    '''
    q = np.asarray(q, float)
    tot = q.sum()
    if method == 'mult' or len(q) < 2 or abs(tot - 1) < 1e-12:
        return q / tot
    if method == 'power':
        k = brentq(lambda k: np.sum(q ** k) - 1, 1e-3, 50)
        return q ** k
    if method == 'shin':
        if tot <= 1:
            return q / tot

        def p_of(z):
            return (np.sqrt(z * z + 4 * (1 - z) * q * q / tot) - z) / (2 * (1 - z))
        z = brentq(lambda z: p_of(z).sum() - 1, 0, 0.999)
        p = p_of(z)
        return p / p.sum()
    raise ValueError('method must be mult, power or shin')


def _read_sqlite(path, sql):
    if not os.path.exists(path):
        return pd.DataFrame()
    with sqlite3.connect(path) as c:
        try:
            return pd.read_sql(sql, c)
        except Exception:
            return pd.DataFrame()


def load_market(seasons, market='game', pulls=('close',), odds_dir=EXTRA_DIR, core_dir=ODDS_DIR):
    '''
    Raw first-basket rows -> one row per game x pull x book x player (latest snapshot of each pull), with
    game_id from the events manifest and player_id from player_alias (extra store, then the core store).
    Run `python -m nba.first_basket --build-alias` if the extra store's alias table is empty.
    '''
    import duckdb
    files = [f for s in seasons for p in pulls for f in glob.glob(os.path.join(odds_dir, s, p, '*.parquet'))]
    if not files:
        raise FileNotFoundError('No {} parquet files for {} {}'.format(odds_dir, seasons, pulls))
    raw = duckdb.sql('''select cast(game_date as varchar) as game_date, home_team, away_team, pull_sequence,
                               snapshot_ts, book, market, player_name, side, price
                        from read_parquet({}, union_by_name = true) where market = '{}' '''.format(
        files, MARKETS[market])).df()
    # player name sits in description (player_name); if a book returns it as the outcome name instead, use that
    swap = raw.player_name.isna() & ~raw.side.isin(['Yes', 'No', 'Over', 'Under'])
    raw.loc[swap, 'player_name'] = raw.loc[swap, 'side']
    raw = raw[~raw.side.isin(['No', 'Under'])].dropna(subset=['player_name', 'price'])
    # nobody is shorter than +100 to score first; prices like -9900 are settled / suspended markets
    raw = raw[raw.price >= 100]
    keys = ['game_date', 'home_team', 'away_team', 'pull_sequence', 'book']
    raw = raw[raw.snapshot_ts == raw.groupby(keys).snapshot_ts.transform('max')]
    raw = raw.drop_duplicates(keys + ['player_name'])

    ev = pd.concat([_read_sqlite(os.path.join(d, 'odds_hist.db'),
                                 'select game_id, game_date, home_team, away_team from events')
                    for d in [odds_dir, core_dir]]).drop_duplicates(['game_date', 'home_team', 'away_team'])
    alias = pd.concat([_read_sqlite(os.path.join(d, 'odds_hist.db'), 'select player_name, player_id from player_alias')
                       for d in [odds_dir, core_dir]]).drop_duplicates('player_name')
    df = raw.merge(ev, on=['game_date', 'home_team', 'away_team'], how='inner')
    n = df.player_name.nunique()
    df = df.merge(alias, on='player_name', how='left')
    miss = df[df.player_id.isna()].player_name.unique()
    logger.info('{} {}: {:,} rows, {} names, {} without an alias'.format(market, seasons, len(df), n, len(miss)))
    df = df.dropna(subset=['player_id'])
    df['player_id'] = df.player_id.astype(str)
    df['game_id'] = df.game_id.astype(str)
    df['price'] = df.price.astype(float)
    df['dec'] = american_to_decimal(df.price)
    df['q'] = 1 / df.dec
    df.attrs['unmatched_names'] = list(miss)
    return df


def market_probs(mkt, out, market='game', method='power'):
    '''
    De-vig each book's market among players who played (late scratches are void), keeping only listings that
    carry every starter of the market (both teams for game, the team for team). Adds p_fair and the book's
    overround on the full listing.
    '''
    o = out[['game_id', 'player_id', 'team_id', 'starter']]
    m = mkt.merge(o, on=['game_id', 'player_id'], how='inner')
    gk = ['game_id', 'pull_sequence', 'book'] + (['team_id'] if market == 'team' else [])
    starters = out[out.starter == 1].groupby(['game_id'] + (['team_id'] if market == 'team' else [])).player_id.size()
    starters = starters.rename('n_starters_all').reset_index()
    m = m.merge(starters, on=[c for c in starters.columns if c != 'n_starters_all'], how='left')
    m['n_starters_listed'] = m.groupby(gk).starter.transform('sum')
    m['complete'] = m.n_starters_listed == m.n_starters_all
    m['overround'] = m.groupby(gk).q.transform('sum') - 1
    m = m[m.complete].copy()
    m['p_fair'] = m.groupby(gk).q.transform(lambda s: pd.Series(devig(s.values, method), index=s.index))
    return m


def consensus(mp, books=None, market='game'):
    '''Geometric mean of the books' fair p per player, renormalized within the market group'''
    m = mp if books is None else mp[mp.book.isin(books)]
    gk = ['game_id', 'pull_sequence'] + (['team_id'] if market == 'team' else [])
    c = (m.assign(lp=np.log(m.p_fair)).groupby(gk + ['player_id', 'starter'], as_index=False)
         .agg(lp=('lp', 'mean'), n_books=('book', 'nunique')))
    c['p_book'] = np.exp(c.lp)
    c['p_book'] /= c.groupby(gk).p_book.transform('sum')
    return c.drop(columns='lp')


def book_summary(mp_all):
    '''Overround and coverage by book (complete listings only)'''
    g = mp_all.drop_duplicates(['game_id', 'pull_sequence', 'book'] + (['team_id'] if 'team_id' in mp_all else []))
    return g.groupby(['pull_sequence', 'book']).agg(markets=('game_id', 'size'),
                                                    overround_mean=('overround', 'mean'),
                                                    overround_median=('overround', 'median')).reset_index()


# ---------------------------------------------------------------- evaluation

def date_bootstrap(pl, stake, dates, B=2000, seed=0):
    '''ROI and 90% interval from resampling whole game dates (bets on one night are correlated)'''
    d = pd.DataFrame({'pl': pl, 's': stake, 'd': dates}).groupby('d')[['pl', 's']].sum()
    if d.empty:
        return np.nan, np.nan, np.nan
    rng = np.random.default_rng(seed)
    w = rng.multinomial(len(d), np.full(len(d), 1 / len(d)), size=B)
    roi = (w @ d.pl.values) / (w @ d.s.values)
    return d.pl.sum() / d.s.sum(), np.quantile(roi, .05), np.quantile(roi, .95)


def roi_table(bets, by=None):
    '''Flat 1-unit bets: n, wins, expected wins (model and price), ROI with date-bootstrap interval'''
    def one(b):
        roi, lo, hi = date_bootstrap(b.pl.values, np.ones(len(b)), b.game_date.values)
        return pd.Series({'bets': len(b), 'wins': b.y.sum(), 'exp_wins_p': b.p.sum(),
                          'exp_wins_price': (1 / b.dec).sum(), 'avg_price': b.price.median(),
                          'roi': roi, 'roi_lo90': lo, 'roi_hi90': hi})
    if bets.empty:
        return pd.DataFrame()
    if by is None:
        return one(bets).to_frame().T
    return bets.groupby(by, observed=True)[[c for c in bets.columns if c not in by]].apply(one).reset_index()


def price_bucket_roi(ev):
    '''Step 0: flat-bet every starter at every bet book, by price bucket (favorite-longshot bias check)'''
    e = ev.assign(p=ev.p_book, pl=ev.y * ev.dec - 1,
                  bucket=pd.cut(ev.price, PRICE_BUCKETS, labels=PRICE_LABELS))
    return roi_table(e, ['book', 'bucket'])


def calibration_table(ev, pcol, bins=(0, .05, .08, .11, .14, .18, .25, .35, 1)):
    '''Reliability of p against outcomes by probability bucket, with a binomial z'''
    e = ev.drop_duplicates(['game_id', 'player_id'])
    e = e.assign(b=pd.cut(e[pcol], bins))
    t = e.groupby('b', observed=True).agg(n=('y', 'size'), p_mean=(pcol, 'mean'), actual=('y', 'mean'),
                                          exp_wins=(pcol, 'sum'), wins=('y', 'sum')).reset_index()
    t['z'] = (t.wins - t.exp_wins) / np.sqrt((t.p_mean * (1 - t.p_mean) * t.n).clip(lower=1e-9))
    return t


def blend_fit(ev, market='game'):
    '''
    Information test: conditional logit of the winner on [log p_model, log p_book] over starters.
    Model weight > 0 with a clustered z beyond ~2 -> the model knows something the price doesn't.
    (log of raw implied q in place of p_book would make the book weight a power de-vig.)
    '''
    gk = ['game_id'] + (['team_id'] if market == 'team' else [])
    e = ev.drop_duplicates(gk + ['player_id'])
    X = np.column_stack([np.log(e.p_model.clip(1e-6)), np.log(e.p_book.clip(1e-6))])
    grp = e[gk].astype(str).agg('_'.join, axis=1).values
    cl = Clogit().fit(X, e.y.values, grp, clusters=e.game_date.values, names=['w_model', 'w_book'])
    s = cl.summary()
    s['groups'] = cl.n_groups
    s['dates'] = cl.n_clusters
    return s, cl


def walk_forward_blend(ev, market='game', min_groups=300):
    '''p_blend per month from blend weights fit on earlier months only; NaN until min_groups are available'''
    gk = ['game_id'] + (['team_id'] if market == 'team' else [])
    e = ev.drop_duplicates(gk + ['player_id']).copy()
    e['month'] = e.game_date.dt.to_period('M')
    e['p_blend'] = np.nan
    grp = e[gk].astype(str).agg('_'.join, axis=1)
    X = np.column_stack([np.log(e.p_model.clip(1e-6)), np.log(e.p_book.clip(1e-6))])
    starter_mass = e.groupby(gk).p_book.transform('sum').values
    for m in sorted(e.month.unique()):
        tr = (e.month < m).values
        if grp[tr].nunique() < min_groups:
            continue
        cl = Clogit().fit(X[tr], e.y.values[tr], grp.values[tr], clusters=e.game_date.values[tr])
        te = (e.month == m).values
        e.loc[te, 'p_blend'] = cl.predict(X[te], grp.values[te]) * starter_mass[te]
    return ev.merge(e[gk + ['player_id', 'p_blend']], on=gk + ['player_id'], how='left')


def simulate_bets(ev, pcol, thresholds=(0, .05, .10, .20), shop=True):
    '''
    Bets where p * decimal - 1 > threshold at the close, flat 1 unit. shop: one bet per player at the best
    price among the bet books (accounts at each); otherwise each book separately.
    '''
    e = ev.dropna(subset=[pcol]).copy()
    if shop:
        e = e.sort_values('dec', ascending=False).drop_duplicates(['game_id', 'player_id'])
    e['p'] = e[pcol]
    e['ev'] = e.p * e.dec - 1
    e['pl'] = e.y * e.dec - 1
    rows = []
    for t in thresholds:
        r = roi_table(e[e.ev > t])
        if r.empty:
            continue
        r.insert(0, 'threshold', t)
        r.insert(1, 'p_source', pcol)
        r['claimed_ev'] = e[e.ev > t].ev.mean()
        rows.append(r)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def roi_power(p=0.12, price=650, n=(500, 1000, 2500, 5000, 10000)):
    '''SE of flat-bet ROI at a typical first-basket price: what sample can resolve what edge'''
    d = float(american_to_decimal(price))
    sd = np.sqrt(p * (1 - p)) * d
    return pd.DataFrame({'bets': n, 'roi_se': [sd / np.sqrt(k) for k in n],
                         'min_detectable_roi_2se': [2 * sd / np.sqrt(k) for k in n]})


def market_eval(out, mp, market='game', ref_books=None, bet_books=FG_GRADED, pull='close'):
    '''
    Starter-level rows at `pull` with the label y and consensus fair p_book, keeping only groups (game, or
    team-game) where every starter has a book price. Returns (one row per starter, one row per starter x bet book).
    '''
    label = LABEL[market]
    valid = 'valid_game' if market == 'game' else 'valid_team'
    gk = ['game_id'] + (['team_id'] if market == 'team' else [])
    keys = ['game_id', 'player_id']
    mp = mp[mp.pull_sequence == pull]
    cons = consensus(mp, ref_books, market)
    cons = cons[cons.starter == 1]
    base = out[(out.starter == 1) & out[valid]][['game_id', 'game_date', 'team_id', 'player_id', label]]
    base = base.rename(columns={label: 'y'})
    base['n_all'] = base.groupby(gk).player_id.transform('size')
    g = base.merge(cons[keys + ['p_book', 'n_books']], on=keys, how='inner')
    g = g[g.groupby(gk).player_id.transform('size') == g.n_all].drop(columns='n_all')
    prices = mp[mp.book.isin(bet_books) & (mp.starter == 1)][keys + ['book', 'price', 'dec']]
    return g, g.merge(prices, on=keys, how='inner')


def run_market(conn, seasons, market='game', method='power', bet_books=FG_GRADED, ref_books=None,
               odds_dir=EXTRA_DIR, core_dir=ODDS_DIR, train_start=TRAIN_START):
    '''Step 0, no model: overround, de-vigged calibration and flat ROI by price bucket'''
    hi = max(season_window(s)[1] for s in seasons)
    out = load_outcomes(conn, train_start, hi)
    mkt = load_market(seasons, market, ('open', 'morning', 'midday', 'close'), odds_dir, core_dir)
    mp = market_probs(mkt, out, market, method)
    groups, ev = market_eval(out, mp, market, ref_books, bet_books)
    gk = ['game_id'] + (['team_id'] if market == 'team' else [])
    tables = {
        'coverage': pd.DataFrame([{'market': market, 'seasons': ','.join(seasons), 'odds_games': mkt.game_id.nunique(),
                                   'eval_groups': groups[gk].drop_duplicates().shape[0], 'eval_bet_rows': len(ev),
                                   'unmatched_names': len(mkt.attrs.get('unmatched_names', []))}]),
        'book_summary': book_summary(mp),
        'price_bucket_roi': price_bucket_roi(ev),
        'calibration_book': calibration_table(groups, 'p_book'),
        'power': roi_power(),
    }
    return tables, {'out': out, 'mkt': mkt, 'mp': mp, 'groups': groups, 'ev': ev}


def run_edge(conn, seasons, market='game', method='power', bet_books=FG_GRADED, ref_books=None,
             odds_dir=EXTRA_DIR, core_dir=ODDS_DIR, train_start=TRAIN_START, features=SHARE_FEATURES):
    '''Steps 0 and 2 for one market over the given odds seasons. Output: dict of tables (+ eval_frame)'''
    tables, f = run_market(conn, seasons, market, method, bet_books, ref_books, odds_dir, core_dir, train_start)
    out, groups, ev = f['out'], f['groups'], f['ev']
    feat = build_features(out)
    teams = build_team_frame(out)
    gk = ['game_id'] + (['team_id'] if market == 'team' else [])
    first = groups.game_date.min().strftime('%Y-%m-%d')
    last = groups.game_date.max().strftime('%Y-%m-%d')
    preds = walk_forward(feat, teams, first, last, train_start, features)
    pcol = 'p_game' if market == 'game' else 'p_share'
    g = groups.merge(preds[['game_id', 'player_id', pcol, 'fit_month']].rename(columns={pcol: 'p_model'}),
                     on=['game_id', 'player_id'], how='left')
    g = g[g.groupby(gk).p_model.transform(lambda s: s.notna().all())]
    g = walk_forward_blend(g, market)
    ev = ev.merge(g[gk + ['player_id', 'p_model', 'p_blend']], on=gk + ['player_id'], how='inner')

    sub = g.dropna(subset=['p_blend'])
    ll = {c: group_log_loss(sub, c, 'y', gk).set_index(gk) for c in ['p_model', 'p_book', 'p_blend']}
    cmp = pd.DataFrame({'game_date': ll['p_book'].game_date,
                        'model_minus_book': ll['p_model'].ll - ll['p_book'].ll,
                        'blend_minus_book': ll['p_blend'].ll - ll['p_book'].ll})
    rows = []
    for c in ['model_minus_book', 'blend_minus_book']:
        d, se = clustered_mean(cmp[c], cmp.game_date)
        rows.append({'comparison': c, 'groups': len(cmp), 'mean_ll_book': ll['p_book'].ll.mean(),
                     'delta_x1000': 1000 * d, 'se_x1000': 1000 * se, 'z': d / se if se else np.nan})
    blend_full, _ = blend_fit(g, market)
    tables.update({
        'calibration_model': calibration_table(g, 'p_model'),
        'log_loss_vs_book': pd.DataFrame(rows),
        'blend_weights': blend_full.reset_index().rename(columns={'index': 'term'}),
        'bets_shop': pd.concat([simulate_bets(ev, c) for c in ['p_model', 'p_blend', 'p_book']], ignore_index=True),
        'bets_by_book': pd.concat([simulate_bets(ev, c, shop=False) for c in ['p_model', 'p_blend']],
                                  ignore_index=True),
        'eval_frame': ev,
    })
    return tables


def run_labels(conn, start, end, train_start=TRAIN_START):
    '''Step 1: share-model and team-first development on labels only (no odds)'''
    out = load_outcomes(conn, train_start, end)
    feat = build_features(out)
    teams = build_team_frame(out)
    table, coefs = compare_share_models(feat, teams, start, end, train_start=train_start)
    wf = walk_forward(feat, teams, start, end, train_start)
    sf = wf[wf.valid_game].groupby(['game_id', 'team_id'], as_index=False).agg(
        p_team=('p_team', 'first'), y=('game_first', 'max'), home=('home', 'first'), game_date=('game_date', 'first'))
    sf = sf[sf.game_id.duplicated(keep=False)]
    ll_t = -(sf.y * np.log(sf.p_team) + (1 - sf.y) * np.log(1 - sf.p_team))
    d, se = clustered_mean(ll_t - np.log(2), sf.game_date)
    tables = {
        'share_models': table,
        'team_first_model': pd.DataFrame([{'team_games': len(sf), 'log_loss': ll_t.mean(), 'coin_flip': np.log(2),
                                           'delta_x1000': 1000 * d, 'se_x1000': 1000 * se,
                                           'z': d / se if se else np.nan,
                                           'home_first_rate': sf[sf.home == 1].y.mean()}]),
        'factorization_check': factorization_check(feat[feat.game_date >= start], teams).reset_index()
        .rename(columns={'index': 'term'}),
        'bench_rate': pd.DataFrame([{'bench_first_rate': wf.bench_rate.iloc[-1] if len(wf) else np.nan}]),
    }
    for k, v in coefs.items():
        tables['coef_{}'.format(k)] = v.reset_index().rename(columns={'index': 'term'})
    return tables


def _print(tables, out_dir=None, tag=''):
    for k, v in tables.items():
        if k == 'eval_frame':
            continue
        print('\n== {}'.format(k))
        print(v.to_string(index=False, float_format=lambda x: '{:.4f}'.format(x)) if len(v) else '(empty)')
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
        for k, v in tables.items():
            if k == 'eval_frame':
                v.to_parquet(os.path.join(out_dir, '{}eval_frame.parquet'.format(tag)))
            else:
                v.to_csv(os.path.join(out_dir, '{}{}.csv'.format(tag, k)), index=False)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='First basket edge test')
    ap.add_argument('--step', choices=['labels', 'market', 'edge', 'all'], default='all')
    ap.add_argument('--market', choices=['game', 'team'], default='game')
    ap.add_argument('--seasons', default='2024-25', help='comma list of odds seasons (market / edge steps)')
    ap.add_argument('--start', default='2023-10-01', help='labels step: first test date')
    ap.add_argument('--end', default='2025-04-15', help='labels step: last test date')
    ap.add_argument('--books', default=','.join(FG_GRADED), help='bet books (fanduel grades on first score incl FTs)')
    ap.add_argument('--ref-books', default=None, help='books in the consensus price (default: every complete listing)')
    ap.add_argument('--devig', choices=['power', 'shin', 'mult'], default='power')
    ap.add_argument('--build-alias', action='store_true', help='build player_alias in the extra store first')
    ap.add_argument('--out', default=None, help='write tables as CSV here')
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    seasons = [s for s in args.seasons.split(',') if s]
    if args.build_alias:
        from .NBAodds import oddsHistory
        oh = oddsHistory(odds_dir=EXTRA_DIR)
        for s in seasons:
            cov, unmatched, fuzzy = oh.build_player_alias(s, min_coverage=0)
            print('{} alias coverage {:.2%}; {} unmatched, {} fuzzy'.format(s, cov, len(unmatched), len(fuzzy)))
    conn = sqlite3.connect(DB_PATH)
    if args.step in ('labels', 'all'):
        _print(run_labels(conn, args.start, args.end), args.out, 'labels_')
    if args.step in ('market', 'edge', 'all'):
        ref = args.ref_books.split(',') if args.ref_books else None
        if args.step == 'market':
            res = run_market(conn, seasons, args.market, args.devig, args.books.split(','), ref)[0]
        else:
            res = run_edge(conn, seasons, args.market, args.devig, args.books.split(','), ref)
        _print(res, args.out, '{}_{}_'.format(args.market, '_'.join(seasons)))
