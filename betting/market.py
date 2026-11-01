'''
Book prices to probabilities: implied and de-vigged probabilities, the Poisson mean a threes line implies,
fair closing probabilities for CLV, and name matching between the books and the players table.
Used by the Player and Tracking tabs.
'''
import re

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import poisson

from betting.constants import books

BOOK_COL = {k: v['odds_col'] for k, v in books.items()}   # API key -> app column name (fanduel -> FanDuel)
PRIMARY = ['fanduel', 'draftkings', 'betmgm']


def decimal(american):
    return np.where(american > 0, 1 + american / 100, 1 + 100 / np.abs(american))


def implied(american):
    return 1 / decimal(american)


def norm_name(name):
    s = re.sub(r'[^a-z ]', '', str(name).lower())
    return re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', s).strip()


def poisson_mean(line, p_over):
    '''Mean threes a line implies: the Poisson mean whose P(X > line) equals the de-vigged over probability.'''
    if not 0 < p_over < 1 or pd.isna(line):
        return np.nan
    k = int(np.floor(line))
    return brentq(lambda lam: poisson.sf(k, lam) - p_over, 1e-6, 40)


def two_way(snap):
    '''Over and under prices side by side per snapshot, book, player and line; the better price where main and
    alternate both list a line. Adds the book's margin and the de-vigged P(over) where both sides exist.'''
    if snap.empty:
        return pd.DataFrame(columns=['game_date', 'pull_sequence', 'snapshot_ts', 'book', 'player_name', 'line',
                                     'over', 'under', 'margin', 'p_over'])
    keys = ['game_date', 'pull_sequence', 'snapshot_ts', 'book', 'player_name', 'line']
    w = snap.pivot_table(index=keys, columns='side', values='price', aggfunc='max').reset_index()
    w.columns.name = None
    w = w.rename(columns={'Over': 'over', 'Under': 'under'})
    for c in ['over', 'under']:
        if c not in w:
            w[c] = np.nan
        w[c] = w[c].astype(float)
    io, iu = implied(w.over), implied(w.under)
    w['margin'] = io + iu - 1
    w['p_over'] = io / (io + iu)
    return w


def main_line(tw):
    '''The main line per snapshot, book and player: the two-way line closest to even money.'''
    t = tw.dropna(subset=['p_over']).copy()
    t['dist'] = (t.p_over - .5).abs()
    keys = ['game_date', 'pull_sequence', 'snapshot_ts', 'book', 'player_name']
    return t.sort_values('dist').drop_duplicates(keys).drop(columns='dist')


def fair_p_over(tw):
    '''De-vigged P(over) at every line a book lists. One-sided (alternate) lines use that book's margin on the
    player's main line that day (or the book's median margin) to strip the vig.'''
    t = tw.copy()
    m = main_line(tw).groupby(['game_date', 'book', 'player_name']).margin.median().rename('main_margin')
    t = t.merge(m, on=['game_date', 'book', 'player_name'], how='left')
    t['main_margin'] = t.main_margin.fillna(t.groupby('book').margin.transform('median')).fillna(.05)
    one_sided = t.p_over.isna() & t.over.notna()
    t.loc[one_sided, 'p_over'] = (implied(t.loc[one_sided, 'over']) / (1 + t.loc[one_sided, 'main_margin'])).clip(0, 1)
    under_only = t.p_over.isna() & t.under.notna()
    t.loc[under_only, 'p_over'] = 1 - (implied(t.loc[under_only, 'under']) / (1 + t.loc[under_only, 'main_margin'])).clip(0, 1)
    return t.drop(columns='main_margin')
