import numpy as np
import pandas as pd
from scipy.stats import poisson

from betting import market as mk


def test_poisson_mean_roundtrip():
    lam = mk.poisson_mean(2.5, .55)
    assert abs(poisson.sf(2, lam) - .55) < 1e-9


def test_two_way_devig_and_one_sided_fallback():
    snap = pd.DataFrame({
        'game_date': '2026-10-31', 'pull_sequence': 'close', 'snapshot_ts': pd.Timestamp('2026-10-31 19:00'),
        'book': 'fanduel', 'player_name': 'A B',
        'line': [2.5, 2.5, 3.5], 'side': ['Over', 'Under', 'Over'], 'price': [110, -130, 250]})
    fair = mk.fair_p_over(mk.two_way(snap)).set_index('line')
    io, iu = 1 / 2.1, 1 / (1 + 100 / 130)
    assert abs(fair.loc[2.5, 'p_over'] - io / (io + iu)) < 1e-9
    assert abs(fair.loc[3.5, 'p_over'] - (1 / 3.5) / (io + iu)) < 1e-9   # alt line: stripped with the main line's margin


def test_main_line_picks_closest_to_even():
    snap = pd.DataFrame({
        'game_date': '2026-10-31', 'pull_sequence': 'open', 'snapshot_ts': pd.Timestamp('2026-10-31 07:00'),
        'book': 'draftkings', 'player_name': 'A B',
        'line': [1.5, 1.5, 2.5, 2.5], 'side': ['Over', 'Under', 'Over', 'Under'], 'price': [-250, 190, -105, -115]})
    assert mk.main_line(mk.two_way(snap)).line.tolist() == [2.5]
