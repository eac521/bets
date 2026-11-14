import numpy as np
import pandas as pd
import pytest

from betting.funcs import odds


def test_combined_matches_enumeration():
    a, b = [.2, .3, .3, .2], [.5, .5]
    out = odds.combo_threes(a, b, [1, 2, 3, 4, 5])
    exact = [sum(pa * pb for i, pa in enumerate(a) for j, pb in enumerate(b) if i + j >= n) for n in [1, 2, 3, 4, 5]]
    assert np.allclose(out.Prob, exact)
    assert out.Makes.tolist() == ['1+', '2+', '3+', '4+', '5+']


def test_either_is_one_minus_both_under():
    a, b = [.2, .3, .3, .2], [.5, .5]
    out = odds.combo_threes(a, b, [1, 2, 3], either=True)
    assert np.allclose(out.Prob, [1 - .2 * .5, 1 - .5 * 1, 1 - .8 * 1])


def test_fair_odds_rounds():
    assert odds.fair_odds(.5) == -100 and odds.fair_odds(.6) == -150 and odds.fair_odds(.25) == 300 and odds.fair_odds(.4) == 150


def test_parlay_price_is_product_of_decimals():
    assert odds.parlay_price([100, 100]) == 300
    assert odds.parlay_price([-200, -200]) == 125
    assert odds.parlay_price([150, -110]) == 377


def legs(rows):
    return pd.DataFrame(rows, columns=['player_id', 'over_under', 'number', 'model_prob'])


def test_parlay_prob_different_players_multiply():
    assert odds.parlay_prob(legs([[1, 'Over', 2.5, .4], [2, 'Under', 1.5, .5]]), {}) == pytest.approx(.2)


def test_parlay_prob_same_player_uses_pmf():
    pmf = pd.Series([.1, .2, .3, .25, .15], index=[0.0, 1.0, 2.0, 3.0, 4.0])
    # over 1.5 and over 2.5 together = over 2.5; over 1.5 and under 3.5 = P(2 or 3)
    both_over = odds.parlay_prob(legs([[7, 'Over', 1.5, .7], [7, 'Over', 2.5, .4]]), {7: pmf})
    band = odds.parlay_prob(legs([[7, 'Over', 1.5, .7], [7, 'Under', 3.5, .85]]), {7: pmf})
    assert both_over == pytest.approx(.4) and band == pytest.approx(.55)
    mixed = odds.parlay_prob(legs([[7, 'Over', 1.5, .7], [7, 'Over', 2.5, .4], [8, 'Under', 0.5, .3]]), {7: pmf})
    assert mixed == pytest.approx(.12)


def test_save_parlay_writes_one_file_per_ticket(tmp_path, monkeypatch):
    monkeypatch.setenv('PARLAY_DIR', str(tmp_path))
    od = odds()
    lg = pd.DataFrame({'game_id': ['0022600001', '0022600002'], 'game_date': ['2026-11-14'] * 2, 'player_id': [1, 2],
                       'name': ['A', 'B'], 'over_under': ['Over', 'Under'], 'number': [2.5, 1.5],
                       'leg_price': [150.0, -110.0], 'model_prob': [.45, .6]})
    pid1, f1 = od.save_parlay(lg, 'FanDuel', 'EAC', 5, 377, .27, 6.25)
    pid2, f2 = od.save_parlay(lg, 'FanDuel', 'EAC', 5, 377, .27, 6.25)
    assert pid1 != pid2 and f1 != f2
    df = pd.read_parquet(f1)
    assert len(df) == 2 and df.parlay_id.nunique() == 1 and df.n_legs.eq(2).all()
    assert df.price_taken.eq(377).all() and df.fair_price.eq(odds.fair_odds(.27)).all()
    assert df.player_name.tolist() == ['A', 'B'] and df.leg_model_p.tolist() == [.45, .6]
