import numpy as np
import pandas as pd

from betting.funcs import odds


def wide(rows):
    '''rows: {name: [P(0), P(1), ...]} -> wide predictions frame like get_preds(pivot=True)'''
    n = max(len(v) for v in rows.values())
    df = pd.DataFrame([[k, 'T', str(i)] + v + [0.0] * (n - len(v)) for i, (k, v) in enumerate(rows.items())],
                      columns=['name', 'team', 'player_id'] + list(range(n)))
    return df


def test_clear_leader_wins_every_sim():
    gl, _ = odds.game_leaders(wide({'A': [0, 0, 0, 1], 'B': [0, 1, 0, 0]}), sims=2000, seed=1)
    assert gl['Win Rate'].to_dict() == {'A': 1.0}


def test_ties_are_dead_heats():
    gl, _ = odds.game_leaders(wide({'A': [0, 0, 1], 'B': [0, 0, 1], 'C': [1, 0, 0]}), sims=2000, seed=1)
    assert gl['Win Rate'].round(6).to_dict() == {'A': 0.5, 'B': 0.5}


def test_matches_exact_win_rate():
    # A: 0 or 2 threes (50/50); B: always 1. A wins outright half the time, never ties.
    gl, _ = odds.game_leaders(wide({'A': [.5, 0, .5], 'B': [0, 1, 0]}), sims=20000, seed=3)
    assert abs(gl.loc['A', 'Win Rate'] - .5) < .02 and abs(gl.loc['B', 'Win Rate'] - .5) < .02
