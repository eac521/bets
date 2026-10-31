import pandas as pd
import pytest

from betting import pikkit


@pytest.mark.parametrize('txt, expected', [
    ('Over 1.5 Jerami Grant Three Pointers Made O/U MEM Grizzlies @ SAC Kings', ('Jerami Grant', 'Over', 1.5)),
    ('Under 0.5 Justin Champagnie Total 3-Pointers Made Washington Wizards @ Charlotte Hornets', ('Justin Champagnie', 'Under', 0.5)),
    ('Under 2.5 James Harden - Three Pointers Minnesota Timberwolves @ X', ('James Harden', 'Under', 2.5)),
    ('Jalen Williams - Under 0.5 Threes Made O/U OKC Thunder @ X', ('Jalen Williams', 'Under', 0.5)),
    ('Jalen Johnson Under 1.5 Jalen Johnson - Made Threes Atlanta Hawks @ X', ('Jalen Johnson', 'Under', 1.5)),
    ('Tre Jones: 1+ Threes Made Threes Made CHI Bulls @ X', ('Tre Jones', 'Over', 0.5)),
    ('Yes Klay Thompson to make 3+ three-pointers Dallas Mavericks @ X', ('Klay Thompson', 'Over', 2.5)),
    ('4+ Brice Sensabaugh Three Pointers Made CHI Bulls @ X', ('Brice Sensabaugh', 'Over', 3.5)),
    ('Jrue Holiday 5+ Made Threes Los Angeles Lakers @ X', ('Jrue Holiday', 'Over', 4.5)),
    ('Jaylen Brown 3+ Threes New York Knicks @ Philadelphia 76ers', ('Jaylen Brown', 'Over', 2.5)),
    ('Toumani Camara Toumani Camara v Ty Jerome - Three Pointers Made Moneyline MEM @ POR', (None, None, None)),
    ('No Player Race to 5 Three Pointers MEM Grizzlies @ POR Trail Blazers', (None, None, None)),
    ('Donovan Mitchell 6+ Assists Washington Wizards @ Cleveland Cavaliers', (None, None, None)),
])
def test_parse_threes(txt, expected):
    assert pikkit.parse_threes(txt) == expected


def test_classify_kinds():
    rows = [  # bet_id, type, status, odds, amount, profit, bet_info, tags, leagues
        ('1', 'straight', 'SETTLED_WIN', 2.0, 10, 10, 'Over 1.5 Jerami Grant Three Pointers Made O/U A @ B', 'ModPlay', 'NBA'),
        ('2', 'straight', 'SETTLED_WIN', 10.75, 50, 487.5, 'Miles McBride Live Most Three Pointers NY Knicks @ BOS Celtics', 'H2h', 'NBA'),
        ('3', 'parlay', 'SETTLED_LOSS', 4.0, 25, -25, 'A Under 2.5 | B Under 2.5', 'ModPlay', 'NBA'),
        ('4', 'straight', 'SETTLED_LOSS', 2.24, 25, -25, 'Under 0.5 Live Anthony Davis Three Pointers Made O/U A @ B', 'ModPlay', 'NBA'),
        ('5', 'straight', 'SETTLED_WIN', 1.85, 40.5, 34.61, 'Under 1.5 Gui Santos Three Pointers Made O/U A @ B', None, 'NBA'),
        ('6', 'straight', 'SETTLED_WIN', 2.0, 100, 100, 'Green Bay Packers Moneyline', None, 'NFL'),
    ]
    pk = pd.DataFrame(rows, columns=['bet_id', 'type', 'status', 'odds', 'amount', 'profit', 'bet_info', 'tags', 'leagues'])
    pk['sportsbook'] = 'Draftkings Sportsbook'
    pk['time_placed_iso'] = '2026-10-24T18:00:00.000Z'
    out = pikkit.classify(pk).set_index('bet_id')['kind']
    assert out.to_dict() == {'1': 'model', '2': 'h2h', '3': 'parlay', '4': 'live', '5': 'model', '6': 'out_of_scope'}
