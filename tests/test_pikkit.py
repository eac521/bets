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


def test_aliases_and_non_players():
    assert pikkit.norm('Cam Johnson') == pikkit.norm('Cameron Johnson') == 'cameron johnson'
    assert pikkit.norm('Joshua Giddey') == 'josh giddey'
    rows = [('1', 'straight', 'SETTLED_CASH_OUT', 1.69, 110, 0, '5+ Any Player Threes Made BKN Nets @ MEM Grizzlies', None, 'NBA'),
            ('2', 'straight', 'SETTLED_CASH_OUT', 2.2, 47, 0, 'Georges Niang 2+ Threes Miami Heat @ Golden State Warriors', 'ModPlay', 'NBA'),
            ('3', 'straight', 'SETTLED_CASH_OUT', 2.0, 20, 6.5, 'Live Most Three Pointers A v B Three', 'H2h', 'NBA')]
    pk = pd.DataFrame(rows, columns=['bet_id', 'type', 'status', 'odds', 'amount', 'profit', 'bet_info', 'tags', 'leagues'])
    pk['sportsbook'] = 'Fanduel Sportsbook'
    pk['time_placed_iso'] = '2026-11-05T15:00:00.000Z'
    assert pikkit.classify(pk).set_index('bet_id')['kind'].to_dict() == {'1': 'other', '2': 'cashout', '3': 'h2h'}


def test_merge_repeats_pays_the_same():
    m = pd.DataFrame({'bet_id': ['a', 'b', 'c'], 'date': ['2026-11-04'] * 3, 'nkey': ['grayson allen'] * 2 + ['sam merrill'],
                      'over_under': ['Over', 'Over', 'Under'], 'number': [3.5, 3.5, 2.5], 'book': ['FanDuel'] * 3,
                      'odds': [4.1, 4.3, 1.75], 'amount': [34.5, 37.75, 14.5], 'profit': [106.95, 124.58, 10.88],
                      'status': ['SETTLED_WIN'] * 3, 'bet_info': ['x', 'y', 'z']})
    out = pikkit.merge_repeats(m).set_index('nkey')
    assert len(out) == 2 and out.loc['grayson allen', 'n_bets'] == 2 and out.loc['sam merrill', 'n_bets'] == 1
    g = out.loc['grayson allen']
    assert g.amount == pytest.approx(72.25) and g.profit == pytest.approx(231.53)
    assert g.amount * (g.odds - 1) == pytest.approx(34.5 * 3.1 + 37.75 * 3.3)  # combined row pays what the two bets pay
    assert g.bet_id == 'a' and out.loc['sam merrill', 'odds'] == pytest.approx(1.75)


def test_reconcile_combines_repeats_instead_of_overwriting():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE players (player_id TEXT, name TEXT)')
    conn.executemany('INSERT INTO players VALUES (?, ?)', [('1', 'Immanuel Quickley'), ('2', 'Cameron Johnson')])
    conn.execute('CREATE TABLE bankroll_adjustments (bet_id TEXT PRIMARY KEY, date TEXT, user TEXT, source TEXT, amount REAL, note TEXT)')
    conn.execute("""CREATE TABLE bets (player_id TEXT, date TEXT, market TEXT, over_under TEXT, number REAL, bet_book TEXT,
                 final_line REAL, bet_amount REAL, user TEXT, boost REAL DEFAULT 0, free_bet INTEGER DEFAULT 0,
                 PRIMARY KEY (player_id, date, over_under, number, bet_book, user))""")
    conn.executemany('INSERT INTO bets VALUES (?,?,?,?,?,?,?,?,?,?,?)', [
        ('1', '2026-11-01', 'threes', 'Under', 2.5, 'DraftKings', -115, 18.25, 'EAC', 0, 0),
        ('2', '2026-11-02', 'threes', 'Under', 1.5, 'FanDuel', 130, 30.75, 'EAC', 0, 0),
        ('2', '2026-11-03', 'threes', 'Under', 1.5, 'FanDuel', 130, 10.0, 'EAC', 0, 0)])  # placed, as Cam Johnson
    rows = [('a', '2026-11-01T19:38:00Z', 1.869565, 85.0, -85.0, 'Under 2.5 Immanuel Quickley Three Pointers Made O/U A @ B', 'Draftkings Sportsbook'),
            ('b', '2026-11-01T19:44:00Z', 1.869565, 18.25, -18.25, 'Under 2.5 Immanuel Quickley Three Pointers Made O/U A @ B', 'Draftkings Sportsbook'),
            ('c', '2026-11-02T19:00:00Z', 2.3, 30.75, -30.75, 'Cam Johnson Under 1.5 Cam Johnson - Made Threes A @ B', 'Fanduel Sportsbook'),
            ('d', '2026-11-03T19:00:00Z', 2.3, 10.0, -10.0, 'Cam Johnson Under 1.5 Cam Johnson - Made Threes A @ B', 'Fanduel Sportsbook'),
            ('e', '2026-11-04T19:00:00Z', 2.3, 10.0, -10.0, 'Cam Johnson Under 1.5 Cam Johnson - Made Threes A @ B', 'Fanduel Sportsbook')]
    pk = pd.DataFrame(rows, columns=['bet_id', 'time_placed_iso', 'odds', 'amount', 'profit', 'bet_info', 'sportsbook'])
    pk = pk.assign(type='straight', status='SETTLED_LOSS', tags='ModPlay', leagues='NBA')
    sql, notes = pikkit.reconcile(pk, conn)
    conn.executescript('BEGIN;' + chr(10).join(sql) + chr(10) + 'COMMIT;')
    got = conn.execute("SELECT player_id, date, bet_amount, final_line FROM bets ORDER BY date").fetchall()
    assert got[0] == ('1', '2026-11-01', 103.25, -115.0)          # $85 + $18.25, not overwritten by the second bet
    assert [g[:3] for g in got[1:]] == [('2', '2026-11-02', 30.75), ('2', '2026-11-03', 10.0), ('2', '2026-11-04', 10.0)]
    assert notes['delete'] == 0 and notes['merged'] == 1 and not notes['unmapped']


def test_reconcile_moves_date_and_drops_voids():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE players (player_id TEXT, name TEXT)')
    conn.executemany('INSERT INTO players VALUES (?, ?)', [('1', 'OG Anunoby'), ('2', 'Kon Knueppel'), ('3', 'Filler Guy')])
    conn.execute('CREATE TABLE bankroll_adjustments (bet_id TEXT PRIMARY KEY, date TEXT, user TEXT, source TEXT, amount REAL, note TEXT)')
    conn.execute("""CREATE TABLE bets (player_id TEXT, date TEXT, market TEXT, over_under TEXT, number REAL, bet_book TEXT,
                 final_line REAL, bet_amount REAL, user TEXT, boost REAL DEFAULT 0, free_bet INTEGER DEFAULT 0,
                 PRIMARY KEY (player_id, date, over_under, number, bet_book, user))""")
    conn.executemany('INSERT INTO bets VALUES (?,?,?,?,?,?,?,?,?,?,?)', [
        ('1', '2026-11-10', 'threes', 'Over', 2.5, 'DraftKings', 143, 49.5, 'EAC', 0, 0),   # placed next day at Fanatics
        ('2', '2026-11-12', 'threes', 'Over', 2.5, 'BetMGM', 130, 12.25, 'EAC', 0, 0),      # book voided it
        ('3', '2026-11-09', 'threes', 'Over', 1.5, 'FanDuel', 100, 5.0, 'EAC', 0, 0),
        ('3', '2026-11-13', 'threes', 'Over', 1.5, 'FanDuel', 100, 5.0, 'EAC', 0, 0)])
    rows = [('a', '2026-11-11T19:00:00Z', 2.65, 20.45, -20.45, 'SETTLED_LOSS', '3+ OG Anunoby - Three Pointers Made A at B', 'Fanatics'),
            ('b', '2026-11-12T19:00:00Z', 2.3, 12.25, 0.0, 'SETTLED_VOID', 'Over 2.5 Kon Knueppel - Three Pointers A at B', 'BetMGM'),
            ('c', '2026-11-09T19:00:00Z', 2.0, 5.0, 5.0, 'SETTLED_WIN', 'Filler Guy 2+ Threes A @ B', 'Fanduel Sportsbook'),
            ('d', '2026-11-13T19:00:00Z', 2.0, 5.0, 5.0, 'SETTLED_WIN', 'Filler Guy 2+ Threes A @ B', 'Fanduel Sportsbook')]
    pk = pd.DataFrame(rows, columns=['bet_id', 'time_placed_iso', 'odds', 'amount', 'profit', 'status', 'bet_info', 'sportsbook'])
    pk = pk.assign(type='straight', tags='ModPlay', leagues='NBA')
    sql, notes = pikkit.reconcile(pk, conn)
    conn.executescript('BEGIN;' + chr(10).join(sql) + chr(10) + 'COMMIT;')
    got = conn.execute("SELECT player_id, date, bet_book, final_line, bet_amount FROM bets WHERE player_id IN ('1', '2')").fetchall()
    assert got == [('1', '2026-11-11', 'Fanatics', 165.0, 20.45)]
    assert conn.execute("SELECT source, amount FROM bankroll_adjustments").fetchall() == [('void', 0.0)]


def test_reconcile_zeroes_boost_on_pikkit_price():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE players (player_id TEXT, name TEXT)')
    conn.execute("INSERT INTO players VALUES ('1', 'Victor Wembanyama')")
    conn.execute('CREATE TABLE bankroll_adjustments (bet_id TEXT PRIMARY KEY, date TEXT, user TEXT, source TEXT, amount REAL, note TEXT)')
    conn.execute("""CREATE TABLE bets (player_id TEXT, date TEXT, market TEXT, over_under TEXT, number REAL, bet_book TEXT,
                 final_line REAL, bet_amount REAL, user TEXT, boost REAL DEFAULT 0, free_bet INTEGER DEFAULT 0,
                 PRIMARY KEY (player_id, date, over_under, number, bet_book, user))""")
    conn.execute("INSERT INTO bets VALUES ('1', '2026-11-06', 'threes', 'Over', 3.5, 'FanDuel', 950, 10.0, 'EAC', 0.5, 0)")
    pk = pd.DataFrame([('a', '2026-11-06T19:00:00Z', 10.5, 10.0, 95.0, 'SETTLED_WIN', 'Victor Wembanyama 4+ Threes A @ B', 'Fanduel Sportsbook')],
                      columns=['bet_id', 'time_placed_iso', 'odds', 'amount', 'profit', 'status', 'bet_info', 'sportsbook'])
    sql, notes = pikkit.reconcile(pk.assign(type='straight', tags='ModPlay', leagues='NBA'), conn)
    conn.executescript('BEGIN;' + chr(10).join(sql) + chr(10) + 'COMMIT;')
    assert conn.execute('SELECT final_line, boost FROM bets').fetchall() == [(950.0, 0.0)]  # same price, boost no longer stacked


def test_book_grade_wins_over_box_score():
    import sqlite3
    conn = sqlite3.connect(':memory:')
    conn.execute('CREATE TABLE players (player_id TEXT, name TEXT)')
    conn.executemany('INSERT INTO players VALUES (?, ?)', [('1', 'Luka Doncic'), ('2', 'Jalen Brunson')])
    conn.execute('CREATE TABLE bankroll_adjustments (bet_id TEXT PRIMARY KEY, date TEXT, user TEXT, source TEXT, amount REAL, note TEXT)')
    conn.execute("CREATE TABLE bets (player_id TEXT, date TEXT, market TEXT, over_under TEXT, number REAL, bet_book TEXT, "
                 "final_line REAL, bet_amount REAL, user TEXT, boost REAL DEFAULT 0, free_bet INTEGER DEFAULT 0, "
                 "PRIMARY KEY (player_id, date, over_under, number, bet_book, user))")
    conn.execute('CREATE TABLE plyrLogs (player_id TEXT, game_id TEXT, game_date TEXT, min REAL, lc_fgm REAL, rc_fgm REAL, abv_fgm REAL)')
    conn.executemany('INSERT INTO plyrLogs VALUES (?,?,?,?,?,?,?)', [
        ('1', '0022600032', '2026-11-06', 38.2, 0, 0, 4),   # 4 zone threes + a made heave: the book says 5
        ('2', '0022600033', '2026-11-06', 36.0, 1, 0, 2)])  # 3: box score and book agree
    conn.executemany('INSERT INTO bets VALUES (?,?,?,?,?,?,?,?,?,?,?)', [
        ('1', '2026-11-06', 'threes', 'Under', 4.5, 'FanDuel', -106, 56.75, 'EAC', 0, 0),
        ('2', '2026-11-06', 'threes', 'Over', 2.5, 'FanDuel', 120, 20.0, 'EAC', 0, 0)])
    rows = [('a', '2026-11-06T22:10:00Z', 1.943396, 56.75, -56.75, 'SETTLED_LOSS', 'Luka Doncic Under 4.5 Luka Doncic - Made Threes A @ B'),
            ('b', '2026-11-06T22:11:00Z', 2.2, 20.0, 24.0, 'SETTLED_WIN', 'Jalen Brunson 3+ Threes A @ B'),
            ('c', '2026-11-07T22:11:00Z', 2.2, 20.0, 24.0, 'SETTLED_WIN', 'Jalen Brunson 3+ Threes A @ B')]  # no box score yet: skipped
    pk = pd.DataFrame(rows, columns=['bet_id', 'time_placed_iso', 'odds', 'amount', 'profit', 'status', 'bet_info'])
    pk = pk.assign(type='straight', tags='ModPlay', leagues='NBA', sportsbook='Fanduel Sportsbook')
    for _ in range(2):  # a rerun replaces the correction instead of adding a second one
        sql, notes = pikkit.reconcile(pk, conn)
        conn.executescript('BEGIN;' + chr(10).join(sql) + chr(10) + 'COMMIT;')
    adj = conn.execute('SELECT bet_id, source, amount FROM bankroll_adjustments').fetchall()
    win = 56.75 * (pikkit.decimal(pikkit.american(1.943396)) - 1)
    assert adj == [('a:grade', 'grade', round(-56.75 - win, 2))]  # undoes the box-score win and books the loss
    assert len(notes['grade']) == 1
