import sqlite3

from nba.NBAetl import etl as NBAetl


def make_etl():
    e = NBAetl.__new__(NBAetl)  # skip __init__: no live DB
    e.conn = sqlite3.connect(':memory:')
    e.conn.execute('CREATE TABLE plyrLogs (player_id TEXT, game_id TEXT, game_date TEXT, min REAL)')
    e.conn.execute('CREATE TABLE teamLog (game_id TEXT, team_id TEXT, game_date TEXT)')
    e.conn.execute('CREATE TABLE plyrQ1Logs (player_id TEXT, game_id TEXT, game_date TEXT, min REAL)')
    e.conn.executemany('INSERT INTO plyrLogs VALUES (?,?,?,?)', [
        ('1', '0022600001', '2026-10-28', 30), ('2', '0022600001', '2026-10-28', 0),   # loaded
        ('3', '0022600002', '2026-10-28', None), ('4', '0022600002', '2026-10-28', None),  # scheduled, never loaded
        ('5', '0022600003', '2026-10-29', 25),                                           # loaded, no team log or Q1
        ('6', '0012600009', '2026-10-29', 20),                                           # preseason: ignored
    ])
    e.conn.execute("INSERT INTO teamLog VALUES ('0022600001', 't', '2026-10-28')")
    e.conn.execute("INSERT INTO plyrQ1Logs VALUES ('1', '0022600001', '2026-10-28', 8)")
    return e


def test_find_gaps_flags_each_missing_load():
    g = make_etl().find_gaps('2026-10-28', '2026-10-29').set_index('game_id')
    assert g.index.tolist() == ['0022600001', '0022600002', '0022600003']
    assert g.loc['0022600001', ['box', 'team', 'q1']].tolist() == [1, 1, 1]
    assert g.loc['0022600002', ['box', 'team', 'q1']].tolist() == [0, 0, 0]
    assert g.loc['0022600003', ['box', 'team', 'q1']].tolist() == [1, 0, 0]
