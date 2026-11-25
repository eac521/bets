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


def test_find_shots_allowed_gaps():
    e = make_etl()
    e.conn.execute('CREATE TABLE shotsAllowed (team_id TEXT, game_date TEXT, game_id TEXT, ra_fga REAL, abv_fga REAL, wide_fg3a REAL, open_fg3a REAL)')
    e.conn.executemany('INSERT INTO shotsAllowed VALUES (?,?,?,?,?,?,?)', [
        ('a', '2026-10-28', '0022600001', 30, 25, 18, 12),      # complete
        ('b', '2026-10-28', '0022600001', 28, 27, 15, 14),
        ('a', '2026-10-29', '0022600003', 31, 22, None, None),  # zones loaded, tracking missing
        ('b', '2026-10-30', '0022600004', 29, 30, 17, None),    # open counts missing
        ('c', '2026-10-31', '0022600005', 0, 0, None, None),    # not played yet: not a gap
        ('d', '2026-10-29', '0012600009', 30, 20, None, None),  # preseason: ignored
    ])
    assert e.find_shots_allowed_gaps('2026-10-28', '2026-10-31') == ['2026-10-29', '2026-10-30']
    assert e.find_shots_allowed_gaps('2026-10-28', '2026-10-28') == []


class FakeEndpoint:
    '''Stands in for nba_api endpoints: returns one frame shaped like the real response.'''
    def __init__(self, **kw):
        self.kw = kw

    def get_data_frames(self):
        import pandas as pd
        kind = self.kw.get('pt_measure_type') or 'openness'
        cols = {
            'Drives': ['PLAYER_ID', 'DRIVE_FGM', 'DRIVE_FGA', 'DRIVE_PASSES', 'DRIVE_AST', 'DRIVE_TOV', 'DRIVE_PF'],
            'PullUpShot': ['PLAYER_ID', 'PULL_UP_FGM', 'PULL_UP_FGA', 'PULL_UP_FG3M', 'PULL_UP_FG3A'],
            'CatchShoot': ['PLAYER_ID', 'CATCH_SHOOT_FGM', 'CATCH_SHOOT_FGA', 'CATCH_SHOOT_FG3M', 'CATCH_SHOOT_FG3A'],
            'Passing': ['PLAYER_ID', 'PASSES_MADE', 'PASSES_RECEIVED', 'FT_AST', 'SECONDARY_AST', 'POTENTIAL_AST', 'AST_PTS_CREATED', 'AST_ADJ'],
            'openness': ['PLAYER_ID', 'PLAYER_LAST_TEAM_ID', 'FG2M', 'FG2A', 'FG3M', 'FG3A'],
        }[kind]
        return [pd.DataFrame([[1] + [4] * (len(cols) - 1), [2] + [2] * (len(cols) - 1)], columns=cols)]


def test_tracking_insert_matches_table(monkeypatch):
    import os
    from nba import NBAetl as mod
    monkeypatch.setattr(mod, 'LeagueDashPtStats', FakeEndpoint)
    monkeypatch.setattr(mod, 'LeagueDashPlayerPtShot', FakeEndpoint)
    monkeypatch.setattr(mod.time, 'sleep', lambda s: None)
    e = make_etl()
    e.cur = e.conn.cursor()
    e.conn.execute(open(os.path.join(os.path.dirname(__file__), 'data', 'playerTracking_schema.sql')).read())
    out = e.get_tracking_data(['2026-10-31'])
    rows = e.conn.execute('SELECT player_id, game_date, drive_fga, pullup_fg2m, Wide_Open_FG3A, Very_Tight_FG2M FROM playerTracking ORDER BY 1').fetchall()
    assert len(out.columns) == 39
    assert rows == [('1', '2026-10-31', 4, 0, 4, 4), ('2', '2026-10-31', 2, 0, 2, 2)]
    assert e.find_tracking_gaps('2026-10-28', '2026-10-31') == ['2026-10-28', '2026-10-29']  # box scores but no tracking
