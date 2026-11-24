import datetime as dt

import pandas as pd

import injury_view as iv
from nba import injury_reports as ir


def raw():
    '''A parsed report the way nbainjuries returns it (team carried down, wrapped reason as a player-less row).'''
    return pd.DataFrame({
        'Game Date': ['11/24/2026'] * 6,
        'Game Time': ['07:00 (ET)'] * 6,
        'Matchup': ['DET@ORL'] * 4 + ['BOS@WAS'] * 2,
        'Team': ['Detroit Pistons', 'Detroit Pistons', 'Detroit Pistons', 'Orlando Magic', 'Boston Celtics', 'Washington Wizards'],
        'Player Name': ['Cunningham, Cade', 'Holland II, Ausar', None, None, 'Tatum, Jayson', 'Powell, Drake'],
        'Current Status': ['Questionable', 'Out', None, None, 'Out', 'Out'],
        'Reason': ['Injury/Illness - Left Ankle; Sprain', 'Injury/Illness - Right Knee; Posterior', 'Capsule Sprain',
                   'NOT YET SUBMITTED', 'Injury/Illness - Right Achilles; Repair', 'G League - On Assignment']})


def test_season_of():
    assert iv.season_of('2026-11-24') == '2026-27' and iv.season_of('2027-03-01') == '2026-27'


def test_for_game_filters_and_orders():
    rep = ir.clean_report(raw(), dt.datetime(2026, 11, 24, 17, 0))
    g = iv.for_game(rep, ['DET', 'ORL'], '2026-11-24')
    assert g.player.tolist() == ['Ausar Holland II', 'Cade Cunningham', None]
    assert g.status.tolist() == ['Out', 'Questionable', iv.NYS]
    assert g.team.tolist() == ['DET', 'DET', 'ORL']
    assert iv.for_game(rep, ['BOS', 'WAS'], '2026-11-24').player.tolist() == ['Jayson Tatum']  # G League dropped


def test_latest_saved_reads_newest_of_the_day(tmp_path, monkeypatch):
    monkeypatch.setattr(ir, 'OUT_ROOT', str(tmp_path))
    d = tmp_path / '2026-27' / 'reports'
    d.mkdir(parents=True)
    for hhmm in ('0930', '1700'):
        ir.clean_report(raw(), dt.datetime(2026, 11, 24, int(hhmm[:2]), int(hhmm[2:]))).to_parquet(d / f'2026-11-24_{hhmm}.parquet')
    ir.clean_report(raw(), dt.datetime(2026, 11, 25, 5, 30)).to_parquet(d / '2026-11-25_0530.parquet')
    assert pd.Timestamp(iv.latest_saved('2026-11-24').report_ts.max()) == pd.Timestamp('2026-11-24 17:00')
    assert iv.latest_saved('2026-11-23').empty
