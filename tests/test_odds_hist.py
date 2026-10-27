import pandas as pd
from nba.NBAodds import (to_et, et_to_utc_iso, team_key, teams_in_text, snapshot_targets,
                         parse_snapshot, has_market, normalize_name, ODDS_COLUMNS)

SAMPLE = {
    'timestamp': '2026-10-22T02:10:00Z',
    'previous_timestamp': '2026-10-22T02:05:00Z',
    'next_timestamp': '2026-10-22T02:15:00Z',
    'data': {
        'id': 'abc123', 'sport_key': 'basketball_nba', 'commence_time': '2026-10-22T02:30:00Z',
        'home_team': 'Sacramento Kings', 'away_team': 'Los Angeles Clippers',
        'bookmakers': [
            {'key': 'fanduel', 'title': 'FanDuel', 'last_update': '2026-10-22T02:09:00Z',
             'markets': [
                 {'key': 'player_threes', 'last_update': '2026-10-22T02:09:00Z',
                  'outcomes': [{'name': 'Over', 'description': 'Keegan Murray', 'price': 124, 'point': 1.5},
                               {'name': 'Under', 'description': 'Keegan Murray', 'price': -160, 'point': 1.5}]},
                 {'key': 'player_threes_alternate', 'last_update': '2026-10-21T19:00:00Z',
                  'outcomes': [{'name': 'Over', 'description': 'Keegan Murray', 'price': 380, 'point': 2.5}]}]}]}}


def test_late_tip_uses_et_date():
    '''A 10:30pm ET tip is the next day in UTC; game_date must be the ET date'''
    assert to_et('2026-10-22T02:30:00Z') == pd.Timestamp('2026-10-21 22:30')
    df = parse_snapshot(SAMPLE, 'close')
    assert (df.game_date.astype(str) == '2026-10-21').all()


def test_et_utc_round_trip():
    assert et_to_utc_iso(pd.Timestamp('2026-10-21 22:10')) == '2026-10-22T02:10:00Z'
    assert et_to_utc_iso(pd.Timestamp('2026-01-15 10:00')) == '2026-01-15T15:00:00Z'


def test_parse_snapshot_long_format():
    df = parse_snapshot(SAMPLE, 'close')
    assert list(df.columns) == ODDS_COLUMNS
    assert len(df) == 3
    assert set(df.side) == {'Over', 'Under'}
    assert df.player_name.unique().tolist() == ['Keegan Murray']
    assert df.loc[df.market == 'player_threes_alternate', 'is_stale'].all()
    assert not df.loc[df.market == 'player_threes', 'is_stale'].any()


def test_empty_snapshot():
    assert parse_snapshot({'timestamp': '2026-10-22T02:10:00Z', 'data': {}}, 'open').empty
    assert parse_snapshot(None, 'open').empty
    assert has_market(SAMPLE, 'player_threes')
    assert not has_market(SAMPLE, 'player_points')


def test_snapshot_targets_skip_after_close():
    '''Early tip: midday (14:00) is after the 12:40 close, so it is skipped'''
    t = dict(snapshot_targets('2026-12-25', '2026-12-25 13:00'))
    assert t['close'] == pd.Timestamp('2026-12-25 12:40')
    assert t['morning'] == pd.Timestamp('2026-12-25 10:00')
    assert 'midday' not in t


def test_snapshot_targets_hourly():
    t = snapshot_targets('2026-10-21', '2026-10-21 19:00', hourly=True)
    hourly = [x for p, x in t if p == 'hourly']
    assert pd.Timestamp('2026-10-21 14:00') not in hourly
    assert max(hourly) <= pd.Timestamp('2026-10-21 17:40')


def test_team_keys_and_text_match():
    assert team_key('Portland Trail Blazers') == 'Blazers'
    assert team_key('LA Clippers') == team_key('Los Angeles Clippers')
    keys = {'Nets', 'Hornets', 'Jazz', '76ers'}
    assert teams_in_text('Brooklyn Nets @ Utah Jazz', keys) == {'Nets', 'Jazz'}
    assert teams_in_text('CHA Hornets @ GS Warriors', keys) == {'Hornets'}


def test_normalize_name():
    assert normalize_name('Nikola Jokić') == 'nikola jokic'
    assert normalize_name('Derrick Jones Jr.') == 'derrick jones'
    assert normalize_name("De'Aaron Fox") == 'deaaron fox'


def test_et_to_utc_dst_transitions():
    '''Open-search midpoints can land in the repeated (fall back) or skipped (spring forward) hour'''
    assert et_to_utc_iso(pd.Timestamp('2025-11-02 01:36:43')) == '2025-11-02T05:36:43Z'
    assert et_to_utc_iso(pd.Timestamp('2026-03-08 02:01:42')) == '2026-03-08T07:00:00Z'
