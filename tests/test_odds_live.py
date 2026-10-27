import json
import sqlite3
import datetime as dt
import pandas as pd
import pytest

from betting import funcs
from betting.constants import books, live_markets
from betting.funcs import ET
from betting import odds_live


@pytest.fixture
def od(tmp_path, monkeypatch):
    cfg = tmp_path / 'config.json'
    cfg.write_text(json.dumps({'oddsApi': {'free': 'FREE', 'paid': 'PAID'}}))
    monkeypatch.setenv('ODDS_LIVE_DIR', str(tmp_path / 'odds_live'))
    return funcs.odds(configPath=str(cfg))


def outcome(side, player, price, line):
    return {'name': side, 'description': player, 'price': price, 'point': line}


def game(eid, commence, books_markets):
    return {'id': eid, 'commence_time': commence, 'home_team': 'Boston Celtics', 'away_team': 'Philadelphia 76ers',
            'bookmakers': [{'key': bk, 'title': bk, 'markets': [{'key': m, 'last_update': '2026-10-24T13:00:00Z', 'outcomes': o}
                                                                 for m, o in mk.items()]}
                           for bk, mk in books_markets.items()]}


def snap(od, g, seq, ts):
    return od.parse_event(g, seq, pd.Timestamp(ts, tz=ET))


def test_book_and_market_limits():
    assert len(books) == 10
    assert len(live_markets) == 9


def test_slate_window_stops_before_tomorrows_games():
    start, end = funcs.odds.slate_window(dt.datetime(2026, 10, 24, 14, 0, tzinfo=ET))
    assert start == '2026-10-24T18:00:00Z'
    assert end == '2026-10-25T10:00:00Z'


def test_parse_event_long_rows(od):
    g = game('e1', '2026-10-24T23:30:00Z', {'draftkings': {
        'player_threes': [outcome('Over', 'Cam Johnson', -130, 2.5), outcome('Under', 'Cam Johnson', 100, 2.5)],
        'player_points_rebounds_assists': [outcome('Over', 'Cam Johnson', -115, 20.5)]}})
    df = snap(od, g, 'open', '2026-10-24 09:00')
    assert len(df) == 3
    assert df.game_date.iloc[0] == '2026-10-24'
    assert df.commence_time_et.iloc[0] == pd.Timestamp('2026-10-24 19:30')
    assert set(df.market) == {'player_threes', 'player_points_rebounds_assists'}
    assert str(df.price.dtype) == 'Int64'


def test_best_prices_takes_better_payout_never_mean(od):
    g = game('e1', '2026-10-24T23:30:00Z', {'draftkings': {
        'player_threes': [outcome('Over', 'Evan Mobley', -150, 0.5), outcome('Over', 'Cam Johnson', -110, 2.5)],
        'player_threes_alternate': [outcome('Over', 'Evan Mobley', -225, 0.5), outcome('Over', 'Cam Johnson', 110, 2.5)],
        'player_points': [outcome('Over', 'Cam Johnson', -110, 14.5)]}})
    odf = od.best_prices(snap(od, g, 'open', '2026-10-24 09:00'), ['player_threes', 'player_threes_alternate'])
    price = odf.set_index(['name', 'number']).DraftKings
    assert price[('Evan Mobley', 0.5)] == -150
    assert price[('Cam Johnson', 2.5)] == 110
    assert 14.5 not in odf.number.values


def test_best_prices_uses_each_games_latest_snapshot(od):
    early = game('e1', '2026-10-24T23:30:00Z', {'fanduel': {'player_threes': [
        outcome('Over', 'Cam Johnson', 120, 2.5), outcome('Over', 'Tyrese Maxey', 150, 3.5)]}})
    late = game('e1', '2026-10-24T23:30:00Z', {'fanduel': {'player_threes': [outcome('Over', 'Cam Johnson', 105, 2.5)]}})
    other = game('e2', '2026-10-25T02:00:00Z', {'espnbet': {'player_threes': [outcome('Over', 'Jalen Green', 140, 2.5)]}})
    rows = pd.concat([snap(od, early, 'open', '2026-10-24 09:00'), snap(od, other, 'open', '2026-10-24 09:00'),
                      snap(od, late, 'manual', '2026-10-24 13:00')])
    odf = od.best_prices(rows, ['player_threes'])
    assert odf.loc[odf.name == 'Cam Johnson', 'FanDuel'].item() == 105
    assert 'Tyrese Maxey' not in odf.name.values  # pulled by the book in the later snapshot
    assert odf.loc[odf.name == 'Jalen Green', 'theScore_Bet'].item() == 140  # game with no newer pull keeps its open
    assert odf.loc[odf.name == 'Jalen Green', 'as_of'].item() == pd.Timestamp('2026-10-24 09:00')


def fake_api(events, odds_by_id, calls):
    class R:
        def __init__(self, body):
            self.body, self.ok, self.status_code = body, True, 200
            self.headers = {'x-requests-remaining': '20000', 'x-requests-last': '6'}
            self.text = ''

        def json(self):
            return self.body

        def raise_for_status(self):
            pass

    def get(url):
        calls.append(url)
        if '/events?' in url:
            return R(events)
        eid = url.split('/events/')[1].split('/')[0]
        return R(odds_by_id[eid])
    return get


def test_pull_close_pulls_each_game_once(od, monkeypatch):
    now = pd.Timestamp.now(tz='UTC')
    iso = lambda m: (now + pd.Timedelta(minutes=m)).strftime('%Y-%m-%dT%H:%M:%SZ')
    events = [{'id': 'soon', 'commence_time': iso(20)}, {'id': 'later', 'commence_time': iso(120)},
              {'id': 'started', 'commence_time': iso(5)}]
    odds_by_id = {e['id']: game(e['id'], e['commence_time'], {'fanduel': {'player_threes': [
        outcome('Over', 'Cam Johnson', 120, 2.5)]}}) for e in events}
    calls = []
    monkeypatch.setattr(funcs.requests, 'get', fake_api(events, odds_by_id, calls))
    df = odds_live.pull_close(od)
    assert set(df.event_id) == {'soon'}
    assert set(od.read_snapshots(pull_sequence='close').event_id) == {'soon'}
    odds_calls = [c for c in calls if '/events/' in c]
    assert len(odds_calls) == 1 and 'player_points_rebounds_assists_alternate' in odds_calls[0]
    assert odds_live.pull_close(od).empty  # already pulled


def test_pull_open_skips_when_today_has_one(od, monkeypatch):
    events = [{'id': 'g1', 'commence_time': '2099-01-01T00:00:00Z'}]
    calls = []
    monkeypatch.setattr(funcs.requests, 'get', fake_api(events, {'g1': game('g1', '2099-01-01T00:00:00Z', {
        'fanduel': {'player_threes': [outcome('Over', 'Cam Johnson', 120, 2.5)]}})}, calls))
    odds_live.pull_open(od)
    n = len(calls)
    assert odds_live.pull_open(od) is None
    assert len(calls) == n


@pytest.fixture
def conn():
    c = sqlite3.connect(':memory:')
    c.execute('CREATE TABLE bets (player_id, date, market, over_under, number, bet_book, final_line, bet_amount, user)')
    c.execute('CREATE TABLE plyrLogs (player_id TEXT, game_date TEXT, min FLOAT, lc_fgm INT, rc_fgm INT, abv_fgm INT)')
    c.executemany('INSERT INTO plyrLogs VALUES (?,?,?,?,?,?)', [
        ('1', '2026-10-21', 30, 1, 0, 2),   # 3 threes
        ('2', '2026-10-21', 28, 0, 0, 1),   # 1 three
        ('3', '2026-10-21', 0, None, None, None),  # DNP
        ('1', '2026-10-22', 31, 0, 0, 0),   # 0 threes
    ])
    c.executemany('INSERT INTO bets VALUES (?,?,?,?,?,?,?,?,?)', [
        (1, '2026-10-21', 'threes', 'Over', 2.5, 'FanDuel', 150, 10, 'EAC'),    # win +15
        (2, '2026-10-21', 'threes', 'Over', 1.5, 'FanDuel', -120, 12, 'EAC'),   # loss -12
        (3, '2026-10-21', 'threes', 'Under', 1.5, 'FanDuel', -110, 11, 'EAC'),  # void
        (9, '2026-10-21', 'threes', 'Over', 0.5, 'FanDuel', 100, 5, 'larBear'),  # other user
        (1, '2026-10-22', 'threes', 'Over', 0.5, 'DraftKings', -200, 20, 'EAC'),  # loss -20
        (1, '2026-10-23', 'threes', 'Over', 1.5, 'DraftKings', 120, 10, 'EAC'),  # pending
    ])
    return c


def test_settle_bets(conn):
    df = funcs.odds.settle_bets(conn, 'EAC', '2026-10-01').sort_values(['date', 'player_id'])
    assert df.status.tolist() == ['win', 'loss', 'void', 'loss', 'pending']
    assert df.profit.round(2).tolist() == [15.0, -12.0, 0.0, -20.0, 0.0]


def test_bankroll_sets_aside_share_of_profit_high(od, conn):
    # day 1: +3 (peak) -> 0.6 set aside; day 2: -20 -> cum -17
    br = od.current_bankroll(conn, 'EAC', start=1000, keep=.8, since='2026-10-01')
    assert br == {'bankroll': 982.4, 'pnl': -17.0, 'set_aside': 0.6, 'pending': 1}


def test_bankroll_losses_count_in_full_below_start(od, conn):
    conn.execute("DELETE FROM bets WHERE date = '2026-10-21'")
    br = od.current_bankroll(conn, 'EAC', start=1000, keep=.8, since='2026-10-01')
    assert br['bankroll'] == 980.0 and br['set_aside'] == 0.0
