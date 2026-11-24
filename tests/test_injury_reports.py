from datetime import datetime, timedelta

import pandas as pd
import pytest

from nba import injury_reports as ir


def test_legacy_candidates_hourly_at_half_past():
    c = ir.report_candidates(datetime(2024, 1, 10, 14, 0))
    assert c[0] == datetime(2024, 1, 10, 13, 30)
    assert all(t.minute == 30 for t in c)
    assert all(b - a == timedelta(hours=-1) for a, b in zip(c, c[1:]))
    assert c[-1] >= datetime(2024, 1, 10, 6, 0)


def test_legacy_target_on_the_half_hour_uses_that_report():
    assert ir.report_candidates(datetime(2024, 1, 10, 17, 30))[0] == datetime(2024, 1, 10, 17, 30)


def test_new_format_every_15_minutes():
    c = ir.report_candidates(datetime(2026, 1, 5, 14, 7))
    assert c[:3] == [datetime(2026, 1, 5, 14, 0), datetime(2026, 1, 5, 13, 45), datetime(2026, 1, 5, 13, 30)]
    assert len(c) == 32                                   # 14:00 back to 06:15 (lookback ends 06:07)


def test_lookback_crosses_into_hourly_era():
    c = ir.report_candidates(datetime(2025, 12, 22, 10, 0))
    assert c[:5] == [datetime(2025, 12, 22, h, m) for h, m in [(10, 0), (9, 45), (9, 30), (9, 15), (9, 0)]]
    assert c[5] == datetime(2025, 12, 22, 8, 30)
    assert all(t.minute == 30 for t in c[5:])


def test_legacy_gap_skipped():
    c = ir.report_candidates(datetime(2025, 12, 19, 17, 0))
    assert datetime(2025, 12, 19, 16, 30) not in c
    assert c[0] == datetime(2025, 12, 19, 15, 30)


def test_candidates_never_after_target():
    for t in [datetime(2024, 3, 1, 5, 30), datetime(2026, 2, 1, 19, 52), datetime(2025, 12, 22, 9, 5)]:
        assert all(c <= t for c in ir.report_candidates(t))


def test_plan_targets():
    sched = pd.DataFrame({'game_date': ['2024-01-10', '2024-01-10'], 'game_id': ['0022300001', '0022300002'],
                          'tip_et': pd.to_datetime(['2024-01-10 19:00', '2024-01-10 22:00'])})
    p = ir.plan_targets(sched)
    assert len(p) == 6
    assert set(p.target) == {'0530', '0930', '1400', '1700', 'tip-90'}
    tip = p[p.target == 'tip-90'].set_index('game_id').target_ts
    assert tip['0022300001'] == datetime(2024, 1, 10, 17, 30)
    assert tip['0022300002'] == datetime(2024, 1, 10, 20, 30)


def raw_report():
    return pd.DataFrame({'Game Date': ['01/10/2024', '01/10/2024', '01/10/2024'],
                         'Game Time': ['07:00 (ET)'] * 3, 'Matchup': ['BOS@MIA'] * 3,
                         'Team': ['Boston Celtics', 'Boston Celtics', 'Miami Heat'],
                         'Player Name': ['Tatum, Jayson', 'White, Derrick', None],
                         'Current Status': ['Questionable', 'Out', None],
                         'Reason': ['Injury/Illness - Right Ankle; Sprain', 'Rest', 'NOT YET SUBMITTED']})


def test_clean_and_validate():
    out = ir.clean_report(raw_report(), datetime(2024, 1, 10, 13, 30))
    assert list(out.columns) == ['report_ts', 'game_date', 'game_time', 'matchup', 'team', 'player_name', 'status',
                                 'reason', 'unsubmitted']
    assert out.game_date.tolist() == ['2024-01-10'] * 3
    assert out.unsubmitted.tolist() == [False, False, True]
    v = ir.validate(out)
    assert v == {'n_rows': 3, 'n_players': 2, 'n_unsubmitted': 1, 'bad_status': 0, 'null_player': 0, 'null_game_date': 0}
    bad = out.copy()
    bad.loc[0, 'status'] = 'Injury/Illness'
    assert ir.validate(bad)['bad_status'] == 1


def test_resolve_steps_back_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(ir, 'OUT_ROOT', str(tmp_path))
    calls = []

    def fake_fetch(ts, tries=3):
        calls.append(ts)
        return None if ts == datetime(2024, 1, 10, 13, 30) else raw_report()

    monkeypatch.setattr(ir, 'fetch', fake_fetch)
    known = {}
    ts, result = ir.resolve('2023-24', datetime(2024, 1, 10, 14, 0), known, pause=(0, 0))
    assert (ts, result) == (datetime(2024, 1, 10, 12, 30), 'ok')
    assert known[datetime(2024, 1, 10, 13, 30)] == 'missing'
    assert (tmp_path / '2023-24' / 'reports' / '2024-01-10_1230.parquet').exists()
    ts2, _ = ir.resolve('2023-24', datetime(2024, 1, 10, 14, 15), known, pause=(0, 0))
    assert ts2 == ts and len(calls) == 2                  # cached: no new calls


def test_resolve_parse_failure_does_not_fall_back(tmp_path, monkeypatch):
    monkeypatch.setattr(ir, 'OUT_ROOT', str(tmp_path))

    def broken(ts, tries=3):
        raise ValueError('parse failed')

    monkeypatch.setattr(ir, 'fetch', broken)
    ts, result = ir.resolve('2023-24', datetime(2024, 1, 10, 14, 0), {}, pause=(0, 0))
    assert (ts, result) == (datetime(2024, 1, 10, 13, 30), 'error')


@pytest.mark.integration
def test_live_single_report():
    df = ir.fetch(datetime(2024, 1, 10, 13, 30))
    assert df is not None and len(df) > 0
