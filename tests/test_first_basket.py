import os
import sqlite3

import numpy as np
import pandas as pd
import pytest

from nba import first_basket as fb


# ---------------------------------------------------------------- unit

def test_devig_sums_to_one_and_power_hits_longshots():
    q = 1 / fb.american_to_decimal(np.array([150, 250, 400, 600, 900, 1500]))
    for m in ['mult', 'power', 'shin']:
        assert abs(fb.devig(q, m).sum() - 1) < 1e-9
    pw, ml = fb.devig(q, 'power'), fb.devig(q, 'mult')
    assert pw[0] > ml[0] and pw[-1] < ml[-1]


def test_softmax_within_groups():
    p = fb.softmax_within(np.array([0., 1., 2., 0., 0.]), np.array(['a', 'a', 'a', 'b', 'b']))
    assert np.isclose(p[:3].sum(), 1) and np.allclose(p[3:], .5)


def test_clogit_recovers_coefficients():
    rng = np.random.default_rng(1)
    G, k = 4000, 5
    X = rng.normal(size=(G * k, 2))
    grp = np.repeat(np.arange(G), k)
    p = fb.softmax_within(X @ np.array([1.0, -.5]), grp)
    y = np.zeros(G * k)
    for g in range(G):
        y[g * k + rng.choice(k, p=p[g * k:(g + 1) * k])] = 1
    cl = fb.Clogit().fit(X, y, grp, clusters=grp // 10)
    assert abs(cl.params.iloc[0] - 1) < 4 * cl.bse.iloc[0]
    assert abs(cl.params.iloc[1] + .5) < 4 * cl.bse.iloc[1]


def test_date_bootstrap_flat_roi():
    roi, lo, hi = fb.date_bootstrap(np.array([4., -1, -1, -1]), np.ones(4), np.array([1, 1, 2, 2]))
    assert roi == pytest.approx(.25) and lo <= roi <= hi


# ---------------------------------------------------------------- synthetic league

def _league(tmp, book_noise=.35, seed=0):
    '''10 teams, 3 seasons, starters with fixed shot shares; truth: team-first share = Q1 share, home 55% first'''
    rng = np.random.default_rng(seed)
    teams = [str(1610612700 + i) for i in range(10)]
    names = {t: 'Team {}'.format(chr(65 + i)) for i, t in enumerate(teams)}
    players = {t: [str(int(t) * 10 + j) for j in range(8)] for t in teams}
    share = {t: rng.dirichlet(np.full(5, 3)) for t in teams}
    rows, tl, q1, odds, ev = [], [], [], [], []
    gid = 0
    for yr in [2022, 2023, 2024]:
        for d in pd.date_range('{}-10-20'.format(yr), '{}-04-10'.format(yr + 1), freq='2D'):
            order = rng.permutation(teams)
            for h, a in zip(order[::2], order[1::2]):
                gid += 1
                game = '002{}{:05d}'.format(str(yr)[2:], gid)
                first_team = h if rng.random() < .55 else a
                truth = {}
                for t, opp, home in [(h, a, 1), (a, h, 0)]:
                    tl.append((game, d.strftime('%Y-%m-%d'), t, opp, home))
                    bench_first = rng.random() < .02
                    winner = players[t][5 + rng.integers(3)] if bench_first else players[t][rng.choice(5, p=share[t])]
                    for j, pid in enumerate(players[t]):
                        st = int(j < 5)
                        s = share[t][j] if st else .02
                        mins = 32 if st else 16
                        fga = rng.poisson(mins * .4 * 5 * s)
                        tf = int(pid == winner)
                        rows.append((pid, t, game, d.strftime('%Y-%m-%d'), mins, st, tf, int(tf and t == first_team),
                                     int(fga), int(rng.binomial(fga, .47))))
                        q1.append((pid, game, 8 if st else 2, int(rng.poisson(10 * s)), 0))
                        if st:
                            truth[pid] = .98 * s * (.55 if t == h else .45)
                if yr == 2024:
                    ev.append(('e{}'.format(gid), game, '2024-25', d.strftime('%Y-%m-%d'), names[h], names[a]))
                    pids = list(truth)
                    lp = np.log(np.array([truth[p] for p in pids]))
                    for book in ['draftkings', 'betmgm']:
                        z = lp + rng.normal(0, book_noise, len(lp))
                        q = np.exp(z * .9)
                        q = q / q.sum() * 1.30
                        for p_, qq in zip(pids, q):
                            price = int(round(100 * (1 / qq - 1)))
                            odds.append((d.date(), names[h], names[a], 'close', d + pd.Timedelta(hours=19), book,
                                         'player_first_basket', 'P' + p_, 'Yes', np.nan, price))
    db = os.path.join(tmp, 'nba.db')
    c = sqlite3.connect(db)
    c.execute('''create table plyrLogs (player_id TEXT, team_id TEXT, game_id TEXT, game_date TEXT, min FLOAT,
                 Starter INT, team_first INT, game_first INT, ra_fga INT, ra_fgm INT, usagePercentage FLOAT,
                 paint_fga INT, paint_fgm INT, mid_fga INT, mid_fgm INT, lc_fga INT, lc_fgm INT, rc_fga INT,
                 rc_fgm INT, abv_fga INT, abv_fgm INT)''')
    c.executemany('insert into plyrLogs (player_id, team_id, game_id, game_date, min, Starter, team_first, game_first,'
                  ' ra_fga, ra_fgm) values (?,?,?,?,?,?,?,?,?,?)', rows)
    c.execute('create table teamLog (game_id TEXT, game_date TEXT, team_id TEXT, opp_id TEXT, home INT)')
    c.executemany('insert into teamLog values (?,?,?,?,?)', tl)
    c.execute('create table plyrQ1Logs (player_id TEXT, game_id TEXT, min REAL, pts INT, ftm INT)')
    c.executemany('insert into plyrQ1Logs values (?,?,?,?,?)', q1)
    c.commit()
    odir = os.path.join(tmp, 'extra')
    os.makedirs(os.path.join(odir, '2024-25', 'close'))
    o = pd.DataFrame(odds, columns=['game_date', 'home_team', 'away_team', 'pull_sequence', 'snapshot_ts', 'book',
                                    'market', 'player_name', 'side', 'line', 'price'])
    o.to_parquet(os.path.join(odir, '2024-25', 'close', 'all_run1.parquet'))
    h = sqlite3.connect(os.path.join(odir, 'odds_hist.db'))
    h.execute('create table events (event_id TEXT, game_id TEXT, season TEXT, game_date TEXT, home_team TEXT, away_team TEXT)')
    h.executemany('insert into events values (?,?,?,?,?,?)', ev)
    h.execute('create table player_alias (player_name TEXT, player_id TEXT, source TEXT)')
    h.executemany('insert into player_alias values (?,?,?)',
                  [('P' + p, p, 'exact') for t in teams for p in players[t]])
    h.commit()
    return c, odir


@pytest.fixture(scope='module')
def league(tmp_path_factory):
    return _league(str(tmp_path_factory.mktemp('fb')))


def test_outcome_validity(league):
    out = fb.load_outcomes(league[0])
    assert out.valid_game.all() and out.valid_team.all()


def test_features_have_no_future_leak(league):
    out = fb.load_outcomes(league[0])
    f1 = fb.build_features(out)
    cut = pd.Timestamp('2024-01-01')
    out2 = out.copy()
    late = out2.game_date >= cut
    out2.loc[late, ['team_first', 'q1_fgpts', 'fga', 'fgm']] = 0
    f2 = fb.build_features(out2)
    cols = ['log_q1_share', 'log_fga_share', 'log_tf_share', 'fg_pct_c']
    a = f1[f1.game_date <= cut].sort_values(['game_id', 'player_id'])[cols].values
    b = f2[f2.game_date <= cut].sort_values(['game_id', 'player_id'])[cols].values
    assert np.allclose(a, b)


def test_labels_step_beats_uniform(league):
    t = fb.run_labels(league[0], '2024-01-01', '2024-04-10')
    sm = t['share_models'].set_index('model')
    assert sm.loc['full', 'skill_vs_uniform'] > .005
    assert t['team_first_model'].home_first_rate.iloc[0] > .5


def test_edge_detects_noisy_book(league):
    conn, odir = league
    t = fb.run_edge(conn, ['2024-25'], 'game', odds_dir=odir, core_dir=odir)
    w = t['blend_weights'].set_index('term')
    assert w.loc['w_model', 'z'] > 2          # the book was given noise the model doesn't have
    assert (t['book_summary'].overround_mean.round(2) == .30).all()
    assert not t['bets_shop'].empty
