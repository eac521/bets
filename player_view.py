"""
Player tab: pick a team, then a player from its roster. Recent threes (trend windows next to the game log),
the day's prediction vs the books, this season's bets on him, and each primary book's line movement over his
last 10 games (threes or points). Rendered by app.py: player_view.render(date, user, since).
"""
import datetime as dt
import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from nba import NBAbase
from betting import market as mk
from betting.constants import BOOK_COLORS, OTHER_BOOK_COLOR
from betting.funcs import odds
from nba_api.stats.static import teams as nba_teams
import tracking_view

db = NBAbase.base()
od = odds()
MARKETS = {'Threes': ('player_threes', 'fg3m'), 'Points': ('player_points', 'pts')}

BOOKS = ['FanDuel', 'DraftKings', 'theScore_Bet']
WINDOWS = [5, 10, 30]
EARLY_GAMES = 5          # players with 1-4 games this season are flagged (season-reset windows)
ROLE_SHIFT_MIN = 6       # minutes shift vs last season that flags a role change
BAR, EDGE, DOT = '#2a78d6', '#1b1b1b', '#eb6834'   # makes / bar outline / model dot


def season_of(date):
    d = pd.Timestamp(date)
    y = d.year if d.month >= 8 else d.year - 1
    return '{}-{}'.format(y, str(y + 1)[2:])


TEAM_NAMES = {str(t['id']): t['full_name'] for t in nba_teams.get_teams()}  # static list; the teams table has typos


@st.cache_data(ttl=300)
def teams_playing(date):
    q = "SELECT DISTINCT team_id FROM pgames WHERE substr(game_date,1,10) = ?"
    return set(pd.read_sql(q, db.conn, params=[date]).team_id.astype(str))


@st.cache_data(ttl=300)
def roster(team_id, date):
    '''Players on the team's roster on that date, most minutes this season first.
    Falls back to players who logged minutes for the team this season if the roster table has no entry.'''
    season = season_of(date)
    q = '''SELECT p.player_id, p.name, AVG(CASE WHEN g.min > 0 THEN g.min END) AS mpg
           FROM ({ids}) r JOIN players p USING(player_id)
           LEFT JOIN pgames g ON g.player_id = p.player_id AND g.season = ? AND substr(g.game_date,1,10) < ?
           GROUP BY p.player_id, p.name
           ORDER BY mpg IS NULL, mpg DESC, p.name'''
    on_roster = '''SELECT playerId AS player_id FROM rosters
                   WHERE teamId = ? AND seasonYear = ? AND startDate <= ? AND (endDate IS NULL OR endDate >= ?)'''
    df = pd.read_sql(q.format(ids=on_roster), db.conn, params=[team_id, season, date, date, season, date])
    if df.empty:
        played = "SELECT DISTINCT player_id FROM pgames WHERE team_id = ? AND season = ? AND substr(game_date,1,10) < ? AND min > 0"
        df = pd.read_sql(q.format(ids=played), db.conn, params=[team_id, season, date, season, date])
    return df


@st.cache_data(ttl=300)
def predicted_ids(date):
    q = "SELECT DISTINCT player_id FROM predictions WHERE date = ? AND market = 'threes'"
    return set(pd.read_sql(q, db.conn, params=[date]).player_id.astype(str))


@st.cache_data(ttl=300)
def game_log(player_id, date):
    q = '''SELECT substr(game_date,1,10) AS game_date, season, team, opponent, home, min,
                  threesMade AS fg3m, threesAtt AS fg3a, pts
           FROM pgames
           WHERE player_id = ? AND game_date < ? AND min > 0
           ORDER BY game_date DESC'''
    return pd.read_sql(q, db.conn, params=[str(player_id), date])


@st.cache_data(ttl=300)
def predictions(player_id, start, end):
    q = '''SELECT date, number, model_prob FROM predictions
           WHERE player_id = ? AND date BETWEEN ? AND ? AND market = 'threes' '''
    return pd.read_sql(q, db.conn, params=[int(player_id), start, end])


@st.cache_data(ttl=300)
def lines_today(player_id, date):
    q = '''SELECT over_under, number, FanDuel, DraftKings, theScore_Bet FROM lines
           WHERE player_id = ? AND date = ? ORDER BY number, over_under'''
    return pd.read_sql(q, db.conn, params=[str(player_id), date])


@st.cache_data(ttl=600)
def book_snapshots(dates, market):
    '''Primary-book snapshot rows for one market on the given dates (tuple, for caching).'''
    frames = []
    for d in dates:
        s = od.read_snapshots(d)
        if not s.empty:
            frames.append(s[(s.market == market) & s.book.isin(mk.PRIMARY)])
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def line_movement(name, dates, market, threes):
    '''First and last snapshot of each day per primary book: main line, de-vigged P(over) and (threes) implied mean.'''
    snap = book_snapshots(tuple(dates), market)
    if snap.empty:
        return pd.DataFrame()
    snap = snap[snap.player_name.map(mk.norm_name) == mk.norm_name(name)]
    ml = mk.main_line(mk.two_way(snap))
    if ml.empty:
        return pd.DataFrame()
    ml = ml.sort_values('snapshot_ts')
    first = ml.groupby(['game_date', 'book']).head(1).assign(when='open')
    last = ml.groupby(['game_date', 'book']).tail(1).assign(when='close')
    out = pd.concat([first, last])
    out['book'] = out.book.map(mk.BOOK_COL)
    out['implied_mean'] = [mk.poisson_mean(l, p) for l, p in zip(out.line, out.p_over)] if threes else np.nan
    return out[['game_date', 'book', 'when', 'line', 'over', 'under', 'p_over', 'implied_mean']]


def window_summary(log, n):
    w = log.head(n)
    if w.empty:
        return None
    return {'window': 'Last {}'.format(n) if n else 'Season', 'games': len(w),
            '3PM': w.fg3m.mean(), '3PA': w.fg3a.mean(),
            '3P%': w.fg3m.sum() / w.fg3a.sum() if w.fg3a.sum() else np.nan,
            'MIN': w['min'].mean(), '3PA per 36': w.fg3a.sum() / w['min'].sum() * 36}


def decimal(american):
    return 1 + american / 100 if american > 0 else 1 + 100 / abs(american)


def pmf_chart(pmf, mean, last10):
    '''Bars of P(threes = k), with the model mean and last-10 average written in the empty top-right corner.'''
    df = pd.DataFrame({'k': pmf.index.astype(int), 'p': pmf.values})
    bars = alt.Chart(df).mark_bar(color=BAR, cornerRadiusEnd=2).encode(
        x=alt.X('k:O', title='Threes made', axis=alt.Axis(labelAngle=0)),
        y=alt.Y('p:Q', title='P(threes = k)', axis=alt.Axis(format='.0%')),
        tooltip=[alt.Tooltip('k:O', title='threes'), alt.Tooltip('p:Q', format='.1%', title='probability')])
    corner = pd.DataFrame({'k': [df.k.max()]})
    notes = [alt.Chart(corner).mark_text(align='right', baseline='top', fontSize=13, dx=12).encode(
                 x='k:O', y=alt.value(y), text=alt.value(t))
             for y, t in [(4, 'Model mean {:.2f}'.format(mean)), (22, 'Last 10 avg {:.2f}'.format(last10))]]
    return alt.layer(bars, *notes).properties(height=260)


def shots_chart(games, preds):
    '''Last 10 games, oldest first: outlined bar = threes attempted, filled bar = made, dot = model mean threes that day.'''
    g = games[['game_date', 'opponent', 'fg3a', 'fg3m']].sort_values('game_date').copy()
    g['game'] = g.game_date.str[5:] + ' ' + g.opponent.astype(str)
    if not preds.empty:
        mean = (preds.assign(w=preds.number * preds.model_prob).groupby('date').agg(w=('w', 'sum'), p=('model_prob', 'sum')))
        g = g.merge((mean.w / mean.p).rename('pred').reset_index().rename(columns={'date': 'game_date'}), how='left', on='game_date')
    else:
        g['pred'] = np.nan
    x = alt.X('game:O', sort=g.game.tolist(), title=None, axis=alt.Axis(labelAngle=-35))
    tip = ['game', alt.Tooltip('fg3a:Q', title='3PA'), alt.Tooltip('fg3m:Q', title='3PM'), alt.Tooltip('pred:Q', format='.2f', title='model mean')]
    base = alt.Chart(g).encode(x=x, tooltip=tip)
    attempts = base.mark_bar(fill='white', stroke=EDGE, strokeWidth=1.2).encode(y=alt.Y('fg3a:Q', title='Threes'))
    made = base.mark_bar(fill=BAR, stroke=EDGE, strokeWidth=1.2).encode(y='fg3m:Q')
    dot = alt.Chart(g[g.pred.notna()]).mark_point(filled=True, size=90, color=DOT, opacity=1).encode(x=x, y='pred:Q', tooltip=tip)
    return alt.layer(attempts, made, dot).properties(height=260)


def bets_on_player(pid, name, user, since):
    '''This season's threes bets on one player: totals, a by-side summary with model-expected wins, and the bet list.'''
    st.subheader('Bets on {} this season'.format(name))
    b = tracking_view.settled_bets(user, since)
    b = b[b.player_id == str(pid)] if not b.empty else b
    if b.empty:
        st.info('No bets logged on this player since {}.'.format(since))
        return
    done = b[b.status.isin(['win', 'loss'])].assign(won=lambda d: (d.status == 'win').astype(int))
    c = st.columns(4)
    c[0].metric('Bets', len(b), '{} pending'.format(int((b.status == 'pending').sum())) if (b.status == 'pending').any() else None, delta_color='off')
    c[1].metric('Staked (settled)', '${:,.2f}'.format(done.bet_amount.sum()))
    c[2].metric('P&L', '${:+,.2f}'.format(done.profit.sum()))
    c[3].metric('ROI', '{:+.1%}'.format(done.profit.sum() / done.bet_amount.sum()) if done.bet_amount.sum() else '-')
    if not done.empty:
        side = done.groupby('over_under').agg(bets=('won', 'size'), wins=('won', 'sum'), expected=('p_model', 'sum'),
                                              staked=('bet_amount', 'sum'), profit=('profit', 'sum')).reset_index()
        side['roi'] = side.profit / side.staked
        st.dataframe(side.rename(columns={'over_under': 'side', 'expected': 'model-expected wins', 'profit': 'P&L'}).style.format(
            {'model-expected wins': '{:.1f}', 'staked': '${:,.2f}', 'P&L': '${:+,.2f}', 'roi': '{:+.1%}'}, na_rep='-'), hide_index=True, width='content')
    with st.expander('Every bet'):
        show = b.sort_values(['date', 'number'], ascending=[False, True])[
            ['date', 'over_under', 'number', 'bet_book', 'final_line', 'bet_amount', 'made', 'status', 'profit']]
        show.loc[show.status.isin(['pending', 'void']), 'made'] = np.nan
        st.dataframe(show.rename(columns={'over_under': 'side', 'number': 'line', 'bet_book': 'book', 'final_line': 'price',
                                          'bet_amount': 'stake', 'made': '3PM', 'profit': 'P&L'}).style.format(
            {'line': '{:.1f}', 'price': '{:+.0f}', 'stake': '${:.2f}', '3PM': '{:.0f}', 'P&L': '${:+.2f}'}, na_rep='-'), hide_index=True, width='content')
    st.caption('A handful of bets on one player, often several lines on the same night, is mostly luck: read wins against the '
               "model-expected wins, not as a verdict on the player.")


@st.fragment  # its widgets rerun only this tab
def render(date, user=None, since=None):
    '''Draws the player tab for a date (YYYY-MM-DD). Returns early instead of st.stop(), which would halt the whole app.'''
    playing = teams_playing(date)
    team_ids = sorted(TEAM_NAMES, key=lambda t: (t not in playing, TEAM_NAMES[t]))  # teams playing that day first
    c1, c2 = st.columns(2)
    team = c1.selectbox('Team', team_ids, format_func=lambda t: TEAM_NAMES[t] + ('  • plays {}'.format(date) if t in playing else ''), key='player_tab_team')
    players = roster(team, date)
    if players.empty:
        st.info('No roster found for the {} on {}.'.format(TEAM_NAMES[team], date))
        return
    today_ids = predicted_ids(date)
    players['label'] = players.name + np.where(players.player_id.astype(str).isin(today_ids), '  • predicted', '')
    pick = c2.selectbox('Player', players.label.tolist(), key='player_tab_player')
    row = players[players.label == pick].iloc[0]
    pid, name = str(row.player_id), row['name']

    log = game_log(pid, date)
    st.header(name)
    if log.empty:
        st.info('No games before {}.'.format(date))
        return

    # ── Flags: early-season window and role change ──
    season = season_of(date)
    this_season = log[log.season == season]
    last_season = log[log.season == season_of(pd.Timestamp(date) - pd.DateOffset(years=1))]
    flags = []
    if 0 < len(this_season) < EARLY_GAMES:
        flags.append('{} games this season: 6-game windows are still mostly game 1 (skip rule: 1-{} games).'.format(len(this_season), EARLY_GAMES - 1))
    if len(this_season) and len(last_season):
        shift = this_season.head(5)['min'].mean() - last_season['min'].mean()
        if abs(shift) > ROLE_SHIFT_MIN:
            flags.append('Role change: last-5 minutes {:+.1f} vs last season ({:.1f} vs {:.1f}).'.format(
                shift, this_season.head(5)['min'].mean(), last_season['min'].mean()))
    if len(this_season) and (this_season.team.nunique() > 1 or (len(last_season) and this_season.team.iloc[0] != last_season.team.iloc[0])):
        flags.append('Team change: now {} (last season {}).'.format(
            this_season.team.iloc[0], last_season.team.iloc[0] if len(last_season) else '-'))
    for f in flags:
        st.warning(f)

    # ── Recent threes (game log or rolling windows) next to today's distribution ──
    st.subheader('Recent threes')
    rows = [window_summary(log, n) for n in WINDOWS]
    rows.append(dict(window_summary(this_season, len(this_season)) or {}, window='This season') if len(this_season) else None)
    rows.append(dict(window_summary(last_season, len(last_season)) or {}, window='Last season') if len(last_season) else None)
    trend = pd.DataFrame([r for r in rows if r])
    gl = log.head(10).copy()
    gl['3P%'] = gl.fg3m / gl.fg3a.replace(0, np.nan)
    gl = gl.rename(columns={'fg3m': '3PM', 'fg3a': '3PA', 'min': 'MIN', 'game_date': 'date', 'opponent': 'opp'})
    pt = predictions(pid, date, date)
    c1, c2 = st.columns([6, 5])
    view = c1.radio('Recent threes view', ['Game log', 'Rolling windows'], horizontal=True, key='player_tab_recent', label_visibility='collapsed')
    if view == 'Game log':
        c1.dataframe(gl[['date', 'opp', 'MIN', '3PM', '3PA', '3P%']].style.format({'MIN': '{:.1f}', '3P%': '{:.0%}'}, na_rep='-'),
                     hide_index=True, width='content')
    else:
        c1.dataframe(trend.style.format({'3PM': '{:.2f}', '3PA': '{:.2f}', '3P%': '{:.1%}', 'MIN': '{:.1f}', '3PA per 36': '{:.2f}'}),
                     hide_index=True, width='content')
    if pt.empty:
        c2.info('No prediction for {} on {} (not eligible or no game).'.format(name, date))
    else:
        pmf = pt.groupby('number').model_prob.sum()
        pmf = pmf / pmf.sum()
        c2.altair_chart(pmf_chart(pmf, (pmf.index.values * pmf.values).sum(), log.head(10).fg3m.mean()), width='stretch')

        # ── Today's prediction: fair odds vs the books ──
        st.subheader('Prediction for {}'.format(date))
        ladder = []
        for n in np.arange(0.5, pmf.index.max(), 1.0):
            p_over = pmf[pmf.index > n].sum()
            ladder.append({'line': n, 'P(over)': p_over, 'fair over': odds.convertPercentToOdds(p_over),
                           'P(under)': 1 - p_over, 'fair under': odds.convertPercentToOdds(1 - p_over)})
        ladder = pd.DataFrame(ladder)
        ln = lines_today(pid, date)
        if not ln.empty:
            for side, pcol in [('Over', 'P(over)'), ('Under', 'P(under)')]:
                s = ln[ln.over_under == side].set_index('number')[[b for b in BOOKS if b in ln.columns]]
                best = s.max(axis=1)
                ladder['best {}'.format(side.lower())] = ladder.line.map(best)
                ladder['EV {}'.format(side.lower())] = [p * decimal(o) - 1 if pd.notna(o) and o != 0 else np.nan
                                                       for p, o in zip(ladder[pcol], ladder['best {}'.format(side.lower())])]
        ladder = ladder[(ladder['P(over)'] > 0.02) & (ladder['P(under)'] > 0.02)]
        fmt = {'P(over)': '{:.1%}', 'P(under)': '{:.1%}', 'EV over': '{:+.1%}', 'EV under': '{:+.1%}', 'line': '{:.1f}'}
        st.dataframe(ladder.style.format(fmt, na_rep='-'), hide_index=True, width='content')
        st.caption('EV uses the raw model probability against the best stored price; claimed edges run 2-4x the realized ones.')

    # ── Attempts, makes and the model's prediction, last 10 games ──
    st.subheader('Attempts, makes and prediction')
    last10 = log.head(10)
    hist = predictions(pid, last10.game_date.min(), last10.game_date.max())
    st.altair_chart(shots_chart(last10, hist), width='stretch')
    st.caption("Outlined bar = threes attempted, filled = made, dot = the model's mean threes for that game (the day's last run). "
               'Games with no stored prediction have no dot.')

    # ── This season's bets on him ──
    if user and since:
        bets_on_player(pid, name, user, since)

    # ── Line movement by book, last 10 games ──
    st.subheader('Line movement by book')
    mkt = st.radio('Market', list(MARKETS), horizontal=True, key='player_tab_market')
    market, stat = MARKETS[mkt]
    games = log.head(10)[['game_date', 'opponent', stat]].rename(columns={stat: 'actual'})
    dates = sorted(set(games.game_date) | {date})
    mv = line_movement(name, dates, market, mkt == 'Threes')
    if mv.empty:
        st.info('No {} lines stored for {} in these games (live snapshots start Oct 27, 2026).'.format(mkt.lower(), name))
        return
    mv['label'] = ['{:g} · {:.0%}'.format(l, p) for l, p in zip(mv.line, mv.p_over)]
    tbl = mv.pivot_table(index='game_date', columns=['book', 'when'], values='label', aggfunc='first')
    order = [(b, w) for b in ['FanDuel', 'DraftKings', 'BetMGM'] for w in ['open', 'close'] if (b, w) in tbl.columns]
    tbl = tbl[order]
    tbl.columns = ['{} {}'.format(b, w) for b, w in tbl.columns]
    tbl = games.set_index('game_date').join(tbl, how='right').sort_index(ascending=False)
    st.dataframe(tbl, width='content')
    st.caption('Main line · de-vigged P(over), at the first and last snapshot of each day. Today shows the latest prices.')
    close = mv[mv.when == 'close'].copy()
    y = 'implied_mean' if mkt == 'Threes' else 'line'
    ytitle = 'Implied mean threes (close)' if mkt == 'Threes' else 'Points line (close)'
    lines = alt.Chart(close).mark_line(point=True, strokeWidth=2).encode(
        x=alt.X('game_date:O', title=None), y=alt.Y('{}:Q'.format(y), title=ytitle),
        color=alt.Color('book:N', title='Book', scale=alt.Scale(domain=sorted(close.book.unique()),
                                                            range=[BOOK_COLORS.get(b, OTHER_BOOK_COLOR) for b in sorted(close.book.unique())])),
        tooltip=['game_date', 'book', alt.Tooltip('line:Q'), alt.Tooltip('p_over:Q', format='.0%', title='P(over)'),
                 alt.Tooltip('implied_mean:Q', format='.2f')])
    actual = alt.Chart(games[games.game_date.isin(close.game_date)]).mark_point(filled=True, size=70, color='#555').encode(
        x='game_date:O', y='actual:Q', tooltip=['game_date', 'opponent', 'actual'])
    st.altair_chart(lines + actual, width='stretch')
