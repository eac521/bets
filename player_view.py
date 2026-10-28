"""
Player tab: pick a team, then a player from its roster. Recent threes trend (last 5 / 10 / 30 games),
the day's prediction vs the books, prediction-vs-actual history, and bet history for that player.
Rendered by app.py as its second tab: player_view.render(date).
"""
import datetime as dt
import numpy as np
import pandas as pd
import streamlit as st

from nba import NBAbase
from betting.funcs import odds
from nba_api.stats.static import teams as nba_teams

db = NBAbase.base()

BOOKS = ['FanDuel', 'DraftKings', 'theScore_Bet']
WINDOWS = [5, 10, 30]
EARLY_GAMES = 5          # players with 1-4 games this season are flagged (season-reset windows)
ROLE_SHIFT_MIN = 6       # minutes shift vs last season that flags a role change


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
                  threesMade AS fg3m, threesAtt AS fg3a
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


@st.cache_data(ttl=300)
def bet_history(player_id):
    q = '''SELECT b.date, b.over_under, b.number, b.bet_book, b.final_line, b.bet_amount, b.user,
                  g.threesMade AS fg3m, g.min
           FROM bets b
           LEFT JOIN pgames g ON g.player_id = b.player_id AND substr(g.game_date,1,10) = b.date
           WHERE b.player_id = ? ORDER BY b.date DESC'''
    return pd.read_sql(q, db.conn, params=[str(player_id)])


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


def render(date):
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

    # ── Trend table ──
    st.subheader('Recent threes')
    rows = [window_summary(log, n) for n in WINDOWS]
    rows.append(dict(window_summary(this_season, len(this_season)) or {}, window='This season') if len(this_season) else None)
    rows.append(dict(window_summary(last_season, len(last_season)) or {}, window='Last season') if len(last_season) else None)
    trend = pd.DataFrame([r for r in rows if r])
    st.dataframe(trend.style.format({'3PM': '{:.2f}', '3PA': '{:.2f}', '3P%': '{:.1%}', 'MIN': '{:.1f}', '3PA per 36': '{:.2f}'}),
                 hide_index=True, width='content')

    # ── Today's prediction ──
    st.subheader('Prediction for {}'.format(date))
    pt = predictions(pid, date, date)
    if pt.empty:
        st.info('No prediction for {} on {} (not eligible or no game).'.format(name, date))
    else:
        pmf = pt.groupby('number').model_prob.sum()
        pmf = pmf / pmf.sum()
        c1, c2 = st.columns([1, 2])
        c1.metric('Model mean threes', '{:.2f}'.format((pmf.index.values * pmf.values).sum()))
        c1.metric('Last 10 average', '{:.2f}'.format(log.head(10).fg3m.mean()))
        c2.bar_chart(pmf.rename('P(threes = k)'))
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

    # ── Prediction vs actual ──
    st.subheader('Prediction vs actual')
    start = log.head(30).game_date.min()
    ph = predictions(pid, start, date)
    if ph.empty:
        st.info('No stored predictions for these games (predictions are stored from 2026-03-23).')
    else:
        means = ph.assign(x=ph.number * ph.model_prob).groupby('date').x.sum().rename('model mean')
        hist = log.head(30).set_index('game_date')[['fg3m', 'fg3a']].rename(columns={'fg3m': '3PM', 'fg3a': '3PA'})
        hist = hist.join(means, how='left').sort_index()
        st.line_chart(hist[['3PM', 'model mean']])
        st.bar_chart(hist['3PA'])

    # ── Game log ──
    st.subheader('Game log (last 10)')
    gl = log.head(10).copy()
    gl['3P%'] = gl.fg3m / gl.fg3a.replace(0, np.nan)
    gl = gl.rename(columns={'fg3m': '3PM', 'fg3a': '3PA', 'min': 'MIN'})[['game_date', 'team', 'opponent', 'MIN', '3PM', '3PA', '3P%']]
    st.dataframe(gl.style.format({'MIN': '{:.1f}', '3P%': '{:.0%}'}, na_rep='-'), hide_index=True, width='content')

    # ── Bet history ──
    st.subheader('Bets on {}'.format(name))
    bh = bet_history(pid)
    if bh.empty:
        st.info('No bets logged on this player.')
    else:
        bh['d'] = bh.final_line.apply(lambda x: decimal(x) if pd.notna(x) and x != 0 else np.nan)
        won = np.where(bh.over_under == 'Over', bh.fg3m > bh.number, bh.fg3m < bh.number)
        bh['result'] = np.where(bh.fg3m.isna(), 'open/void', np.where(won, 'W', 'L'))
        bh['profit'] = np.where(bh.result == 'W', bh.bet_amount * (bh.d - 1), np.where(bh.result == 'L', -bh.bet_amount, 0.0))
        settled = bh[bh.result.isin(['W', 'L'])]
        c1, c2, c3 = st.columns(3)
        c1.metric('Record', '{}-{}'.format((settled.result == 'W').sum(), (settled.result == 'L').sum()))
        c2.metric('Profit', '${:,.2f}'.format(settled.profit.sum()))
        c3.metric('ROI', '{:.1%}'.format(settled.profit.sum() / settled.bet_amount.sum()) if len(settled) else '-')
        st.dataframe(bh[['date', 'over_under', 'number', 'bet_book', 'final_line', 'bet_amount', 'fg3m', 'result', 'profit', 'user']]
                     .style.format({'bet_amount': '${:.2f}', 'profit': '${:+.2f}', 'number': '{:.1f}'}, na_rep='-'),
                     hide_index=True, width='content')
