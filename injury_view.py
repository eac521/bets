'''
Bets tab injury report: the newest official NBA report for today's games, pulled through nba.injury_reports
(nbainjuries), so the app and the second layer read the same parsed files in nba/data/injury/{season}/reports/.
'''
import datetime as dt
import glob
import logging
import os

import pandas as pd
import streamlit as st

from nba import injury_reports as ir

logger = logging.getLogger(__name__)

NYS = 'NOT YET SUBMITTED'
ORDER = [NYS, 'Out', 'Doubtful', 'Questionable', 'Probable', 'Available']
TEAMS = {
    'Atlanta Hawks': 'ATL', 'Boston Celtics': 'BOS', 'Brooklyn Nets': 'BKN', 'Charlotte Hornets': 'CHA', 'Chicago Bulls': 'CHI',
    'Cleveland Cavaliers': 'CLE', 'Dallas Mavericks': 'DAL', 'Denver Nuggets': 'DEN', 'Detroit Pistons': 'DET',
    'Golden State Warriors': 'GSW', 'Houston Rockets': 'HOU', 'Indiana Pacers': 'IND', 'LA Clippers': 'LAC',
    'Los Angeles Clippers': 'LAC', 'Los Angeles Lakers': 'LAL', 'Memphis Grizzlies': 'MEM', 'Miami Heat': 'MIA',
    'Milwaukee Bucks': 'MIL', 'Minnesota Timberwolves': 'MIN', 'New Orleans Pelicans': 'NOP', 'New York Knicks': 'NYK',
    'Oklahoma City Thunder': 'OKC', 'Orlando Magic': 'ORL', 'Philadelphia 76ers': 'PHI', 'Phoenix Suns': 'PHX',
    'Portland Trail Blazers': 'POR', 'Sacramento Kings': 'SAC', 'San Antonio Spurs': 'SAS', 'Toronto Raptors': 'TOR',
    'Utah Jazz': 'UTA', 'Washington Wizards': 'WAS'}


def season_of(date):
    '''YYYY-MM-DD -> '2026-27' '''
    y, m = int(date[:4]), int(date[5:7])
    y = y if m >= 8 else y - 1
    return f'{y}-{str(y + 1)[2:]}'


def pull(date):
    '''Newest report published by now (saved by injury_reports if it isn't on disk yet). Output: (report_ts or None, state)'''
    return ir.resolve(season_of(date), dt.datetime.now(), {}, pause=(0, 0))


def latest_saved(date):
    '''Newest saved report from the game date, or an empty frame.'''
    files = sorted(glob.glob(os.path.join(ir.OUT_ROOT, season_of(date), 'reports', f'{date}_*.parquet')))
    return pd.read_parquet(files[-1]) if files else pd.DataFrame()


@st.cache_data(ttl=300)
def load(date):
    '''Newest report for the date; pulls one when none is saved yet. Output: (frame, error message or None)'''
    rep = latest_saved(date)
    if not rep.empty:
        return rep, None
    try:
        ts, state = pull(date)
    except Exception as e:  # nbainjuries / Java missing, nba.com unreachable
        logger.warning(f'Injury report pull failed: {e}')
        return rep, str(e)
    return latest_saved(date), None if state == 'ok' else f'no report pulled ({state})'


def refresh(date):
    '''Refresh Lines hook: pull the newest report. Output: message for the sidebar.'''
    try:
        ts, state = pull(date)
    except Exception as e:
        return f'Injury report not updated: {e}'
    finally:
        load.clear()
    return f'Injury report as of {ts:%I:%M %p}' if state == 'ok' else f'Injury report not updated ({state})'


def for_game(rep, teams, date):
    '''Rows for the given team abbreviations on the date: player names as "First Last", most serious status first,
    G League assignments and player-less parse rows (wrapped reasons) dropped.'''
    if rep.empty:
        return rep
    r = rep.assign(team=rep.team.map(lambda t: TEAMS.get(t, t)))
    r = r[r.team.isin(teams) & (r.game_date.isna() | (r.game_date == date))]
    r = r[r.unsubmitted | r.player_name.notna()]
    r = r[~r.reason.fillna('').astype(str).str.startswith('G League')]
    r = r.assign(player=r.player_name.map(lambda n: f"{n.split(', ', 1)[1]} {n.split(', ', 1)[0]}" if isinstance(n, str) and ', ' in n else n),
                 status=r.status.where(~r.unsubmitted, NYS))
    return r.assign(_o=r.status.map({s: i for i, s in enumerate(ORDER)})).sort_values(['team', '_o', 'player']).drop(columns='_o')


def render(teams, date):
    '''Expander with the report for the given teams.'''
    rep, err = load(date)
    as_of = f' (as of {pd.Timestamp(rep.report_ts.max()):%I:%M %p})' if not rep.empty else ''
    with st.expander(f'Injury report{as_of}'):
        if err:
            st.caption(f'Could not pull the latest report: {err}')
        if rep.empty:
            st.info('No injury report saved for today yet. Use Refresh Lines.')
            return
        g = for_game(rep, teams, date)
        waiting = g[g.status == NYS].team.tolist()
        if waiting:
            st.caption(f"Not yet submitted: {', '.join(waiting)}")
        g = g[g.status != NYS]
        if g.empty:
            st.caption('Nobody listed.')
        else:
            st.dataframe(g[['team', 'player', 'status', 'reason']], hide_index=True, width='content',
                         column_config={'team': 'Team', 'player': 'Player', 'status': 'Status', 'reason': 'Reason'})
