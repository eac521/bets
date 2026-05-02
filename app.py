"""
NBA Betting Dashboard
Entry point for the Streamlit app. Handles basicConfig, sidebar, and dashboard display.

Run with: streamlit run app.py
"""
import logging
import sqlite3

import pandas as pd
import streamlit as st

from nba import NBAbase, NBAetl, NBAdata, NBAmodels
from run import run_pipeline,data_pull,run_model

etl = NBAetl.etl()
data = NBAdata.data()
from betting import funcs
od = funcs.odds()


# ── Logging (runs once on app startup, picked up by all module-level loggers) ──
logging.basicConfig(
    filename='logs/nba_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ── Config ──

USERS = ['EAC', 'larBear']
BOOKS = ['FanDuel', 'DraftKings', 'ESPNBet']
MODEL_NAME = ['threes']

st.set_page_config(page_title='NBA Betting Dashboard', layout='wide')


# ── Sidebar ──
st.sidebar.title('Settings')
active_user = st.sidebar.selectbox('User', USERS)
bankroll = st.sidebar.number_input(
    'Current Bankroll ($)',
    min_value = 1000,
    format='%.2f'
)
od.budget = bankroll

# ── Override refresh for when data still hasnt loaded ──
if st.sidebar.button('Refresh Data (Override)'):
    with st.spinner('Running data update...'):
        data_pull()
        st.sidebar.success('Data update complete')
        logger.info('Manual data update refresh triggered by {}'.format(active_user))


# ── Dashboard ──
st.title('Bets to Place')


@st.cache_data(ttl=300)
def create_todays_bets(MODEL_NAME,date=None,value=0):
    """
    This will go through the process of running the model
    """
    date = date or dt.datetime.today().strftime('%Y-%m-%d')
    overs = pd.read_sql("SELECT * FROM predictions WHERE game_date = '{}' ".format(date),etl.conn)
    odf = od.fetch_odds(MODEL_NAME)
    df = od.bet_table(overs, odf, od.market_vars.get(MODEL_NAME).get('col_name'))
    etl.insert_data(df,'lines',sort=True)
    etl.insert_data(df,'predictions',sort=True)
    amts = [col for col in df.columns if re.search('Amount$', col) != None]
    final = df[(df[amts]>value).any(axis=1)]
    return final


@st.cache_data(ttl=300)
def load_current_plays(user):
    """Load bets already placed today so they drop off the active list."""
    query = """
        SELECT player_id, over_under
        FROM bets
        WHERE date = DATE('now')
        AND user = ?
        AND bet_amount is not Null
    """
    df = pd.read_sql(query, etl.conn, params=[user])
    return df




# ── Display predictions and bet recording form ──
plays = create_todays_bets(model_name)

if plays.empty:
    st.info('No predictions available for today. Data may not have been refreshed yet.')
else:
    # Filter out bets already placed by this user
    open_bets = load_current_plays(active_user)
    if not open_bets.empty:
        placed_keys = set(
            zip(open_bets['player_id'], open_bets['over_under'])
        )
        predictions = plays[
            ~plays.apply(
                lambda row: (row['player_id'], row['over_under']) in placed_keys,
                axis=1
            )
        ]

    if predictions.empty:
        st.success('All bets placed for today!')
    else:
        st.dataframe(
            predictions[['player_name', 'over_under', 'number', 'model_prob']],
            use_container_width=True,
            hide_index=True,
        )

        st.subheader('Record Bets')
        bets_to_confirm = []

        for idx, row in plays.iterrows():
            col1, col2, col3, col4 = st.columns([0.5, 2, 1.5, 1.5])
            with col1:
                selected = st.checkbox(
                    'Select',
                    key='chk_{}'.format(idx),
                    label_visibility='collapsed',
                )
            with col2:
                st.write('{} — {} {}'.format(
                    row.get('player_name', row.get('player_id')),
                    row['over_under'],
                    row['number'],
                ))
            with col3:
                book = st.selectbox(
                    'Book',
                    BOOKS,
                    key='book_{}'.format(idx),
                    label_visibility='collapsed',
                )
            with col4:
                wager = st.number_input(
                    'Wager ($)',
                    min_value=0.0,
                    step=5.0,
                    key='wager_{}'.format(idx),
                    label_visibility='collapsed',
                )

            if selected and wager > 0:
                bets_to_confirm.append({
                    'player_id': row['player_id'],
                    'over_under': row['over_under'],
                    'number': row['number'],
                    'model_prob': row.get('model_prob'),
                    'bet_book': book,
                    'final_line': row.get('final_line', row['number']),
                    'bet_amount': wager,
                    'user': active_user,
                })

        if bets_to_confirm:
            st.write('---')
            st.write('{} bet(s) selected — ${:.2f} total'.format(
                len(bets_to_confirm),
                sum(b['bet_amount'] for b in bets_to_confirm),
            ))
            if st.button('Confirm Bets'):
                etl.insert_data(pd.DataFrame(bets_to_confirm),'bets',sort=True)
                st.success('Recorded {} bet(s)!'.format(len(bets_to_confirm)))
                st.cache_data.clear()
                st.rerun()