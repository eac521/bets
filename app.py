"""
NBA Betting Dashboard
Entry point for the Streamlit app. Handles basicConfig, sidebar, and dashboard display.

Run with: streamlit run app.py
"""
import logging
import sqlite3
import re
import datetime as dt
import pandas as pd
import streamlit as st



from nba import NBAbase, NBAetl, NBAdata, NBAmodels
from nba.run import run_pipeline,data_pull,run_model

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
BOOKS = ['FanDuel', 'DraftKings', 'theScore_Bet']
MODEL_NAME = 'threes'


st.set_page_config(page_title='NBA Betting Dashboard', layout='wide')


# ── Sidebar ──
test_mode = st.sidebar.checkbox('Test Mode',value=True)
test_date = None
if test_mode:
    test_date = st.sidebar.date_input('Test Date', value=dt.date(2026, 3, 23))
st.sidebar.title('Settings')
active_user = st.sidebar.selectbox('User', USERS)
bankroll = st.sidebar.number_input(
    'Current Bankroll ($)',
    value = 1000.0,
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
def create_todays_bets(MODEL_NAME,date=None,value=0,test=False,bankroll=1000):
    """
    This will go through the process of running the model
    """
    od.budget = bankroll
    date = date or dt.datetime.today().strftime('%Y-%m-%d')
    preds = pd.read_sql('''SELECT name,team,over_under,number,model_line FROM predictions p
    LEFT JOIN pgames on pgames.player_id = p.player_id and date = game_date WHERE date = '{}' '''.format(date),etl.conn)
    if test:
        odf = pd.read_sql('''SELECT name, over_under,number,FanDuel,DraftKings,theScore_Bet   
        FROM lines l
        LEFT JOIN players USING (player_id)
        WHERE date = '{}' '''.format(date), etl.conn)
    else:
        odf = od.fetch_odds(MODEL_NAME)
    df = od.bet_table(preds, odf)
    if not test:
        etl.insert_data(df, 'lines', sort=True)
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
if test_mode:
    plays = create_todays_bets(MODEL_NAME,date=str(test_date), test=True,bankroll=bankroll)
else:
    plays = create_todays_bets(MODEL_NAME,bankroll=bankroll)

if plays.empty:
    st.info('No predictions available for today. Data may not have been refreshed yet.')
else:
    # Filter out bets already placed by this user
    open_bets = load_current_plays(active_user)
    if not open_bets.empty:
        placed_keys = set(zip(open_bets['player_id'], open_bets['over_under']))
        predictions = plays[
            ~plays.apply(
                lambda row: (row['player_id'], row['over_under']) in placed_keys,
                axis=1
            )
        ]
    else:
        predictions = plays

    if predictions.empty:
        st.success('All bets placed for today!')
    else:
        # Display table — hide player_id, show what matters
        display_cols =['name', 'over_under', 'number', 'prob'] + [b for b in BOOKS if b in predictions.columns]
        # Add filter controls
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            ou_filter = st.selectbox('Over/Under', ['All', 'Over', 'Under'])
        with col_f2:
            team_filter = st.selectbox('Team', ['All'] + sorted(predictions['team'].unique().tolist()))
        filtered = predictions.copy()
        filtered['best_book'] = filtered[[b for b in BOOKS if b in predictions.columns]].idxmax(axis=1)
        best_book_filter = st.selectbox('Best Book', ['All'] + BOOKS)
        odds_range = st.slider('Odds Range', min_value=-1000, max_value=1000, value=(-200, 200), step=50)
        if best_book_filter != 'All':
            filtered = filtered[filtered['best_book'] == best_book_filter]
        if ou_filter != 'All':
            filtered = filtered[filtered['over_under'] == ou_filter]
        if team_filter != 'All':
            filtered = filtered[filtered['team'] == team_filter]

        book_cols = [b for b in BOOKS if b in filtered.columns]
        filtered = filtered[
            filtered[book_cols].apply(
                lambda row: row.between(odds_range[0], odds_range[1]).any(), axis=1
            )
        ]

        # Add columns for bet recording

        filtered.insert(0, 'bet', False)
        filtered['book'] = 'None'
        amt_cols = [col for col in filtered.columns if col.endswith('Amount')]
        filtered['wager'] =  (filtered[amt_cols].max(axis=1) / 0.25).round() * 0.25

        # Display with inline editing
        ev_config = {col: st.column_config.NumberColumn(col, format='%.2f%%')
                     for col in filtered.columns if col.endswith('EV')}
        edited = st.data_editor(
            filtered,
            column_config={
                'bet': st.column_config.CheckboxColumn('Bet?'),
                'book': st.column_config.SelectboxColumn('Book', options=BOOKS),
                'wager': st.column_config.NumberColumn('Wager ($)', min_value=0.0, step=.25, format='$%.2f'),
                'player_id': None,  # hides the column
                **ev_config,
            },
            use_container_width=True,
            hide_index=True,
        )

