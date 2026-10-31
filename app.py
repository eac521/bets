"""
NBA Betting Dashboard
Entry point for the Streamlit app. Handles basicConfig, sidebar, and dashboard display.

Run with: streamlit run app.py
"""
import logging
import os
os.makedirs('logs', exist_ok=True)

import re
import datetime as dt
import pandas as pd
import streamlit as st



from nba import NBAetl, NBAdata
from nba.run import data_pull

etl = NBAetl.etl()
data = NBAdata.data()
from betting import funcs
from betting.constants import books as BOOK_META
import player_view
od = funcs.odds()


# Logging setup
logger = logging.getLogger(__name__)

# ── Config ──
USERS = ['EAC', 'larBear']
PRIMARY = ['FanDuel', 'DraftKings', 'BetMGM']  # best book is always one of these
SECONDARY = ['Fanatics']  # limited: shown when it beats the best primary price
BOOKS = PRIMARY + SECONDARY
PREFIX = {v['odds_col']: v['col_prefix'] for v in BOOK_META.values()}
START_BANKROLL = 1000
KEEP_SHARE = .8  # share of profits that goes back into the bankroll
SEASON_START = '2026-10-01'
MODEL_NAME = 'threes'


st.set_page_config(page_title='NBA Betting Dashboard', layout='wide')


# ── Sidebar ──
test_mode = st.sidebar.checkbox('Test Mode',value=False)
test_date = None
if test_mode:
    test_date = st.sidebar.date_input('Test Date', value=dt.date(2026, 3, 23))
run_date = str(test_date) if test_mode else dt.datetime.today().strftime('%Y-%m-%d')
st.sidebar.title('Settings')
active_user = st.sidebar.selectbox('User', USERS)
@st.cache_data(ttl=300)
def load_bankroll(user):
    return od.current_bankroll(etl.conn, user, start=START_BANKROLL, keep=KEEP_SHARE, since=SEASON_START)

br = load_bankroll(active_user)
bankroll = st.sidebar.number_input(
    'Current Bankroll ($)',
    value = float(br['bankroll']),
    format='%.2f'
)
st.sidebar.caption('Start ${:,.0f} · settled P&L ${:,.2f} · set aside ${:,.2f} · {} bets pending'.format(
    START_BANKROLL, br['pnl'], br['set_aside'], br['pending']))
od.budget = bankroll


# ── Override refresh for when data still hasnt loaded ──
if st.sidebar.button('Refresh Data (Override)'):
    with st.spinner('Running data update...'):
        data_pull()
        st.sidebar.success('Data update complete')
        logger.info('Manual data update refresh triggered by {}'.format(active_user))


# ── Tabs: Bets and Player. The player tab is drawn first, so a st.stop() in the bets tab can't blank it. ──
bets_tab, player_tab = st.tabs(['Bets', 'Player'])
with player_tab:
    player_view.render(run_date)

with bets_tab:
    # ── Dashboard ──
    st.title('Bets to Place')

    def get_preds(MODEL_NAME,date,pivot=False):
       df =  pd.read_sql('''SELECT name,team,p.player_id,number,model_prob FROM predictions p
            LEFT JOIN pgames on pgames.player_id = p.player_id and date = game_date WHERE date = '{}' 
            AND market = '{}' '''.format(date, MODEL_NAME), etl.conn)
       if pivot:
           wide = df.pivot_table(index=['name','team','player_id'],columns = 'number',values='model_prob').reset_index()
           return wide
       else:
           return df

    @st.cache_data(ttl=300)
    def create_todays_bets(MODEL_NAME,date=None,value=0,test=False,bankroll=1000):
        """
        This will go through the process of running the model
        """
        od.budget = bankroll
        date = date or dt.datetime.today().strftime('%Y-%m-%d')
        wide = get_preds(MODEL_NAME,date,pivot=True)
        preds = od.oddsTable(wide)
        if test:
            odf = pd.read_sql('''SELECT name, over_under,number,FanDuel,DraftKings,theScore_Bet   
            FROM lines l
            LEFT JOIN players USING (player_id)
            WHERE date = '{}' '''.format(date), etl.conn)
        else:
            odf = od.latest_odds(MODEL_NAME, date)
            if odf.empty:
                return pd.DataFrame()
        df = od.bet_table(preds, odf)
        if not test:
            etl.insert_data(df.assign(date=date, market=MODEL_NAME).reindex(columns=[c[1] for c in etl.conn.execute('pragma table_info(lines)')]), 'lines', sort=True)  # a book with no posted lines still gets its column
        amts = [col for col in df.columns if re.search('Amount$', col) != None]
        final = df[(df[amts]>value).any(axis=1)]
        return final


    @st.cache_data(ttl=300)
    def load_current_plays(user, date):
        """Load bets already placed today so they drop off the active list."""
        query = """
            SELECT CAST(player_id AS INTEGER) AS player_id, over_under
            FROM bets
            WHERE date = ?
            AND user = ?
            AND bet_amount is not Null
        """
        df = pd.read_sql(query, etl.conn, params=[date, user])
        return df



    # ── Line refresh: one manual snapshot of every game, then the table reads from it ──
    if not test_mode and st.sidebar.button('Refresh Lines'):
        with st.spinner('Pulling lines...'):
            snap = od.pull_snapshot('manual')
        create_todays_bets.clear()
        st.sidebar.success('Pulled {} games'.format(snap.event_id.nunique() if not snap.empty else 0))

    # ── Display predictions and bet recording form ──
    if test_mode:
        plays = create_todays_bets(MODEL_NAME,date=str(test_date), test=True,bankroll=bankroll)
    else:
        plays = create_todays_bets(MODEL_NAME,bankroll=bankroll)

    games = pd.read_sql("""
        SELECT GROUP_CONCAT(teamAbrv) as teams,
               MAX(CASE WHEN t.home = 0 THEN tms.teamAbrv END) || ' @ ' ||
               MAX(CASE WHEN t.home = 1 THEN tms.teamAbrv END) AS game_label
        FROM teamLog t
        LEFT JOIN teams tms USING (team_id)
        WHERE t.game_date = ?
        GROUP BY t.game_id
    """, etl.conn, params=[run_date])
    edited = None
    if not plays.empty and 'as_of' in plays.columns and plays.as_of.notna().any():
        st.caption('Prices as of {:%I:%M %p} (oldest game: {:%I:%M %p})'.format(plays.as_of.max(), plays.as_of.min()))
    if plays.empty:
        st.info('No predictions or no lines for today yet. Data may not have been refreshed, or use Refresh Lines.')
    else:
        # Filter out bets already placed by this user
        open_bets = load_current_plays(active_user, run_date)
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
                game_filter = st.selectbox('Game', ['All'] + games.game_label.tolist())
            filtered = predictions.copy()
            if game_filter != 'All':
                filtered = filtered[filtered['team'].isin(game_filter.split(' @ '))]
            with col_f2:
                team_filter = st.selectbox('Team', ['All'] + sorted(predictions['team'].unique().tolist()))
            prim = [b for b in PRIMARY if b in filtered.columns]
            sec = [b for b in SECONDARY if b in filtered.columns]
            best_price = filtered[prim].max(axis=1)
            filtered['best_book'] = filtered[prim].fillna(-100000).idxmax(axis=1).where(best_price.notna())
            beats = filtered[sec].gt(best_price.fillna(-100000), axis=0)
            filtered['beats'] = [', '.join(b for b, v in zip(sec, row) if v) for row in beats.values]
            best_book_filter = st.selectbox('Best Book', ['All'] + PRIMARY)
            odds_range = st.slider('Odds Range', min_value=-1000, max_value=1000, value=(-200, 200), step=50)
            if best_book_filter != 'All':
                filtered = filtered[filtered['best_book'] == best_book_filter]
            if team_filter != 'All':
                filtered = filtered[filtered['team'] == team_filter]

            book_cols = [b for b in BOOKS if b in filtered.columns]
            filtered = filtered[
                filtered[book_cols].apply(
                    lambda row: row.between(odds_range[0], odds_range[1]).any(), axis=1
                ).astype(bool)  # an empty result must stay a row mask, not a column list
            ]

            # Add columns for bet recording

            filtered.insert(0, 'bet', False)
            filtered['book'] = filtered['best_book'].fillna('None')
            best_amt = pd.Series(float('nan'), index=filtered.index)
            for b in prim:
                best_amt = best_amt.mask(filtered['best_book'] == b, filtered['{}Amount'.format(PREFIX[b])])
            filtered['wager'] =  (best_amt.clip(lower=0) / 0.25).round() * 0.25
            best_ev = pd.Series(float('nan'), index=filtered.index)
            for b in prim:
                best_ev = best_ev.mask(filtered['best_book'] == b, filtered['{}EV'.format(PREFIX[b])])
            filtered['ev'] = best_ev
            filtered['line'] = best_price
            filtered['price'] = float('nan')  # price actually taken; blank = the chosen book's line
            filtered['boost'] = 0.0  # profit boost %, e.g. 25 for a 25% boost
            filtered['free_bet'] = False

            # Display with inline editing
            ev_config = {col: st.column_config.NumberColumn(col, format='percent') for col in filtered.columns if col.endswith('EV')}
            ev_config.update({col: st.column_config.NumberColumn(col, format='dollar') for col in filtered.columns if col.endswith('Amount')})
            edited = st.data_editor(
                filtered,
                column_order=['bet', 'name', 'team', 'over_under', 'number', 'model_line', 'line', 'best_book', 'ev', 'beats',
                              'book', 'price', 'boost', 'free_bet', 'wager'],  # everything else stays in the frame, hidden
                disabled=['name', 'team', 'over_under', 'number', 'model_line', 'line', 'best_book', 'ev', 'beats'],
                column_config={
                    'bet': st.column_config.CheckboxColumn('Bet?'),
                    'over_under': st.column_config.TextColumn('O/U'),
                    'number': st.column_config.NumberColumn('Line', format='%.1f'),
                    'model_line': st.column_config.NumberColumn('Model', format='%d'),
                    'line': st.column_config.NumberColumn('Best', format='%d'),
                    'best_book': st.column_config.TextColumn('Best book'),
                    'ev': st.column_config.NumberColumn('EV', format='percent'),
                    'beats': st.column_config.TextColumn('Beats best', help='Secondary book with a better price than the best primary book'),
                    'book': st.column_config.SelectboxColumn('Book', options=BOOKS + ['None']),
                    'price': st.column_config.NumberColumn('Price taken', help="Blank = the chosen book's line", format='%d', step=1),
                    'boost': st.column_config.NumberColumn('Boost %', min_value=0, max_value=100, step=5, format='%d%%'),
                    'free_bet': st.column_config.CheckboxColumn('Free bet'),
                    'wager': st.column_config.NumberColumn('Wager ($)', min_value=0.0, step=.25, format='$%.2f'),
                    **ev_config,
                },
                width = 'content',
                hide_index=True,
            )
            with st.expander('All book prices'):
                odds_cols = [v['odds_col'] for v in BOOK_META.values() if v['odds_col'] in filtered.columns]
                st.dataframe(filtered[['name', 'over_under', 'number', 'model_line'] + odds_cols], hide_index=True, width='content')
    if st.button('Save Bets') and edited is not None:
        selected = edited[edited['bet'] == True]
        if selected.empty:
            st.warning('No bets selected.')
        elif (selected['book'] == 'None').any():
            st.error('Select a book for all checked bets.')
        elif selected.apply(lambda r: pd.isna(r['price']) and pd.isna(r.get(r['book'])), axis=1).any():
            st.error("Enter the price taken where the chosen book has no line.")
        else:
            bet_date = run_date
            save_df = pd.DataFrame({
                'player_id': selected['player_id'].values,
                'date': bet_date,
                'market': MODEL_NAME,
                'over_under': selected['over_under'].values,
                'number': selected['number'].values,
                'bet_book': selected['book'].values,
                'final_line': selected.apply(lambda r: r['price'] if pd.notna(r['price']) else r.get(r['book']), axis=1).values,
                'bet_amount': selected['wager'].values,
                'user': active_user,
                'boost': (selected['boost'].fillna(0) / 100).values,
                'free_bet': selected['free_bet'].fillna(False).astype(int).values,
            })
            etl.insert_data(save_df, 'bets')
            load_current_plays.clear()
            create_todays_bets.clear()
            st.rerun()
    ## special markets
    st.divider()
    st.header('Special Markets')
    game_filter = st.selectbox('Select Game', ['All'] + games.game_label.tolist())
    all_preds = get_preds(MODEL_NAME, run_date, pivot = True)
    if all_preds.empty:
        st.info('No predictions for {}.'.format(run_date))
        st.stop()
    special_market = all_preds[all_preds['team'].isin(game_filter.split(' @ '))]

    @st.cache_data
    def run_game_leaders(game_filter, special_market):
        df = special_market[special_market['team'].isin(game_filter.split(' @ '))]
        return od.game_leaders(df)

    col1, col2, col3, col4 = st.columns(4)
    pred_val = col1.number_input('Predicted Odds', value=100)
    market_val = col2.number_input('Market Odds', value=100,)

    if pred_val and market_val:
        ev = od.ev(pred_val, market_val)
        bet = od.kellyCrit(pred_val, market_val)
        col3.metric('EV', '{:.1%}'.format(ev))
        col4.metric('Bet Amount', '${:.2f}'.format(bet))


    if game_filter != 'All':
        gl, makeX = run_game_leaders(game_filter, special_market)
        col1, col2 = st.columns(2)
        col1.subheader('Game Leader')
        col1.dataframe(gl.style.format({'Win Rate': '{:.1%}'}))

        col2.subheader('Make X Threes')
        col2.dataframe(makeX.style.format({'Prob': '{:.1%}'}))

    st.subheader('H2H')

    col1, col2 = st.columns(2)
    fav = col1.selectbox('Favorite', special_market['name'].unique())
    udog = col2.selectbox('Underdog', special_market['name'].unique())
    spread = st.number_input('Spread', value=0.5,step=1.0)
    ovrLine = st.number_input('Over/Under Line', value=5.5,step=1.0)

    if st.button('Calculate H2H'):
        result = od.h2h(special_market, fav, udog, spread, ovrLine)
        st.dataframe(result)



