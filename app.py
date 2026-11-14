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
import tracking_view
import parlay_view
import query_view
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
def load_bankroll(user, date):
    return od.current_bankroll(etl.conn, user, start=START_BANKROLL, keep=KEEP_SHARE, since=SEASON_START, today=date)

bankroll_box = st.sidebar.container()  # drawn after the refresh button runs, so a data pull shows up at once

# ── Override refresh for when data still hasnt loaded ──
if st.sidebar.button('Refresh Data (Override)'):
    with st.spinner('Running data update...'):
        data_pull()
        st.cache_data.clear()  # new box scores and predictions: every cached query is stale
        st.sidebar.success('Data update complete')
        logger.info('Manual data update refresh triggered by {}'.format(active_user))

br = load_bankroll(active_user, run_date)
bankroll = bankroll_box.number_input(
    'Current Bankroll ($)',
    value = float(br['bankroll']),
    format='%.2f'
)
od.budget = bankroll
bankroll_box.markdown('**Pending today**')
pend = br['pending_today']
if pend.empty:
    bankroll_box.caption('No bets pending today')
else:
    pend = pd.concat([pend, pd.DataFrame([{'book': 'Total', 'bets': pend.bets.sum(), 'amount': pend.amount.sum()}])], ignore_index=True)
    bankroll_box.dataframe(pend.rename(columns={'book': 'Book', 'bets': 'Bets', 'amount': 'Amount'}),
                         column_config={'Amount': st.column_config.NumberColumn(format='$%.2f')}, hide_index=True)


# ── Bets tab data: cached so a rerun reads the same table until the cache expires or Refresh Lines / Save clears it ──
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
        SELECT CAST(player_id AS INTEGER) AS player_id, over_under, number
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


@st.cache_data
def run_game_leaders(game_filter, special_market):
    df = special_market[special_market['team'].isin(game_filter.split(' @ '))]
    return od.game_leaders(df)


@st.fragment
def bets_section(plays, games):
    '''The bets table, filters and save form. A fragment: its widgets rerun only this function, not the other tabs.'''
    edited = None
    if not plays.empty and 'as_of' in plays.columns and plays.as_of.notna().any():
        st.caption('Prices as of {:%I:%M %p} (oldest game: {:%I:%M %p})'.format(plays.as_of.max(), plays.as_of.min()))
    if plays.empty:
        st.info('No predictions or no lines for today yet. Data may not have been refreshed, or use Refresh Lines.')
    else:
        # Filter out bets already placed by this user
        open_bets = load_current_plays(active_user, run_date)
        if not open_bets.empty:
            placed_keys = set(zip(open_bets['player_id'], open_bets['over_under'], open_bets['number']))  # a new line on a bet player still shows
            predictions = plays[
                ~plays.apply(
                    lambda row: (row['player_id'], row['over_under'], row['number']) in placed_keys,
                    axis=1
                )
            ]
            held = open_bets.assign(held=open_bets.over_under.str[0] + open_bets.number.astype(str)).groupby('player_id').held.agg(', '.join).reset_index()
            predictions = predictions.merge(held, how='left', on='player_id')  # lines already bet on this player, e.g. U0.5
        else:
            predictions = plays.assign(held=None)

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
            book_cols = [b for b in BOOKS if b in filtered.columns]
            best_price = filtered[prim].max(axis=1)
            best_book = filtered[prim].fillna(-100000).idxmax(axis=1).where(best_price.notna())
            book_filter = st.selectbox('Book', ['All'] + book_cols)
            odds_range = st.slider('Odds Range', min_value=-1000, max_value=2000, value=(-200, 1000), step=50)
            if team_filter != 'All':
                filtered = filtered[filtered['team'] == team_filter]

            # Each row is priced at one book: the chosen book, or the best primary book when no book is chosen
            filtered.insert(0, 'bet', False)
            filtered['line'] = best_price
            filtered['book'] = book_filter if book_filter != 'All' else best_book.fillna('None')
            book_price, book_amt, book_ev = (pd.Series(float('nan'), index=filtered.index) for _ in range(3))
            for b in book_cols:
                at_book = filtered['book'] == b
                book_price = book_price.mask(at_book, filtered[b])
                book_amt = book_amt.mask(at_book, filtered['{}Amount'.format(PREFIX[b])])
                book_ev = book_ev.mask(at_book, filtered['{}EV'.format(PREFIX[b])])
            filtered['book_price'] = book_price
            filtered['wager'] = (book_amt.clip(lower=0) / 0.25).round() * 0.25
            filtered['ev'] = book_ev
            filtered['price'] = float('nan')  # price actually taken; blank = the chosen book's line
            filtered['boost'] = 0.0  # profit boost %, e.g. 25 for a 25% boost
            filtered['free_bet'] = False
            filtered = filtered[filtered['book_price'].between(odds_range[0], odds_range[1])]  # the price the wager is sized on
            filtered = filtered[filtered['wager'] > 0]  # no price at this book or a $0 Kelly wager
            shown_price = ['book_price'] if book_filter != 'All' else []  # with a book chosen, its price sits next to the best price

            if filtered.empty:
                st.success('No open bets with a positive wager (filters applied).')
            else:
                # Inline editing inside a form: ticks and wagers rerun nothing until Save
                with st.form('bet_form', border=False):
                    ev_config = {col: st.column_config.NumberColumn(col, format='percent') for col in filtered.columns if col.endswith('EV')}
                    ev_config.update({col: st.column_config.NumberColumn(col, format='dollar') for col in filtered.columns if col.endswith('Amount')})
                    edited = st.data_editor(
                        filtered,
                        column_order=['bet', 'name', 'team', 'over_under', 'number', 'held', 'model_line'] + shown_price + ['line', 'wager', 'ev',
                                      'book', 'price', 'boost', 'free_bet'],  # everything else stays in the frame, hidden
                        disabled=['name', 'team', 'over_under', 'number', 'held', 'model_line', 'book_price', 'line', 'ev'],
                        column_config={
                            'bet': st.column_config.CheckboxColumn('Bet?'),
                            'over_under': st.column_config.TextColumn('O/U'),
                            'number': st.column_config.NumberColumn('Line', format='%.1f'),
                            'held': st.column_config.TextColumn('Held', help='Lines already bet on this player today'),
                            'model_line': st.column_config.NumberColumn('Model', format='%d'),
                            'book_price': st.column_config.NumberColumn(book_filter, format='%d', help='Price at the chosen book: wager and EV use it'),
                            'line': st.column_config.NumberColumn('Best', format='%d', help='Best price across FanDuel, DraftKings and BetMGM'),
                            'ev': st.column_config.NumberColumn('EV', format='percent'),
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
                    submitted = st.form_submit_button('Save Bets', type='primary')
                with st.expander('All book prices'):
                    odds_cols = [v['odds_col'] for v in BOOK_META.values() if v['odds_col'] in filtered.columns]
                    st.dataframe(filtered[['name', 'over_under', 'number', 'model_line'] + odds_cols], hide_index=True, width='content')
                if submitted:
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
                        load_bankroll.clear()
                        st.session_state['saved_msg'] = 'Saved {} bet{}'.format(len(save_df), '' if len(save_df) == 1 else 's')
                        st.rerun()


@st.fragment
def special_markets(games):
    '''Game leader, make-X and H2H calculators. A fragment, like bets_section.'''
    ## special markets
    st.divider()
    st.header('Special Markets')
    game_filter = st.selectbox('Select Game', ['All'] + games.game_label.tolist())
    all_preds = get_preds(MODEL_NAME, run_date, pivot = True)
    if all_preds.empty:
        st.info('No predictions for {}.'.format(run_date))
        return
    special_market = all_preds[all_preds['team'].isin(game_filter.split(' @ '))]

    col1, col2, col3, col4 = st.columns(4)
    pred_val = col1.number_input('Predicted Odds', value=100)
    market_val = col2.number_input('Market Odds', value=100,)

    if pred_val and market_val:
        ev = od.ev(pred_val, market_val)
        bet = od.kellyCrit(pred_val, market_val)
        col3.metric('EV', '{:.1%}'.format(ev))
        col4.metric('Bet Amount', '${:.2f}'.format(bet))


    if game_filter != 'All':
        excluded = st.multiselect('Exclude players', sorted(special_market['name'].unique()),
                                  help="Use for players who are out (or voided by the book). Leaving out a player who will play "
                                       "overstates everyone else's chances.")
        gl, makeX = run_game_leaders(game_filter, special_market[~special_market['name'].isin(excluded)])
        col1, col2 = st.columns(2)
        col1.subheader('Game Leader')
        col1.dataframe(gl.style.format({'Win Rate': '{:.1%}'}))

        col2.subheader('Make X Threes')
        col2.dataframe(makeX.style.format({'Prob': '{:.1%}'}))

        st.subheader('Combined / Either')
        nums = sorted(c for c in special_market.columns if isinstance(c, (int, float)))
        pool = special_market[~special_market['name'].isin(excluded)].copy()
        pmfs = pool[nums].fillna(0)
        pool['exp'] = (pmfs * nums).sum(axis=1) / pmfs.sum(axis=1)
        pool = pool.nlargest(8, 'exp').set_index('player_id')  # the 8 highest expected makes in the game
        if len(pool) < 2:
            st.info('Fewer than two predicted players in this game.')
        else:
            label = lambda pid: '{} ({}, {:.1f})'.format(pool.loc[pid, 'name'], pool.loc[pid, 'team'], pool.loc[pid, 'exp'])
            c1, c2, c3 = st.columns(3)
            pa = c1.selectbox('Player A', pool.index.tolist(), index=0, format_func=label, key='combo_a')
            pb = c2.selectbox('Player B', pool.index.tolist(), index=1, format_func=label, key='combo_b')
            mode = c3.radio('Market', ['Combined', 'Either'], horizontal=True, key='combo_mode')
            if pa == pb:
                st.warning('Pick two different players.')
            else:
                either = mode == 'Either'
                combo = od.combo_threes(pool.loc[pa, nums].fillna(0).values, pool.loc[pb, nums].fillna(0).values,
                                        range(3, 8) if either else range(5, 10), either=either)
                exp_total = pool.loc[pa, 'exp'] + pool.loc[pb, 'exp']
                if not either:
                    combo['vs expected'] = ['above' if int(m[:-1]) > exp_total else 'at or below' for m in combo.Makes]
                st.dataframe(combo.drop(columns='Prob').style.format({'Fair odds': '{:+d}'}), hide_index=True, width='content')
                if either:
                    st.caption('Either = at least one of the two makes that many. Fair odds with the two players independent. '
                               'Expected makes: {:.1f} and {:.1f}.'.format(pool.loc[pa, 'exp'], pool.loc[pb, 'exp']))
                else:
                    st.caption('Combined = the two players\' makes added together. Fair odds with the two players independent. '
                               'Expected combined makes: {:.1f}. The model\'s distributions run narrow, so prices above the expected total '
                               'come out too long (safe side) and prices at or below it too short (distrust those).'.format(exp_total))

    st.subheader('H2H')

    col1, col2 = st.columns(2)
    fav = col1.selectbox('Favorite', special_market['name'].unique())
    udog = col2.selectbox('Underdog', special_market['name'].unique())
    spread = st.number_input('Spread', value=0.5,step=1.0)
    ovrLine = st.number_input('Over/Under Line', value=5.5,step=1.0)

    if st.button('Calculate H2H'):
        result = od.h2h(special_market, fav, udog, spread, ovrLine)
        st.dataframe(result)






# ── Tabs: Bets, Parlays, Player, Tracking, Query. Each tab is a fragment: a widget in one tab reruns only that tab. ──
bets_tab, parlays_tab, player_tab, tracking_tab, query_tab = st.tabs(['Bets', 'Parlays', 'Player', 'Tracking', 'Query'])
with player_tab:
    player_view.render(run_date, active_user, SEASON_START)
with tracking_tab:
    tracking_view.render(active_user, START_BANKROLL, KEEP_SHARE, SEASON_START)
with query_tab:
    query_view.render()

with bets_tab:
    st.title('Bets to Place')
    if 'saved_msg' in st.session_state:
        st.toast(st.session_state.pop('saved_msg'))
    # ── Display predictions and bet recording form ──
    if test_mode:
        plays = create_todays_bets(MODEL_NAME,date=str(test_date), test=True,bankroll=bankroll)
    else:
        plays = create_todays_bets(MODEL_NAME,bankroll=bankroll)

    games = pd.read_sql("""
        SELECT t.game_id, GROUP_CONCAT(teamAbrv) as teams,
               MAX(CASE WHEN t.home = 0 THEN tms.teamAbrv END) || ' @ ' ||
               MAX(CASE WHEN t.home = 1 THEN tms.teamAbrv END) AS game_label
        FROM teamLog t
        LEFT JOIN teams tms USING (team_id)
        WHERE t.game_date = ?
        GROUP BY t.game_id
    """, etl.conn, params=[run_date])
    bets_section(plays, games)
    special_markets(games)

with parlays_tab:
    parlay_view.render(plays, games, get_preds(MODEL_NAME, run_date, pivot=True), od, active_user, run_date, BOOKS)
