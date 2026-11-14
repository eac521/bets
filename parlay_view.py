"""
Parlays tab: pick a book, tick two or three of its +EV plays, and price every combination (each pair and the three-leg).
Enter the book's parlay price to get EV and the Kelly stake; Save records the ticket to parquet (nba/data/parlays).
Results and P&L still come from the Pikkit reconcile. Rendered by app.py as its second tab.
"""
from itertools import combinations

import pandas as pd
import streamlit as st

from nba import NBAbase
from betting.constants import books as BOOK_META

db = NBAbase.base()
PREFIX = {v['odds_col']: v['col_prefix'] for v in BOOK_META.values()}
MAX_LEGS = 3


@st.cache_data(ttl=60)
def held_today(user, date):
    '''Stakes already placed today as straight bets, per player, side and line.'''
    q = '''SELECT CAST(player_id AS INTEGER) AS player_id, over_under, number, SUM(bet_amount) AS held
           FROM bets WHERE date = ? AND user = ? AND bet_amount IS NOT NULL GROUP BY 1, 2, 3'''
    return pd.read_sql(q, db.conn, params=[date, user])


def leg_label(r):
    return '{} {}{:g} ({:+d})'.format(r['name'], r.over_under[0], r.number, int(r.leg_price))


@st.fragment  # its widgets rerun only this tab
def render(plays, games, preds, od, user, date, books):
    '''
    Inputs: today's plays (create_todays_bets), games (game_id, game_label), wide predictions (one column per number of threes),
            the app's odds object (its budget is the current bankroll), user, date (YYYY-MM-DD), bettable books
    '''
    st.title('Parlays')
    book_cols = [b for b in books if b in plays.columns] if not plays.empty else []
    if not book_cols:
        st.info('No predictions or no lines for today yet.')
        return
    book = st.selectbox('Book', book_cols, key='parlay_book')
    ev_col, amt_col = '{}EV'.format(PREFIX[book]), '{}Amount'.format(PREFIX[book])
    p = plays[plays[book].notna() & (plays[amt_col] > 0)].copy()  # this book's +EV plays only: books are never mixed
    if p.empty:
        st.info('No +EV plays at {} right now.'.format(book))
        return
    p = p.assign(leg_price=p[book], ev=p[ev_col], kelly=p[amt_col]).sort_values('ev', ascending=False).reset_index(drop=True)
    game_of = {t: (g.game_id, g.game_label) for g in games.itertuples() for t in g.game_label.split(' @ ')}
    p['game_id'] = [game_of.get(t, (None, None))[0] for t in p.team]
    p['game'] = [game_of.get(t, (None, t))[1] for t in p.team]

    st.caption('Tick two or three plays (the box at the left of each row).')
    event = st.dataframe(
        p[['name', 'team', 'over_under', 'number', 'model_prob', 'model_line', 'leg_price', 'ev', 'kelly']],
        on_select='rerun', selection_mode='multi-row', key='parlay_pick_{}'.format(book), hide_index=True, width='content',
        column_config={
            'name': 'Player', 'team': 'Team',
            'over_under': st.column_config.TextColumn('O/U'),
            'number': st.column_config.NumberColumn('Line', format='%.1f'),
            'model_prob': st.column_config.NumberColumn('Model p', format='percent'),
            'model_line': st.column_config.NumberColumn('Model', format='%d'),
            'leg_price': st.column_config.NumberColumn(book, format='%d'),
            'ev': st.column_config.NumberColumn('EV', format='percent'),
            'kelly': st.column_config.NumberColumn('Kelly ($)', format='dollar'),
        })
    rows = list(event.selection.rows)
    if len(rows) > MAX_LEGS:
        st.warning('Using the first {} plays ticked; untick the rest.'.format(MAX_LEGS))
        rows = rows[:MAX_LEGS]
    if len(rows) < 2:
        return
    legs = p.iloc[rows].assign(game_date=date)
    nums = [c for c in preds.columns if isinstance(c, (int, float))]
    pmfs = {r.player_id: r[nums].fillna(0).astype(float) for _, r in preds[preds.player_id.isin(legs.player_id)].iterrows()}
    held = legs.merge(held_today(user, date), how='left', on=['player_id', 'over_under', 'number']).held.fillna(0).values
    legs = legs.assign(held=held)

    st.subheader('Combinations')
    widths = [5, 1.2, 1.2, 1.6, 1.2, 1.2, 1.6, 1.2]
    for col, title in zip(st.columns(widths), ['Legs', 'Model p', 'Fair', '{} price'.format(book), 'EV', 'Kelly', 'Stake ($)', '']):
        col.caption(title)
    for size in range(2, len(legs) + 1):
        for idx in combinations(range(len(legs)), size):
            combo = legs.iloc[list(idx)]
            key = '|'.join('{}{}{}'.format(r.player_id, r.over_under[0], r.number) for r in combo.itertuples())
            joint = od.parlay_prob(combo, pmfs)
            notes = []
            same_player = combo.player_id.duplicated().any()
            if same_player:
                notes.append("same player: joint from his distribution, and the book may not allow it")
            elif combo.game.duplicated().any():
                notes.append('same game: legs treated as independent')
            if combo.held.sum() > 0:
                notes.append('${:,.2f} already on these legs as straights'.format(combo.held.sum()))
            c = st.columns(widths, vertical_alignment='center')
            c[0].markdown(' + '.join(leg_label(r) for _, r in combo.iterrows()) + (''.join('  \n:orange[{}]'.format(n) for n in notes)))
            c[1].markdown('{:.1%}'.format(joint))
            c[2].markdown('{:+d}'.format(od.fair_odds(joint)))
            start = None if same_player else od.parlay_price(combo.leg_price)  # the product means nothing for two lines on one player
            price = c[3].number_input('Price', value=start, step=5, key='parlay_price_{}'.format(key), placeholder='price',
                                      label_visibility='collapsed', help='Starts at the product of the leg prices; enter what the book offers')
            if price is None or abs(price) < 100:
                c[4].markdown('enter the price' if price is None else ':red[not a price]')
                continue
            kelly = round(max(od.kellyCrit(joint, price, False), 0) / .25) * .25
            c[4].markdown('{:+.1%}'.format(od.ev(joint, price)))
            c[5].markdown('${:,.2f}'.format(kelly))
            stake = c[6].number_input('Stake', value=float(kelly), min_value=0.0, step=.25, format='%.2f',
                                      key='parlay_stake_{}_{}'.format(key, price), label_visibility='collapsed')  # resets to Kelly when the price changes
            if c[7].button('Save', key='parlay_save_{}'.format(key), disabled=stake <= 0):
                od.save_parlay(combo, book, user, stake, price, joint, kelly)
                st.toast('Saved {}-leg parlay at {} {:+d} for ${:,.2f}'.format(size, book, int(price), stake))
    st.caption('Model p = the legs multiplied together (two lines on one player use his distribution instead). '
               'EV and Kelly use the raw model probability, and a parlay multiplies any overstatement in each leg. '
               "Kelly is sized on the parlay alone: it doesn't net out straights already held on the legs. "
               'Save records the ticket to nba/data/parlays; results and P&L come from the Pikkit reconcile.')
