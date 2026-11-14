"""
Tracking tab: cumulative P&L over a chosen date range with one line per book, expected vs actual wins by side and price,
and closing line value by book. Each section has its own book selection.
Rendered by app.py: tracking_view.render(user, start, keep, since).
"""
import datetime as dt

import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from nba import NBAbase
from betting import market as mk
from betting.constants import BOOK_COLORS, OTHER_BOOK_COLOR, STOPLIGHT
from betting.funcs import odds

db = NBAbase.base()
od = odds()
BUCKETS = [1, 1 + 100 / 150, 2.0, 2.4, 3.0, 4.0, 1000]  # decimal odds edges
BUCKET_LABELS = ['< -150', '-150 to -100', '+100 to +140', '+140 to +200', '+200 to +300', '+300+']
POS = '#2a78d6'
THIN = 5  # CLV cells with fewer bets than this are faded


@st.cache_data(ttl=300)
def settled_bets(user, since):
    df = odds.settle_bets(db.conn, user, since)
    df['player_id'] = df.player_id.astype(str)
    df['dec'] = mk.decimal(df.final_line.astype(float))
    df['bucket'] = pd.cut(df.dec, BUCKETS, labels=BUCKET_LABELS)
    p = pd.read_sql('''SELECT CAST(b.player_id AS TEXT) AS player_id, b.date, b.number,
                              SUM(CASE WHEN p.number > b.number THEN p.model_prob ELSE 0 END) AS p_over
                       FROM (SELECT DISTINCT player_id, date, number FROM bets WHERE user = ? AND market = 'threes' AND date >= ?) b
                       JOIN predictions p ON CAST(p.player_id AS INTEGER) = CAST(b.player_id AS INTEGER) AND p.date = b.date AND p.market = 'threes'
                       GROUP BY 1, 2, 3''', db.conn, params=[user, since])
    df = df.merge(p, on=['player_id', 'date', 'number'], how='left')
    df['p_model'] = np.where(df.over_under == 'Over', df.p_over, 1 - df.p_over)
    names = pd.read_sql('SELECT CAST(player_id AS TEXT) AS player_id, name FROM players', db.conn)
    return df.merge(names, on='player_id', how='left')


@st.cache_data(ttl=300)
def adjustments(user, since):
    has = db.conn.execute("SELECT 1 FROM sqlite_master WHERE name = 'bankroll_adjustments'").fetchone()
    if not has:
        return pd.DataFrame(columns=['date', 'source', 'amount'])
    return pd.read_sql('SELECT date, source, amount FROM bankroll_adjustments WHERE user = ? AND date >= ?', db.conn, params=[user, since])


@st.cache_data(ttl=600)
def closing_fair(dates):
    '''De-vigged closing P(over) for every threes line: per book, and the market consensus (mean across books).'''
    frames = []
    for d in dates:
        s = od.read_snapshots(d, pull_sequence='close')
        if not s.empty:
            frames.append(s[s.market.isin(['player_threes', 'player_threes_alternate'])])
    if not frames:
        return pd.DataFrame(), pd.DataFrame()
    snap = pd.concat(frames, ignore_index=True)
    snap = snap[snap.snapshot_ts == snap.groupby(['event_id', 'book']).snapshot_ts.transform('max')]
    fair = mk.fair_p_over(mk.two_way(snap))
    fair['nkey'] = fair.player_name.map(mk.norm_name)
    fair['book'] = fair.book.map(mk.BOOK_COL)
    fair = fair.rename(columns={'p_over': 'close_p_over'})
    own = fair[['game_date', 'book', 'nkey', 'line', 'close_p_over']]
    consensus = fair.groupby(['game_date', 'nkey', 'line']).close_p_over.mean().reset_index()
    return own, consensus


def daily_frame(bets, adj, start, keep):
    '''One row per day: model and other P&L, staked, ROI, cumulative P&L, set-aside and bankroll.'''
    s = bets[bets.status.isin(['win', 'loss'])]
    d = s.groupby('date').agg(model=('profit', 'sum'), staked=('bet_amount', 'sum'))
    d = d.join(adj.groupby('date').amount.sum().rename('other'), how='outer').fillna(0).sort_index()
    d['total'] = d.model + d.other
    d['roi'] = np.where(d.staked > 0, d.model / d.staked, np.nan)
    d['cum'] = d.total.cumsum()
    d['set_aside'] = (1 - keep) * d.cum.cummax().clip(lower=0)
    d['bankroll'] = start + d.cum - d.set_aside
    d = d.rename_axis('date').reset_index()
    d['date'] = pd.to_datetime(d.date)
    return d


def range_frame(bets, adj, start, end):
    '''
    Daily and cumulative P&L inside [start, end] (YYYY-MM-DD strings), one series per book plus Total.
    Total = settled straights at every book + parlays (bankroll_adjustments, source = parlay). Live and H2H are left out.
    Every series starts at $0 the day before the range, and a day with no settled bets carries the line forward flat.
    Output: long frame date, series, daily, cum
    '''
    s = bets[bets.status.isin(['win', 'loss']) & bets.date.between(start, end)]
    days = pd.date_range(start, end).strftime('%Y-%m-%d')
    daily = s.pivot_table(index='date', columns='bet_book', values='profit', aggfunc='sum').reindex(days).fillna(0.0)
    parlays = adj[(adj.source == 'parlay') & adj.date.between(start, end)].groupby('date').amount.sum()
    daily['Total'] = daily.sum(axis=1) + parlays.reindex(days).fillna(0.0)
    origin = (pd.Timestamp(start) - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    daily = pd.concat([pd.DataFrame(0.0, index=[origin], columns=daily.columns), daily])
    out = daily.rename_axis('date').reset_index().melt(id_vars='date', var_name='series', value_name='daily')
    out['cum'] = out.groupby('series').daily.cumsum()
    out['date'] = pd.to_datetime(out.date)
    return out


def range_chart(long, series):
    '''Cumulative P&L within the range: one line per selected series, in the book's color.'''
    src = long[long.series.isin(series)]
    colors = [BOOK_COLORS.get(b, OTHER_BOOK_COLOR) for b in series]
    x = alt.X('yearmonthdate(date):T', title=None, axis=alt.Axis(format='%b %d', tickCount='day', labelOverlap=True))
    line = alt.Chart(src).mark_line(point=alt.OverlayMarkDef(size=45), strokeWidth=2.5).encode(
        x=x, y=alt.Y('cum:Q', title='Cumulative P&L in range ($)', axis=alt.Axis(format='$,.0f')),
        color=alt.Color('series:N', title=None, scale=alt.Scale(domain=list(series), range=colors)),
        tooltip=[alt.Tooltip('yearmonthdate(date):T', title='date'), alt.Tooltip('series:N', title='book'),
                 alt.Tooltip('daily:Q', format='$,.2f', title='day'), alt.Tooltip('cum:Q', format='$,.2f', title='cumulative')])
    zero = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='#888').encode(y='y:Q')
    return (zero + line).properties(height=320)


def expected_vs_actual(bets):
    s = bets[bets.status.isin(['win', 'loss'])].copy()
    s['won'] = (s.status == 'win').astype(int)
    s['p_book'] = 1 / s.dec
    g = s.groupby(['over_under', 'bucket'], observed=True).agg(
        bets=('won', 'size'), actual=('won', 'sum'), model=('p_model', 'sum'), book=('p_book', 'sum'),
        var=('p_model', lambda p: (p * (1 - p)).sum()), staked=('bet_amount', 'sum'), profit=('profit', 'sum')).reset_index()
    g['z (actual vs model)'] = (g.actual - g.model) / np.sqrt(g['var'])
    # same z with bets on one date allowed to move together (ladders, shared games): the date is the unit, not the bet
    r = s.dropna(subset=['p_model']).assign(resid=lambda d: d.won - d.p_model)
    by_date = r.groupby(['over_under', 'bucket', 'date'], observed=True).resid.sum().reset_index()
    cl = by_date.groupby(['over_under', 'bucket'], observed=True).resid.agg(
        total='sum', sd=lambda x: np.sqrt((x ** 2).sum()), dates='size').reset_index()
    cl['z (by date)'] = np.where(cl.dates >= 5, cl.total / cl.sd.replace(0, np.nan), np.nan)
    g = g.merge(cl[['over_under', 'bucket', 'z (by date)']], on=['over_under', 'bucket'], how='left')
    g['roi'] = g.profit / g.staked
    long = g.melt(id_vars=['over_under', 'bucket'], value_vars=['actual', 'model', 'book'], var_name='series', value_name='wins')
    chart = alt.Chart(long).mark_bar(cornerRadiusEnd=3).encode(
        x=alt.X('series:N', title=None, axis=None), y=alt.Y('wins:Q', title='Wins'),
        color=alt.Color('series:N', title=None, scale=alt.Scale(domain=['actual', 'model', 'book'], range=['#1b1b1b', POS, '#9aa4b2'])),
        column=alt.Column('bucket:N', title=None, sort=BUCKET_LABELS, header=alt.Header(labelFontSize=10)),
        row=alt.Row('over_under:N', title=None),
        tooltip=['over_under', 'bucket', 'series', alt.Tooltip('wins:Q', format='.1f')]).properties(width=70, height=120)
    table = g[['over_under', 'bucket', 'bets', 'actual', 'model', 'book', 'z (actual vs model)', 'z (by date)', 'roi']]
    return chart, table


def clv_table(bets, ref):
    '''
    Mean CLV per book x side x price bucket ('All books' included), plus coverage: bets per book and how many matched a close.
    Output: (cells: bet_book, over_under, bucket, clv, n; cover: bet_book, total, matched)
    '''
    keys = ['date', 'player_id', 'over_under', 'number', 'bet_book']
    s = bets[bets.status.isin(['win', 'loss', 'pending'])].drop_duplicates(keys).copy()
    total = s.groupby('bet_book').size()
    own, consensus = closing_fair(tuple(sorted(s.date.unique())))
    if own.empty:
        return pd.DataFrame(), pd.DataFrame()
    s['nkey'] = s.name.map(mk.norm_name)
    if ref == 'own':
        s = s.merge(own.rename(columns={'game_date': 'date', 'line': 'number', 'book': 'bet_book'}),
                    on=['date', 'bet_book', 'nkey', 'number'], how='inner')
    else:
        s = s.merge(consensus.rename(columns={'game_date': 'date', 'line': 'number'}), on=['date', 'nkey', 'number'], how='inner')
    s = s.drop_duplicates(keys)
    s['p_close'] = np.where(s.over_under == 'Over', s.close_p_over, 1 - s.close_p_over)
    s['clv'] = s.p_close * s.dec - 1
    allb = s.assign(bet_book='All books')
    cells = pd.concat([allb, s]).groupby(['bet_book', 'over_under', 'bucket'], observed=True).agg(
        clv=('clv', 'mean'), n=('clv', 'size')).reset_index()
    cover = pd.DataFrame({'total': total, 'matched': s.groupby('bet_book').size()}).fillna(0).astype(int)
    cover.loc['All books'] = cover.sum()
    return cells, cover.rename_axis('bet_book').reset_index()


def clv_heatmap(cells, cover, books):
    '''Overs and unders side by side: price buckets down, books across (every selected book, matched or not).'''
    books = ['All books'] + list(books)
    if cells.empty:  # no bet matched a close: still draw every book, all empty
        cells = pd.DataFrame(columns=['bet_book', 'over_under', 'bucket', 'clv', 'n']).astype({'clv': float, 'n': float})
    cover = cover.set_index('bet_book').reindex(books).fillna(0).astype(int)
    label = {b: '{} ({}/{})'.format(b, cover.loc[b, 'matched'], cover.loc[b, 'total']) for b in books}
    grid = pd.MultiIndex.from_product([books, ['Over', 'Under'], BUCKET_LABELS], names=['bet_book', 'over_under', 'bucket']).to_frame(index=False)
    g = grid.merge(cells.astype({'bucket': str}), how='left', on=['bet_book', 'over_under', 'bucket'])
    g['n'] = g.n.fillna(0).astype(int)
    g['book'] = g.bet_book.map(label)
    lim = max(.05, float(g.clv.abs().quantile(.95))) if g.clv.notna().any() else .05
    color = alt.Color('clv:Q', title='Mean CLV', legend=alt.Legend(format='+.0%'),
                      scale=alt.Scale(domain=[-lim, -lim / 2, 0, lim / 2, lim], range=STOPLIGHT, clamp=True))  # yellow sits at 0 CLV
    fade = alt.condition('datum.n < {}'.format(THIN), alt.value(.4), alt.value(1))
    panels = []
    for side, title in [('Over', 'Overs'), ('Under', 'Unders')]:
        base = alt.Chart(g[g.over_under == side]).encode(
            x=alt.X('book:N', sort=[label[b] for b in books], title=None, axis=alt.Axis(orient='top', labelAngle=-30, labelLimit=220)),
            y=alt.Y('bucket:N', sort=BUCKET_LABELS, title=None))
        empty = base.mark_rect(color='#eef0f3', stroke='white', strokeWidth=2)
        filled = base.transform_filter('datum.n > 0')
        rect = filled.mark_rect(stroke='white', strokeWidth=2).encode(
            color=color, opacity=fade, tooltip=['bet_book', 'bucket', alt.Tooltip('clv:Q', format='+.1%'), 'n'])
        value = filled.mark_text(fontSize=11, fontWeight='bold', dy=-6, color='#1b1b1b').encode(text=alt.Text('clv:Q', format='+.1%'), opacity=fade)
        count = filled.transform_calculate(label="'n = ' + datum.n").mark_text(fontSize=9, dy=8, color='#1b1b1b').encode(text='label:N', opacity=fade)
        panels.append((empty + rect + value + count).properties(title=title, width=alt.Step(84), height=alt.Step(40)))
    return alt.hconcat(*panels).resolve_scale(color='shared')


@st.fragment  # its widgets rerun only this tab
def render(user, start, keep, since):
    bets = settled_bets(user, since)
    if bets.empty:
        st.info('No bets since {}.'.format(since))
        return
    adj = adjustments(user, since)
    options = sorted(bets.bet_book.dropna().unique())

    st.subheader('Daily results')
    today = dt.date.today()
    c = st.columns([1, 1, 3])
    first = c[0].date_input('Start', value=max(today - dt.timedelta(days=6), dt.date.fromisoformat(since)),
                            min_value=dt.date.fromisoformat(since), max_value=today, key='trk_start')
    last = c[1].date_input('Stop', value=today, min_value=dt.date.fromisoformat(since), max_value=today, key='trk_stop')
    series = c[2].multiselect('Books', ['Total'] + options, default=['Total'], key='trk_series')
    full = daily_frame(bets, adj, start, keep)  # bankroll and set-aside are account-wide and season-long, whatever is selected
    if first > last:
        st.warning('Start is after Stop.')
    elif not series:
        st.info('Pick Total or at least one book.')
    else:
        long = range_frame(bets, adj, str(first), str(last))
        in_range = bets[bets.status.isin(['win', 'loss']) & bets.date.between(str(first), str(last))]
        m = st.columns(4)
        m[0].metric('Bankroll', '${:,.2f}'.format(full.bankroll.iloc[-1]))
        m[1].metric('Total P&L in range', '${:,.2f}'.format(long[long.series == 'Total'].daily.sum()))
        m[2].metric('Straights in range', '${:,.2f}'.format(in_range.profit.sum()),
                    '{:.1%} ROI'.format(in_range.profit.sum() / in_range.bet_amount.sum()) if in_range.bet_amount.sum() else None)
        m[3].metric('Set aside', '${:,.2f}'.format(full.set_aside.iloc[-1]))
        st.altair_chart(range_chart(long, series), width='stretch')
        st.caption('Cumulative P&L inside the chosen dates; every line starts at $0 the day before Start. A book line is settled '
                   'straights at that book. Total = straights at every book + parlays (parlays arrive with the Pikkit import, so they lag); '
                   'live and H2H are left out. Bankroll and set-aside are for the whole season.')

    st.subheader('Expected vs actual wins')
    eva_books = st.multiselect('Books', options, default=options, key='trk_eva_books')
    if not eva_books:
        st.info('Pick at least one book.')
    else:
        chart, table = expected_vs_actual(bets[bets.bet_book.isin(eva_books)])
        st.altair_chart(chart)
        st.dataframe(table.style.format({'model': '{:.1f}', 'book': '{:.1f}', 'z (actual vs model)': '{:+.2f}', 'z (by date)': '{:+.2f}',
                                         'roi': '{:+.1%}'}, na_rep='-'), hide_index=True, width='content')
        st.caption('Model = sum of the model probability at bet time; book = sum of the implied probability of the price taken (vig included). '
                   'z beyond about ±2 means actual wins are outside what the model expects. z (by date) treats each date as one observation, '
                   "so ladders and same-night bets don't count as independent (blank under 5 dates); trust it over the first z. "
                   'Slicing by book makes many cells: expect one in twenty past ±2 by chance.')

    st.subheader('Closing line value')
    c = st.columns([3, 2])
    clv_books = c[0].multiselect('Books', options, default=options, key='trk_clv_books')
    ref = c[1].radio('Closing reference', ['Market consensus close', "Bet book's close"], horizontal=True, key='clv_ref')
    if not clv_books:
        st.info('Pick at least one book.')
        return
    cells, cover = clv_table(bets[bets.bet_book.isin(clv_books)], 'own' if ref.startswith('Bet') else 'consensus')
    if cover.empty:
        st.info('No closing snapshots for these bets yet (close pulls start Oct 28, 2026).')
        return
    st.altair_chart(clv_heatmap(cells, cover, clv_books))
    st.caption("CLV = de-vigged closing probability x price taken - 1. Red = you got a worse price than the close, yellow = even, green = better. "
               "Each cell shows mean CLV and the number of bets; cells under {} bets are faded. (x/y) next to each book = bets "
               "matched to a closing price / bets placed (close pulls start Oct 28, 2026). Consensus = mean de-vigged "
               "close across all books listing that line; a book's own close is shaded toward its own position.".format(THIN))
