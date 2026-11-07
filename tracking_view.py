"""
Tracking tab: daily P&L with ROI and cumulative P&L (filterable by book), expected vs actual wins by side and price, and closing line value
by book. Rendered by app.py as its third tab: tracking_view.render(user, start, keep, since).
"""
import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from nba import NBAbase
from betting import market as mk
from betting.funcs import odds

db = NBAbase.base()
od = odds()
BUCKETS = [1, 1 + 100 / 150, 2.0, 2.4, 3.0, 4.0, 1000]  # decimal odds edges
BUCKET_LABELS = ['< -150', '-150 to -100', '+100 to +140', '+140 to +200', '+200 to +300', '+300+']
POS, NEG, CUM = '#2a78d6', '#eb6834', '#7b5cd6'


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


def aligned_domains(*ranges):
    '''
    Axis domains with zero at the same height on every axis, so a dual-axis chart has one zero line.
    Inputs: (low, high) per axis (NaN = no data); each is widened to include 0 and padded 5%.
    Output: list of [low, high], one per axis
    '''
    rs = []
    for lo, hi in ranges:
        lo, hi = min(np.nan_to_num(lo), 0.0), max(np.nan_to_num(hi), 0.0)
        pad = .05 * (hi - lo) or 1.0
        rs.append((lo - pad if lo < 0 else 0.0, hi + pad))
    f = max(-lo / (hi - lo) for lo, hi in rs)  # zero's height from the bottom, as a share of the axis
    f = min(f, .95)
    return [[min(lo, -f * hi / (1 - f)), max(hi, -lo * (1 - f) / f) if f > 0 else hi] for lo, hi in rs]


def daily_chart(d):
    '''Daily P&L bars (model / other, labeled with the model ROI) and cumulative P&L as a line on the right axis.'''
    x = alt.X('yearmonthdate(date):T', title=None, axis=alt.Axis(format='%b %d'))
    xc = alt.X('yearmonthdate(date):T', title=None, bandPosition=.5)  # center of the day's bar
    bars_src = d.melt(id_vars=['date'], value_vars=['model', 'other'], var_name='part', value_name='pl')
    bars = alt.Chart(bars_src).mark_bar(cornerRadiusEnd=3).encode(
        x=x, y=alt.Y('pl:Q', title='Daily P&L ($)', stack='zero'),
        color=alt.Color('part:N', title=None, scale=alt.Scale(domain=['model', 'other'], range=[POS, '#9aa4b2'])),
        tooltip=[alt.Tooltip('yearmonthdate(date):T', title='date'), 'part', alt.Tooltip('pl:Q', format='$,.2f')])
    d = d.assign(label_y=d.total.clip(lower=0))
    labels = alt.Chart(d).mark_text(dy=-6, fontSize=10, color='#666').encode(
        x=xc, y='label_y:Q', text=alt.Text('roi:Q', format='.0%'))
    bar_dom, cum_dom = aligned_domains(
        (bars_src[bars_src.pl < 0].groupby('date').pl.sum().min(), bars_src[bars_src.pl > 0].groupby('date').pl.sum().max() * 1.12),
        (d.cum.min(), d.cum.max()))
    bars = bars.encode(y=alt.Y('pl:Q', title='Daily P&L ($)', stack='zero', scale=alt.Scale(domain=bar_dom, nice=False)))
    labels = labels.encode(y=alt.Y('label_y:Q', scale=alt.Scale(domain=bar_dom, nice=False)))
    zero = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='#888').encode(y=alt.Y('y:Q', scale=alt.Scale(domain=bar_dom, nice=False)))
    cum_axis = alt.Axis(orient='right', titleColor=CUM, labelColor=CUM, format='$,.0f', grid=False)
    line = alt.Chart(d).mark_line(point=alt.OverlayMarkDef(color=CUM), strokeWidth=2, color=CUM).encode(
        x=xc, y=alt.Y('cum:Q', title='Cumulative P&L ($)', axis=cum_axis, scale=alt.Scale(domain=cum_dom, nice=False)),
        tooltip=[alt.Tooltip('yearmonthdate(date):T', title='date'), alt.Tooltip('cum:Q', format='$,.2f', title='cumulative')])
    return alt.layer(bars + labels + zero, line).resolve_scale(y='independent').properties(height=300)


def expected_vs_actual(bets):
    s = bets[bets.status.isin(['win', 'loss'])].copy()
    s['won'] = (s.status == 'win').astype(int)
    s['p_book'] = 1 / s.dec
    g = s.groupby(['over_under', 'bucket'], observed=True).agg(
        bets=('won', 'size'), actual=('won', 'sum'), model=('p_model', 'sum'), book=('p_book', 'sum'),
        var=('p_model', lambda p: (p * (1 - p)).sum()), staked=('bet_amount', 'sum'), profit=('profit', 'sum')).reset_index()
    g['z (actual vs model)'] = (g.actual - g.model) / np.sqrt(g['var'])
    g['roi'] = g.profit / g.staked
    long = g.melt(id_vars=['over_under', 'bucket'], value_vars=['actual', 'model', 'book'], var_name='series', value_name='wins')
    chart = alt.Chart(long).mark_bar(cornerRadiusEnd=3).encode(
        x=alt.X('series:N', title=None, axis=None), y=alt.Y('wins:Q', title='Wins'),
        color=alt.Color('series:N', title=None, scale=alt.Scale(domain=['actual', 'model', 'book'], range=['#1b1b1b', POS, '#9aa4b2'])),
        column=alt.Column('bucket:N', title=None, sort=BUCKET_LABELS, header=alt.Header(labelFontSize=10)),
        row=alt.Row('over_under:N', title=None),
        tooltip=['over_under', 'bucket', 'series', alt.Tooltip('wins:Q', format='.1f')]).properties(width=70, height=120)
    table = g[['over_under', 'bucket', 'bets', 'actual', 'model', 'book', 'z (actual vs model)', 'roi']]
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
    color = alt.Color('clv:Q', title='Mean CLV', scale=alt.Scale(scheme='redblue', domain=[-lim, lim]), legend=alt.Legend(format='.0%'))
    panels = []
    for side, title in [('Over', 'Overs'), ('Under', 'Unders')]:
        base = alt.Chart(g[g.over_under == side]).encode(
            x=alt.X('book:N', sort=[label[b] for b in books], title=None, axis=alt.Axis(orient='top', labelAngle=-30, labelLimit=220)),
            y=alt.Y('bucket:N', sort=BUCKET_LABELS, title=None))
        empty = base.mark_rect(color='#eef0f3', stroke='white', strokeWidth=2)
        rect = base.transform_filter('datum.n > 0').mark_rect(stroke='white', strokeWidth=2).encode(
            color=color, tooltip=['bet_book', 'bucket', alt.Tooltip('clv:Q', format='+.1%'), 'n'])
        text = base.transform_filter('datum.n > 0').mark_text(fontSize=10).encode(text=alt.Text('n:Q'), color=alt.value('#1b1b1b'))
        panels.append((empty + rect + text).properties(title=title, width=alt.Step(80), height=alt.Step(30)))
    return alt.hconcat(*panels).resolve_scale(color='shared')


@st.fragment  # its widgets rerun only this tab
def render(user, start, keep, since):
    bets = settled_bets(user, since)
    if bets.empty:
        st.info('No bets since {}.'.format(since))
        return
    adj = adjustments(user, since)
    options = sorted(bets.bet_book.dropna().unique())
    sel = st.multiselect('Books', options, default=options, key='tracking_books')
    if not sel:
        st.info('Pick at least one book.')
        return
    all_books = set(sel) == set(options)
    fb = bets[bets.bet_book.isin(sel)]

    st.subheader('Daily results')
    full = daily_frame(bets, adj, start, keep)  # bankroll and set-aside are account-wide, whatever the book filter
    d = daily_frame(fb, adj if all_books else adj.iloc[0:0], start, keep)  # other P&L has no book: only with every book selected
    if d.empty:
        st.info('No settled bets for these books yet.')
    else:
        c = st.columns(4)
        c[0].metric('Bankroll', '${:,.2f}'.format(full.bankroll.iloc[-1]))
        c[1].metric('Model P&L', '${:,.2f}'.format(d.model.sum()), '{:.1%} ROI'.format(d.model.sum() / d.staked.sum()) if d.staked.sum() else None)
        c[2].metric('Other P&L', '${:,.2f}'.format(d.other.sum()) if all_books else '-')
        c[3].metric('Set aside', '${:,.2f}'.format(full.set_aside.iloc[-1]))
        st.altair_chart(daily_chart(d), width='stretch')
        st.caption("Bars: model plays and other (live, H2H, parlays), labeled with the model plays' ROI that day. "
                   'Line (right axis): cumulative P&L. Other P&L has no book, so it shows only with every book selected.')

    st.subheader('Expected vs actual wins')
    chart, table = expected_vs_actual(fb)
    st.altair_chart(chart)
    st.dataframe(table.style.format({'model': '{:.1f}', 'book': '{:.1f}', 'z (actual vs model)': '{:+.2f}', 'roi': '{:+.1%}'}, na_rep='-'),
                 hide_index=True, width='content')
    st.caption('Model = sum of the model probability at bet time; book = sum of the implied probability of the price taken (vig included). '
               'z beyond about ±2 means actual wins are outside what the model expects.')

    st.subheader('Closing line value')
    ref = st.radio('Closing reference', ['Market consensus close', "Bet book's close"], horizontal=True, key='clv_ref')
    cells, cover = clv_table(fb, 'own' if ref.startswith('Bet') else 'consensus')
    if cover.empty:
        st.info('No closing snapshots for these bets yet (close pulls start Oct 28, 2026).')
        return
    st.altair_chart(clv_heatmap(cells, cover, sel))
    st.caption("CLV = de-vigged closing probability x price taken - 1. Cells show the number of bets; (x/y) next to each book = bets "
               "matched to a closing price / bets placed (close pulls start Oct 28, 2026). Consensus = mean de-vigged "
               "close across all books listing that line; a book's own close is shaded toward its own position.")
