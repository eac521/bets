"""
Tracking tab: daily P&L with ROI and the bankroll, expected vs actual wins by side and price, and closing line value
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
POS, NEG = '#2a78d6', '#eb6834'


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


def daily_panel(bets, adj, start, keep):
    s = bets[bets.status.isin(['win', 'loss'])]
    d = s.groupby('date').agg(model=('profit', 'sum'), staked=('bet_amount', 'sum'))
    d = d.join(adj.groupby('date').amount.sum().rename('other'), how='outer').fillna(0).sort_index()
    d['total'] = d.model + d.other
    d['roi'] = np.where(d.staked > 0, d.model / d.staked, np.nan)
    cum = d.total.cumsum()
    d['set_aside'] = (1 - keep) * cum.cummax().clip(lower=0)
    d['bankroll'] = start + cum - d.set_aside
    d = d.reset_index()
    d['date'] = pd.to_datetime(d.date)
    x = alt.X('yearmonthdate(date):T', title=None, axis=alt.Axis(format='%b %d'))
    bars_src = d.melt(id_vars=['date'], value_vars=['model', 'other'], var_name='part', value_name='pl')
    bars = alt.Chart(bars_src).mark_bar(cornerRadiusEnd=3).encode(
        x=x, y=alt.Y('pl:Q', title='Daily P&L ($)', stack='zero'),
        color=alt.Color('part:N', title=None, scale=alt.Scale(domain=['model', 'other'], range=[POS, '#9aa4b2'])),
        tooltip=[alt.Tooltip('yearmonthdate(date):T', title='date'), 'part', alt.Tooltip('pl:Q', format='$,.2f')])
    d['label_y'] = d.total.clip(lower=0)
    labels = alt.Chart(d).mark_text(dy=-6, fontSize=10, color='#444').encode(
        x=x, y='label_y:Q', text=alt.Text('roi:Q', format='.0%'))
    zero = alt.Chart(pd.DataFrame({'y': [0]})).mark_rule(color='#888').encode(y='y:Q')
    top = (bars + labels + zero).properties(height=230)
    bk = d.melt(id_vars=['date'], value_vars=['bankroll', 'set_aside'], var_name='series', value_name='usd')
    bottom = alt.Chart(bk).mark_line(point=True, strokeWidth=2).encode(
        x=x, y=alt.Y('usd:Q', title='Bankroll ($)', scale=alt.Scale(zero=False)),
        color=alt.Color('series:N', title=None, scale=alt.Scale(domain=['bankroll', 'set_aside'], range=['#1b1b1b', NEG])),
        tooltip=[alt.Tooltip('yearmonthdate(date):T', title='date'), 'series', alt.Tooltip('usd:Q', format='$,.2f')]
    ).properties(height=170)
    return alt.vconcat(top, bottom).resolve_scale(x='shared', color='independent'), d


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
    s = bets[bets.status.isin(['win', 'loss', 'pending'])].copy()
    own, consensus = closing_fair(tuple(sorted(s.date.unique())))
    if own.empty:
        return pd.DataFrame()
    s['nkey'] = s.name.map(mk.norm_name)
    if ref == 'own':
        s = s.merge(own.rename(columns={'game_date': 'date', 'line': 'number', 'book': 'bet_book'}),
                    on=['date', 'bet_book', 'nkey', 'number'], how='inner')
    else:
        s = s.merge(consensus.rename(columns={'game_date': 'date', 'line': 'number'}), on=['date', 'nkey', 'number'], how='inner')
    s = s.drop_duplicates(['date', 'player_id', 'over_under', 'number', 'bet_book'])
    s['p_close'] = np.where(s.over_under == 'Over', s.close_p_over, 1 - s.close_p_over)
    s['clv'] = s.p_close * s.dec - 1
    s['col'] = s.over_under + ' ' + s.bucket.astype(str)
    allb = s.assign(bet_book='All books')
    return pd.concat([allb, s]).groupby(['bet_book', 'col']).agg(clv=('clv', 'mean'), n=('clv', 'size')).reset_index()


def clv_heatmap(t):
    cols = ['{} {}'.format(side, b) for side in ['Over', 'Under'] for b in BUCKET_LABELS]
    rows = ['All books'] + sorted([b for b in t.bet_book.unique() if b != 'All books'])
    lim = max(.05, float(t.clv.abs().quantile(.95)))
    base = alt.Chart(t).encode(x=alt.X('col:N', sort=cols, title=None, axis=alt.Axis(labelAngle=-35)),
                               y=alt.Y('bet_book:N', sort=rows, title=None))
    rect = base.mark_rect(stroke='white', strokeWidth=2).encode(
        color=alt.Color('clv:Q', title='Mean CLV', scale=alt.Scale(scheme='redblue', domain=[-lim, lim]), legend=alt.Legend(format='.0%')),
        tooltip=['bet_book', 'col', alt.Tooltip('clv:Q', format='+.1%'), 'n'])
    text = base.mark_text(fontSize=10).encode(text=alt.Text('n:Q'), color=alt.value('#1b1b1b'))
    return (rect + text).properties(height=34 * len(rows))


def render(user, start, keep, since):
    bets = settled_bets(user, since)
    if bets.empty:
        st.info('No bets since {}.'.format(since))
        return
    adj = adjustments(user, since)

    st.subheader('Daily results')
    chart, d = daily_panel(bets, adj, start, keep)
    c = st.columns(4)
    c[0].metric('Bankroll', '${:,.2f}'.format(d.bankroll.iloc[-1]))
    c[1].metric('Model P&L', '${:,.2f}'.format(d.model.sum()), '{:.1%} ROI'.format(d.model.sum() / d.staked.sum()) if d.staked.sum() else None)
    c[2].metric('Other P&L', '${:,.2f}'.format(d.other.sum()))
    c[3].metric('Set aside', '${:,.2f}'.format(d.set_aside.iloc[-1]))
    st.altair_chart(chart, width='stretch')
    st.caption("Bars: model plays and other (live, H2H, parlays), labeled with the model plays' ROI that day. "
               "Bottom: bankroll at the end of each day and the amount set aside.")

    st.subheader('Expected vs actual wins')
    chart, table = expected_vs_actual(bets)
    st.altair_chart(chart)
    st.dataframe(table.style.format({'model': '{:.1f}', 'book': '{:.1f}', 'z (actual vs model)': '{:+.2f}', 'roi': '{:+.1%}'}, na_rep='-'),
                 hide_index=True, width='content')
    st.caption('Model = sum of the model probability at bet time; book = sum of the implied probability of the price taken (vig included). '
               'z beyond about ±2 means actual wins are outside what the model expects.')

    st.subheader('Closing line value')
    ref = st.radio('Closing reference', ['Market consensus close', "Bet book's close"], horizontal=True, key='clv_ref')
    t = clv_table(bets, 'own' if ref.startswith('Bet') else 'consensus')
    if t.empty:
        st.info('No closing snapshots for these bets yet (close pulls start Oct 28, 2026).')
        return
    st.altair_chart(clv_heatmap(t), width='stretch')
    st.caption("CLV = de-vigged closing probability x price taken - 1. Cells show the number of bets. Consensus = mean de-vigged "
               "close across all books listing that line; a book's own close is shaded toward its own position.")
