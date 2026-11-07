"""
Query tab: ad hoc SQL against nba.db through a read-only connection (mode=ro + query_only), so nothing typed here can
change the database. Results stay in session state, so other widgets don't rerun the query.
Rendered by app.py as its fourth tab: query_view.render().
"""
import sqlite3
import time
import pandas as pd
import streamlit as st

from nba import NBAbase

DB_PATH = NBAbase.base().db
MAX_SECONDS = 30      # a query running longer is cancelled (it would hold this session until it finished)
MAX_ROWS = 10000      # rows shown; the CSV download has them all


def read_only_conn():
    conn = sqlite3.connect('file:{}?mode=ro'.format(DB_PATH), uri=True, check_same_thread=False)
    conn.execute('PRAGMA query_only = ON')
    return conn


def run_query(sql):
    '''Runs one statement read-only with a time limit. Output: (DataFrame, seconds) or raises the sqlite error.'''
    conn = read_only_conn()
    start = time.time()
    conn.set_progress_handler(lambda: 1 if time.time() - start > MAX_SECONDS else 0, 10000)  # nonzero = interrupt
    try:
        df = pd.read_sql(sql, conn)
    finally:
        conn.close()
    return df, time.time() - start


@st.cache_data(ttl=3600)
def schema():
    '''Tables and views with their columns, for the reference list.'''
    conn = read_only_conn()
    try:
        objs = pd.read_sql("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY type, name", conn)
        cols = {n: ', '.join(r[1] for r in conn.execute('PRAGMA table_info("{}")'.format(n))) for n in objs.name}
    finally:
        conn.close()
    return objs.assign(columns=objs.name.map(cols))


def render():
    st.caption('Read-only: writes are rejected. Queries stop after {}s; up to {:,} rows are shown.'.format(MAX_SECONDS, MAX_ROWS))
    with st.expander('Tables and columns'):
        st.dataframe(schema(), hide_index=True, width='stretch')
    with st.form('query_form', border=False):
        sql = st.text_area('SQL', height=180, key='query_sql', placeholder='SELECT * FROM bets ORDER BY date DESC LIMIT 50')
        run = st.form_submit_button('Run', type='primary')
    if run and sql.strip():
        try:
            st.session_state['query_result'] = run_query(sql)
            st.session_state.pop('query_error', None)
        except Exception as e:
            st.session_state['query_error'] = 'Query cancelled after {}s.'.format(MAX_SECONDS) if 'interrupted' in str(e) else str(e)
            st.session_state.pop('query_result', None)
    if 'query_error' in st.session_state:
        st.error(st.session_state['query_error'])
    elif 'query_result' in st.session_state:
        df, secs = st.session_state['query_result']
        st.caption('{:,} rows · {:.2f}s{}'.format(len(df), secs, ' · showing the first {:,}'.format(MAX_ROWS) if len(df) > MAX_ROWS else ''))
        st.dataframe(df.head(MAX_ROWS), hide_index=True, width='stretch')
        st.download_button('Download CSV', df.to_csv(index=False), file_name='query.csv', mime='text/csv')
