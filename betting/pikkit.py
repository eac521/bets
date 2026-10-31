'''
Reconcile a Pikkit export against the app's `bets` table and the bankroll.

    python -m betting.pikkit nba/data/pikkit/transactions_2026-11-02.csv            # review: writes logs/reconcile_<date>.sql
    python -m betting.pikkit nba/data/pikkit/transactions_2026-11-02.csv --apply    # also runs it against the DB

Each in-scope Pikkit bet is one of:
  model  - threes over/under straight placed from the app -> must match a `bets` row (price, stake, book, line, free bet)
  live / h2h / parlay - bankroll money that isn't a model play -> one `bankroll_adjustments` row per settled bet (keyed by Pikkit bet_id)
In scope = tagged ModPlay, H2h or InverseParlay, or an untagged NBA threes over/under. Everything else (other sports, untagged
parlays) is listed in the summary but doesn't touch the bankroll.
Re-running on overlapping exports is safe: inserts and adjustments are keyed, updates are idempotent.
'''
import argparse
import os
import re
import sqlite3

import numpy as np
import pandas as pd

SCOPE_TAGS = ['ModPlay', 'H2h', 'InverseParlay']
BOOKS = {'Draftkings Sportsbook': 'DraftKings', 'Fanduel Sportsbook': 'FanDuel', 'BetMGM': 'BetMGM', 'Fanatics': 'Fanatics',
         'theScore Bet': 'theScore_Bet', 'theScore': 'theScore_Bet', 'ESPN BET': 'theScore_Bet'}
LINE = r'(\d+(?:\.\d)?)'
PATS = [  # (kind, pattern) for each book's threes over/under wording, first match wins
    ('side_first', re.compile(r'^(Over|Under)\s+' + LINE + r'\s+(.+?)(?:\s+-\s+|\s+\(|:|\s+Three|\s+Total)', re.I)),  # DK, MGM, Fanatics
    ('name_dash', re.compile(r'^(.+?)\s+-\s+(Over|Under)\s+' + LINE, re.I)),        # theScore
    ('name_side', re.compile(r'^(.+?)\s+(Over|Under)\s+' + LINE, re.I)),            # FanDuel
    ('name_colon', re.compile(r'^(.+?):\s+(\d+)\+', re.I)),                          # theScore ladders
    ('yes', re.compile(r'^Yes\s+(.+?)\s+to make\s+(\d+)\+', re.I)),                  # MGM ladders
    ('plus_first', re.compile(r'^(\d+)\+\s+(.+?)\s+Three', re.I)),                   # DK ladders
    ('name_plus', re.compile(r'^(.+?)\s+(\d+)\+\s+(?:Made|Threes)', re.I)),          # FanDuel ladders
]
H2H = re.compile(r'\sv\s.*Three|Race to \d|Most Three|Most Total Threes', re.I)
THREES = re.compile(r'three|3-pointers', re.I)


def parse_threes(txt):
    '''Player, side and line from a threes over/under description; (None, None, None) if it isn't one. N+ ladders are Over N-0.5.'''
    if H2H.search(txt) or not THREES.search(txt):
        return None, None, None
    for kind, pat in PATS:
        m = pat.search(txt)
        if not m:
            continue
        g = m.groups()
        if kind == 'side_first':
            return g[2].strip(), g[0].title(), float(g[1])
        if kind in ('name_dash', 'name_side'):
            return g[0].strip(), g[1].title(), float(g[2])
        if kind == 'plus_first':
            return g[1].strip(), 'Over', float(g[0]) - 0.5
        return g[0].strip(), 'Over', float(g[1]) - 0.5
    return None, None, None


def norm(name):
    s = re.sub(r'[^a-z ]', '', str(name).lower().replace('live ', ''))
    return re.sub(r'\s+(jr|sr|ii|iii|iv)$', '', s).strip()


def american(dec):
    return int(round((dec - 1) * 100)) if dec >= 2 else int(round(-100 / (dec - 1)))


def decimal(amer):
    return 1 + amer / 100 if amer > 0 else 1 + 100 / abs(amer)


def classify(pk):
    '''Adds date (ET placement date), book, player/side/line and kind (model, live, h2h, parlay, other, out_of_scope).'''
    pk = pk.copy()
    pk['tags'] = pk['tags'].fillna('').str.strip()
    pk['date'] = pd.to_datetime(pk['time_placed_iso']).dt.tz_convert('America/New_York').dt.strftime('%Y-%m-%d')
    pk['book'] = pk['sportsbook'].map(BOOKS).fillna(pk['sportsbook'])
    parsed = pk['bet_info'].apply(parse_threes)
    pk['player'], pk['over_under'], pk['number'] = parsed.str[0], parsed.str[1], parsed.str[2]
    pk['nkey'] = pk['player'].map(lambda x: norm(x) if x else None)
    straight = pk['type'] == 'straight'
    nba = pk['leagues'].fillna('').str.contains('NBA')
    live = pk['bet_info'].str.contains(r'\bLive\b')
    tagged = pk['tags'].apply(lambda t: any(s in t for s in SCOPE_TAGS))
    in_scope = tagged | (straight & nba & pk['player'].notna() & (pk['tags'] == ''))
    pk['kind'] = np.select(
        [~in_scope, ~straight, pk['tags'].str.contains('H2h') | pk['bet_info'].str.contains(H2H), live, pk['player'].notna()],
        ['out_of_scope', 'parlay', 'h2h', 'live', 'model'], 'other')
    return pk


def reconcile(pk, conn, user='EAC'):
    '''Returns (sql statements, summary dict). Nothing is written.'''
    pk = classify(pk)
    model = pk[pk['kind'] == 'model'].copy()
    lo, hi = model['date'].min(), model['date'].max()
    db = pd.read_sql('''SELECT b.*, p.name FROM bets b LEFT JOIN players p USING (player_id)
                        WHERE b.user = ? AND b.market = 'threes' AND b.date BETWEEN ? AND ?''', conn, params=[user, lo, hi])
    db['nkey'] = db['name'].map(norm)
    players = pd.read_sql('SELECT player_id, name FROM players', conn)
    pid = players.assign(nkey=players['name'].map(norm)).drop_duplicates('nkey').set_index('nkey')['player_id']

    keys = ['date', 'nkey', 'over_under', 'number']
    model['bet_book'] = model['book']
    for d in (model, db):
        d['k'] = d.groupby(keys + ['bet_book']).cumcount()
    m = model.merge(db, on=keys + ['bet_book', 'k'], how='outer', suffixes=('', '_db'), indicator=True)
    left = m[m['_merge'] == 'left_only'].copy()
    right = m[m['_merge'] == 'right_only'].copy()
    # second pass: same player and side within a day, different book or line (logged at the wrong book/line)
    pairs, used, used_left = [], set(), []
    for i, r in left.iterrows():
        c = right[(right['nkey'] == r['nkey']) & (right['over_under'] == r['over_under']) & ~right.index.isin(used)]
        c = c[(pd.to_datetime(c['date']) - pd.to_datetime(r['date'])).abs().dt.days <= 1]
        if len(c):
            used.add(c.index[0])
            used_left.append(i)
            pairs.append((r, right.loc[c.index[0]]))
    left = left.drop(index=used_left)
    right = right.drop(index=list(used))

    sql, notes = [], {'update': 0, 'insert': 0, 'delete': 0, 'unmapped': []}

    def where(player_id, date, ou, number, book):
        return "player_id = '{}' AND date = '{}' AND over_under = '{}' AND number = {} AND bet_book = '{}' AND user = '{}'".format(
            player_id, date, ou, number, book, user)

    def fix(r, old):  # r = Pikkit row, old = bets row (date, player_id, over_under, number, bet_book, final_line, bet_amount)
        price, amt = american(r['odds']), round(float(r['amount']), 2)
        settled_loss = r['status'] == 'SETTLED_LOSS'
        free = int(settled_loss and round(r['profit'], 2) == 0)
        if settled_loss and not free and round(r['profit'], 2) != round(-r['amount'], 2):
            amt = round(-float(r['profit']), 2)  # part of the stake was a bonus: keep the cash part
        same = (abs(decimal(old['final_line']) - r['odds']) < .02 and abs(amt - old['bet_amount']) < .01
                and r['book'] == old['bet_book'] and r['number'] == old['number'] and free == int(old['free_bet'] or 0))
        if same:
            return
        sql.append('-- {} {} {} {} {}: {} ${} -> {} {} ${}{}'.format(
            r['date'], r['player'], r['over_under'], old['number'], old['bet_book'], int(old['final_line']), old['bet_amount'],
            r['book'], price, amt, ', free bet' if free else ''))
        sql.append("UPDATE bets SET final_line = {}, bet_amount = {}, bet_book = '{}', number = {}, free_bet = {} WHERE {};".format(
            price, amt, r['book'], r['number'], free, where(old['player_id'], old['date'], old['over_under'], old['number'], old['bet_book'])))
        notes['update'] += 1

    sql.append('-- 1) Logged bets that differ from what was placed')
    for _, r in m[m['_merge'] == 'both'].iterrows():
        if pd.notna(r['final_line']):
            fix(r, {'date': r['date'], 'player_id': r['player_id'], 'over_under': r['over_under'], 'number': r['number'],
                    'bet_book': r['bet_book'], 'final_line': r['final_line'], 'bet_amount': r['bet_amount'], 'free_bet': r['free_bet']})
    for r, old in pairs:
        if pd.notna(old['final_line']):
            fix(r, {'date': old['date'], 'player_id': old['player_id'], 'over_under': old['over_under'], 'number': old['number'],
                    'bet_book': old['bet_book'], 'final_line': old['final_line'], 'bet_amount': old['bet_amount'], 'free_bet': old['free_bet']})

    sql.append('-- 2) Placed but never logged')
    for _, r in left.sort_values(['date', 'nkey']).iterrows():
        if r['nkey'] not in pid.index:
            notes['unmapped'].append(r['player'])
            continue
        free = int(r['status'] == 'SETTLED_LOSS' and round(r['profit'], 2) == 0)
        sql.append("INSERT OR REPLACE INTO bets VALUES ('{}', '{}', 'threes', '{}', {}, '{}', {}, {}, '{}', 0, {});  -- {} {}".format(
            pid[r['nkey']], r['date'], r['over_under'], r['number'], r['book'], american(r['odds']), round(float(r['amount']), 2),
            user, free, r['player'], r['bet_info'][:60].replace('\n', ' ')))
        notes['insert'] += 1

    sql.append('-- 3) Logged but not placed (only for days the export fully covers)')
    for _, r in right.sort_values(['date', 'nkey']).iterrows():
        if not (lo < r['date'] < hi):
            continue
        sql.append('-- {} {} {} {} {} {} ${}'.format(r['date'], r['name'], r['over_under'], r['number'], r['bet_book'],
                                                     int(r['final_line']), r['bet_amount']))
        sql.append('DELETE FROM bets WHERE {};'.format(where(r['player_id'], r['date'], r['over_under'], r['number'], r['bet_book'])))
        notes['delete'] += 1

    sql.append('-- 4) Bankroll money that isn\'t a model play (settled only; keyed by Pikkit bet_id)')
    adj = pk[pk['kind'].isin(['live', 'h2h', 'parlay', 'other']) & pk['status'].str.startswith('SETTLED')]
    for _, r in adj.iterrows():
        sql.append("INSERT OR REPLACE INTO bankroll_adjustments VALUES ('{}', '{}', '{}', '{}', {}, '{}');".format(
            r['bet_id'], r['date'], user, r['kind'], round(float(r['profit']), 2), r['bet_info'][:80].replace("'", "''")))

    settled = pk['status'].str.startswith('SETTLED')
    summary = pk[settled].groupby('kind').agg(bets=('bet_id', 'size'), staked=('amount', 'sum'), profit=('profit', 'sum')).round(2)
    notes['by_kind'] = summary
    notes['range'] = (lo, hi)
    return sql, notes


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('export', help='Pikkit CSV export')
    ap.add_argument('--db', default=os.environ.get('NBA_DB_PATH', os.path.join(os.path.dirname(__file__), '..', 'nba', 'data', 'database', 'nba.db')))
    ap.add_argument('--user', default='EAC')
    ap.add_argument('--apply', action='store_true', help='run the SQL against the DB (one transaction)')
    a = ap.parse_args()
    conn = sqlite3.connect(a.db)
    sql, notes = reconcile(pd.read_csv(a.export), conn, a.user)
    lo, hi = notes['range']
    out = os.path.join(os.path.dirname(__file__), '..', 'logs', 'reconcile_{}.sql'.format(hi))
    body = ['-- Pikkit reconcile: {} ({} to {})'.format(os.path.basename(a.export), lo, hi), 'BEGIN;'] + sql + ['COMMIT;']
    with open(out, 'w') as f:
        f.write('\n'.join(body) + '\n')
    print('Pikkit settled by kind:\n{}'.format(notes['by_kind'].to_string()))
    print('bets: {} updates, {} inserts, {} deletes{}'.format(notes['update'], notes['insert'], notes['delete'],
          '; unmapped names: {}'.format(notes['unmapped']) if notes['unmapped'] else ''))
    print('SQL: {}'.format(os.path.abspath(out)))
    if a.apply:
        conn.executescript('\n'.join(body))
        from betting.funcs import odds
        print('Applied. Bankroll: {}'.format(odds().current_bankroll(conn, a.user)))


if __name__ == '__main__':
    main()
