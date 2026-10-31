import json
import requests
import re
import os
import glob
import logging
import pandas as pd
import numpy as np
import datetime as dt
from zoneinfo import ZoneInfo
from itertools import combinations


from betting.constants import books, live_markets
from collections import Counter, defaultdict
logger = logging.getLogger(__name__)
ET = ZoneInfo('America/New_York')
'''
Creating general betting functions that will be shared between NFL and NBA
'''
class odds():

    def __init__(self,configPath=os.path.join(os.path.dirname(__file__),'config.json')):
        self.dct = json.loads(open(configPath,'r').read())
        self.freeApi = self.dct.get('oddsApi').get('free')
        self.paid = self.dct.get('oddsApi').get('paid')
        self.nbaEvents = 'https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        self.nflEvents = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        self.budget = 1000
        self.kellyVal = .125
        self.parlayBudget = 750
        self.unit = self.budget * .01
        self.base_url = 'https://api.the-odds-api.com/v4/sports/{sport}/events/{{}}/odds?apiKey={{}}&regions=us&markets={markets}&dateFormat=iso&oddsFormat=american&bookmakers={books}'
        self.liveDir = os.environ.get('ODDS_LIVE_DIR', os.path.join(os.path.dirname(__file__), '..', 'nba', 'data', 'odds_live'))
        self.market_vars = {
            'threes': {
                'sport':'basketball_nba',
                'markets': 'player_threes,player_threes_alternate',
                'eventURL':self.nbaEvents},
            'points':{
                'sport': 'basketball_nba',
                'markets': 'player_points,player_points_alternate',
                'eventURL':self.nbaEvents},
            'pra':{
                'sport': 'basketball_nba',
                'markets': 'player_points_rebounds_assists,player_points_rebounds_assists_alternate',
                'eventURL':self.nbaEvents}
        }

    @staticmethod
    def slate_window(now=None):
        '''
        Time window for today's slate: now until 6am ET tomorrow, so a 2pm pull doesn't pick up tomorrow's noon games.
        Computed at call time (a long-running Streamlit process would otherwise keep the window from when it started).
        Output: start and end as UTC ISO strings
        '''
        now = now or dt.datetime.now(ET)
        end = (now + dt.timedelta(1)).replace(hour=6, minute=0, second=0, microsecond=0)
        fmt = '%Y-%m-%dT%H:%M:00Z'
        return now.astimezone(dt.timezone.utc).strftime(fmt), end.astimezone(dt.timezone.utc).strftime(fmt)

    def oddsData(self, eventURL, usePaid=False, cost=1, select=None):
        '''
        Gets today's events for the odds pulls (the events endpoint is free). Uses the free key while it has enough
        requests left for the games being pulled (games x cost per game), otherwise, or if the free key errors, the paid key.
        Inputs: events url, force the paid key, request cost per game (number of markets pulled),
                optional function that filters the event list (e.g. games tipping in 15-25 minutes)
        Output: list of event dicts, api key to use
        '''
        start, end = self.slate_window()
        free = requests.get(eventURL.format(self.freeApi, start, end))
        remaining = int(float(free.headers.get('x-requests-remaining', 0))) if free.ok else 0
        events = free.json() if free.ok else []
        events = select(events) if select else events
        needed = len(events) * cost
        if usePaid or not free.ok or remaining < needed:
            logger.info('Using paid key: free has {} requests left, pull needs {} (status {})'.format(remaining, needed, free.status_code))
            r = requests.get(eventURL.format(self.paid, start, end))
            r.raise_for_status()
            events = select(r.json()) if select else r.json()
            return events, self.paid
        logger.info('Using free key: {} requests left, pull needs {}'.format(remaining, needed))
        return events, self.freeApi

    @staticmethod
    def parse_event(game, pull_sequence, snapshot_ts):
        '''
        Flattens one event-odds response into long rows: one row per book x market x player x side x line.
        Same schema as the historical backfill (nba/NBAodds.py) plus event_id, so live and historical stack together.
        Inputs: event odds json, pull label (open/midday/close/manual), snapshot time (tz-aware ET)
        Output: DataFrame
        '''
        commence = pd.Timestamp(game.get('commence_time')).tz_convert(ET)
        rows = []
        for bk in game.get('bookmakers', []):
            for mrkt in bk.get('markets', []):
                last_update = pd.Timestamp(mrkt.get('last_update')).tz_convert(ET) if mrkt.get('last_update') else pd.NaT
                for o in mrkt.get('outcomes', []):
                    rows.append({'event_id': game.get('id'), 'game_date': commence.strftime('%Y-%m-%d'),
                                 'commence_time_et': commence.tz_localize(None),
                                 'home_team': game.get('home_team'), 'away_team': game.get('away_team'),
                                 'pull_sequence': pull_sequence, 'snapshot_ts': snapshot_ts.tz_localize(None),
                                 'book': bk.get('key'), 'market': mrkt.get('key'),
                                 'player_name': o.get('description'), 'side': o.get('name'),
                                 'line': o.get('point'), 'price': o.get('price'),
                                 'last_update': last_update.tz_localize(None) if pd.notna(last_update) else pd.NaT})
        df = pd.DataFrame(rows)
        if not df.empty:
            df['price'] = df['price'].round().astype('Int64')
            df['is_stale'] = (df.snapshot_ts - df.last_update) > pd.Timedelta(minutes=60)
        return df

    def pull_snapshot(self, pull_sequence, select=None):
        '''
        Pulls all live markets for today's games (or the games picked by select) and appends them as one parquet file:
        {liveDir}/{season}/{pull_sequence}/{date}_{HHMMSS}.parquet. Files are never overwritten.
        Inputs: pull label (open/midday/close/manual), optional event filter
        Output: DataFrame of the rows written (empty if nothing was posted)
        '''
        url = self.build_odds_url(markets=live_markets)
        events, akey = self.oddsData(self.nbaEvents, cost=len(live_markets), select=select)
        if not events:
            return pd.DataFrame()
        snapshot_ts = pd.Timestamp.now(tz=ET).floor('s')
        l, used = [], 0
        for event in events:
            r = requests.get(url.format(event.get('id'), akey))
            if not r.ok:
                logger.error('{} pull failed for event {} ({}): {}'.format(pull_sequence, event.get('id'), r.status_code, r.text[:300]))
                continue
            used += int(float(r.headers.get('x-requests-last', 0)))
            l.append(self.parse_event(r.json(), pull_sequence, snapshot_ts))
        df = pd.concat(l, ignore_index=True) if l else pd.DataFrame()
        if df.empty:
            logger.info('{} pull: no props posted for {} events'.format(pull_sequence, len(events)))
            return df
        date = snapshot_ts.strftime('%Y-%m-%d')
        folder = os.path.join(self.liveDir, self.season_of(date), pull_sequence)
        os.makedirs(folder, exist_ok=True)
        df.to_parquet(os.path.join(folder, '{}_{}.parquet'.format(date, snapshot_ts.strftime('%H%M%S'))), index=False)
        logger.info('{} pull: {} games, {:,} rows, {} credits'.format(pull_sequence, df.event_id.nunique(), len(df), used))
        return df

    @staticmethod
    def season_of(date):
        d = pd.to_datetime(date)
        start = d.year if d.month > 9 else d.year - 1
        return '{}-{}'.format(start, str(start + 1)[2:])

    def read_snapshots(self, date=None, pull_sequence='*'):
        '''Reads every snapshot file for one date (all pulls unless pull_sequence is given).'''
        date = date or dt.datetime.now(ET).strftime('%Y-%m-%d')
        files = glob.glob(os.path.join(self.liveDir, self.season_of(date), pull_sequence, '{}_*.parquet'.format(date)))
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True) if files else pd.DataFrame()

    @staticmethod
    def best_prices(snap, markets):
        '''
        Current price table from the snapshot rows. For each game, only its most recent snapshot counts (a line the book
        has since pulled drops out). The main and alternate markets often list the same line at different prices:
        keep the better payout (higher American odds), since that's the one you'd take. Never average prices.
        Inputs: long snapshot rows, list of market keys for the model
        Output: wide table name, number, over_under, one column per book, plus as_of (snapshot time)
        '''
        df = snap[snap.market.isin(markets)].copy() if not snap.empty else snap  # no snapshot yet today
        if df.empty:
            return pd.DataFrame(columns=['name', 'number', 'over_under'])
        df = df[df.snapshot_ts == df.groupby('event_id').snapshot_ts.transform('max')]
        df['book'] = df.book.map({k: v['odds_col'] for k, v in books.items()})
        best = df.groupby(['player_name', 'line', 'side', 'book']).agg(price=('price', 'max'), as_of=('snapshot_ts', 'max')).reset_index().astype({'price': float})  # NaN, not pd.NA, where a book has no price
        odf = best.pivot(index=['player_name', 'line', 'side'], columns='book', values='price').reset_index()
        odf.columns.name = None
        odf = odf.merge(best.groupby(['player_name', 'line', 'side']).as_of.max().reset_index(), on=['player_name', 'line', 'side'])
        return odf.rename(columns={'player_name': 'name', 'line': 'number', 'side': 'over_under'})

    def latest_odds(self, market, date=None):
        '''Current prices for a model's markets from today's snapshots (no API call).'''
        return self.best_prices(self.read_snapshots(date), self.market_vars[market]['markets'].split(','))

    @staticmethod
    def settle_bets(conn, user, since):
        '''
        Settles threes bets from the box scores: over wins if threes made > line, under if < line.
        void = box scores loaded for that date but the player didn't log minutes (DNP); pending = box scores not loaded yet.
        boost = profit boost as a fraction (.25 = 25%) on top of final_line; free_bet = 1 means a loss costs nothing.
        Inputs: sqlite connection, user, first date to include (YYYY-MM-DD)
        Output: DataFrame of bets with status and profit
        '''
        df = pd.read_sql('''
            SELECT b.date, b.player_id, b.over_under, b.number, b.bet_book, b.final_line, b.bet_amount, b.boost, b.free_bet,
                   pl.min, COALESCE(pl.lc_fgm,0) + COALESCE(pl.rc_fgm,0) + COALESCE(pl.abv_fgm,0) AS made,
                   ld.d AS loaded
            FROM bets b
            LEFT JOIN plyrLogs pl ON CAST(pl.player_id AS INTEGER) = CAST(b.player_id AS INTEGER) AND date(pl.game_date) = b.date
            LEFT JOIN (SELECT DISTINCT date(game_date) AS d FROM plyrLogs WHERE date(game_date) >= ? AND min > 0) ld ON ld.d = b.date
            WHERE b.user = ? AND b.market = 'threes' AND b.date >= ? AND b.bet_amount IS NOT NULL
        ''', conn, params=[since, user, since])
        played = df['min'].fillna(0) > 0
        won = np.where(df.over_under == 'Over', df.made > df.number, df.made < df.number)
        df['status'] = np.select([df.loaded.isna(), ~played, won], ['pending', 'void', 'win'], 'loss')
        price = df.final_line.astype(float)
        mult = np.where(price > 0, price / 100, 100 / price.abs())
        df['profit'] = np.select([df.status == 'win', df.status == 'loss'],
                                 [df.bet_amount * mult * (1 + df.boost.fillna(0)), np.where(df.free_bet.fillna(0) == 1, 0.0, -df.bet_amount)], 0.0)  # boost = profit boost; a free bet's stake isn't ours
        return df

    def current_bankroll(self, conn, user, start=1000, keep=.8, since='2026-10-01'):
        '''
        Daily Kelly bankroll: starting bankroll + settled P&L, minus 20% of each new profit high set aside.
        Losses count in full; set-aside money never comes back (a high-water skim, like withdrawing it).
        Inputs: sqlite connection, user, starting bankroll, share of profits kept in the bankroll, season start date
        Output: dict with bankroll, pnl, set_aside, pending (count of unsettled bets)
        '''
        df = self.settle_bets(conn, user, since)
        cum = df[df.status.isin(['win', 'loss'])].groupby('date').profit.sum().sort_index().cumsum()
        pnl = cum.iloc[-1] if len(cum) else 0.0
        set_aside = (1 - keep) * max(cum.max(), 0) if len(cum) else 0.0
        return {'bankroll': round(start + pnl - set_aside, 2), 'pnl': round(pnl, 2),
                'set_aside': round(set_aside, 2), 'pending': int((df.status == 'pending').sum())}

    def kellyCrit(self,prob,odds,show=True):
        '''
        Calculates the kelly criterion to help determin betting size, will give as a percentage of bankroll to use
        Inputs: your probability of the event winning, odds (American) that you will be paid out if the win occurs
        Outputs: a float with the amount of your bankroll
        '''
        if abs(prob) > .9999999:
            prob = self.convertOddsToPercent(prob)
        if abs(odds) < 100:
            return 0  # not a real American price (fetch_odds averaged a + and a - price): no bet
        b = odds / 100 if odds > 0 else 100 / abs(odds)
        kv = prob - (1-prob) / b
        wager = kv * self.budget * self.kellyVal
        pbudg = self.parlayBudget / self.budget
        if show:
            print('EV: {:.2%}\nStraight Wager {:.2f}\nParlay {:.2f}\nPayout: ${:.2f}'.format(
                self.ev(prob,odds),wager,wager * pbudg,wager * b))
        return wager

    def accumulateOdds(self,df,order,convert=True):
        final = pd.DataFrame(np.array(
            [self.convertPercentToOdds(v) if convert else v for r in df[order].cumsum(axis=1).values for v in r])
            .reshape(df[order].shape),columns=order, index=df.index)
        return final

    def oddsTable(self,preds):
        outcomes = [c for c in preds.columns if isinstance(c, (int, float))]
        ids = [c for c in preds.columns if c not in outcomes]
        finalo = self.accumulateOdds(preds,sorted(outcomes,reverse=True),convert=False)
        finalu = self.accumulateOdds(preds,outcomes,convert=False)
        finalu['over_under'] = 'Under'
        finalo['over_under'] = 'Over'
        overs = finalo.join(preds[ids]).melt(id_vars = ['name','team','player_id','over_under'],value_vars = outcomes, var_name = 'number',value_name = 'model_prob')
        unders = finalu.join(preds[ids]).melt(id_vars = ['name','team','player_id','over_under'],value_vars = outcomes, var_name = 'number',value_name = 'model_prob')
        final = pd.concat([overs, unders])
        final['number'] = np.where(final.over_under=='Over',final['number']-.5, final['number'] +.5)
        final = final[final['number']>0]
        return final

    def bet_table(self, lines, odf, sportsbooks=None):
        bks = sportsbooks or [k for k, v in books.items() if v['role'] != 'info']

        active = {k: v for k, v in books.items() if k in bks}
        final = lines.merge(odf, how='left', on=['name', 'number', 'over_under'])
        final['model_line'] = [self.convertPercentToOdds(x) for x in final.model_prob]
        for book, meta in active.items():
            odds_col = meta['odds_col']
            prefix = meta['col_prefix']
            if odds_col not in final.columns:
                final[odds_col] = np.nan
            kelly = [self.kellyCrit(p, odd, False) for p, odd in zip(final.model_prob, final[odds_col])]
            final['{}EV'.format(prefix)] = [self.ev(p, odd) for p, odd in
                                            zip(final.model_prob, final[odds_col].replace(0, 1))]
            final['{}Amount'.format(prefix)] = [round(x, 2) for x in kelly]
        return final

    def twoWayOdds(self,df,numCol,book):
        '''
        Will get all the two-way lines on from your data frame and provide the fair odds
        '''
        df = df[df.groupby(['name', numCol])['over_under'].transform(lambda x: set(x) >= {'Over', 'Under'})]
        grouped = df.groupby(['name', numCol]).apply(
            lambda g: pd.Series({
                'over_odds': g.loc[g['over_under'] == 'Over', book].iloc[0],
                'under_odds': g.loc[g['over_under'] == 'Under', book].iloc[0]
            })
        ).reset_index()

        grouped['over_odds_pct'] = grouped['over_odds'].apply(lambda x: self.convertOddsToPercent(x))
        grouped['under_odds_pct'] = grouped['under_odds'].apply(lambda x: self.convertOddsToPercent(x))
        grouped['{}Vig'.format(book)] = grouped.over_odds_pct + grouped.under_odds_pct - 1
        grouped['fair_over_odds_{}'.format(book)] = grouped['over_odds_pct'] / (grouped['over_odds_pct'] + grouped['under_odds_pct'])
        grouped['fair_under_odds_{}'.format(book)] = grouped['under_odds_pct'] / (grouped['over_odds_pct'] + grouped['under_odds_pct'])
        return grouped[['name',numCol,'fair_over_odds_{}'.format(book),'fair_under_odds_{}'.format(book),'{}Vig'.format(book)]]

    def build_odds_url(self, market=None, sportsbooks=None, markets=None):
        bks = list(books.keys()) if sportsbooks is None else sportsbooks
        mkts = ','.join(markets) if markets else self.market_vars[market]['markets']
        return self.base_url.format(
            sport='basketball_nba' if markets else self.market_vars[market]['sport'],
            markets=mkts,
            books='%2c'.join(bks)
        )

    @staticmethod
    def convertPercentToOdds(x):
        '''
        Take a decimal value and convert that into a US betting odd
        input: float value
        Output: int
        '''
        if x < .5:
            if x <= .01:
                return 9900
            else:
                return int(100 / x - 100)
        else:
            if x >= .99:
                return -9900
            else:
                return int(1 - (100 / (1 - x) - 100))


    def ev(self,winProb, odds):
        '''
        Need the probability your bet wins and given odds.  Will caluclate the effective value by this formula
        winProb * odds/100 - (1-winProb)
        '''
        mult = odds / 100 if odds > 0 else 100/abs(odds)
        wp = self.convertOddsToPercent(winProb) if abs(winProb) > 1 else winProb
        l = 1 - wp
        return wp * mult - l



    @staticmethod
    def convertOddsToPercent(x):
        '''
        give American odds and will convert into a percentage
        Input: Integer
        Output: float
        '''
        return np.abs(x) / (np.abs(x) + 100) if x < 0 else 100 / (x + 100)



    def zeroSumOdds(self,df):
        '''
        Takes Dataframe.  Assumes series is exhausitve will calculate the implied probability of each id, giving the id with its implied probability.  Also will write out vig on the bet.
        Inputs: DataFrame - 2xN - second columnn to be values
        Output: Text with vig and dataframe with implied probabilities
        '''
        total = sum([self.convertOddsToPercent(x) for x in df.iloc[:, 1].values])
        df['implied'] = [self.convertOddsToPercent(x) / total for x in df.iloc[:, 1].values]
        print("The total vig on this bet is: {:.2%}".format(1 - total))
        return df


    def parlayOdds(df):
        '''
        This will take a dataframe that has name, book odds, best odds/model odds and calulcate the price for each combination
        input: Dataframe with three columns
        ouptut: Dataframe
        '''

    def devigged(self,over,under,side,convert=True):

        if convert:
            over = self.convertOddsToPercent(over)
            under = self.convertOddsToPercent(under)
        return over/(over+under) if side == "over" else under/(under+over)

    @staticmethod
    def game_leaders(df, sims=10000):
        results = []
        df = df.reset_index(drop=True)
        cols = [x for x in df.columns if str(x).replace('.0', '').isdigit()]
        int_cols = [int(c) for c in cols]
        prob_matrix = (df[cols].values / df[cols].values.sum(axis=1, keepdims=True))

        for _ in range(sims):
            vals = np.array([np.random.choice(int_cols, p=prob_matrix[i]) for i in range(len(df))])
            winner_idx = vals.argmax()
            results.append((df.loc[winner_idx]['name'], vals[winner_idx]))

        winner_counts = Counter([r[0] for r in results])
        final = pd.DataFrame().from_dict(winner_counts, orient='index', columns=['Wins'])
        final['Win Rate'] = final.Wins / sims
        final['Line'] = [odds.convertPercentToOdds(x) for x in final['Win Rate']]
        gl =final.sort_values(by='Win Rate', ascending=False)[['Line', 'Win Rate']]
        makeX = (1 - (1 - (df.iloc[:, 4:].T.sort_index(ascending=False).cumsum())).product(axis=1)).round(5)
        makeX = makeX.to_frame(name='Prob')
        makeX['line'] = makeX['Prob'].apply(lambda x: odds.convertPercentToOdds(x))
        return gl,makeX

    def h2h(self,preds, fav, udog, spread, ovrLine):
        ovr = {}
        df = preds[preds.name.isin([fav, udog])].drop(['team','player_id'],axis=1).set_index('name')

        # Spread calculations (correct as-is)
        fsp = sum([df.loc[udog][i] * df.loc[fav][int(np.ceil(i + spread)):].sum()
                   for i in range(0, 10)])
        usp = sum([df.loc[fav][i] * df.loc[udog][int(np.ceil(i - spread)) if i - spread > 0 else 0:].sum()
                   for i in range(0, 10)])

        # Moneyline - strict wins only
        fml_wins = sum([df.loc[fav][i] * df.loc[udog][:i].sum() for i in range(1, 10)])
        uml_wins = sum([df.loc[udog][i] * df.loc[fav][:i].sum() for i in range(1, 10)])

        # Push probability
        push = sum([df.loc[fav][i] * df.loc[udog][i] for i in range(10)])

        # Conditional probabilities (for comparing to book odds)
        fml_cond = fml_wins / (fml_wins + uml_wins) if (fml_wins + uml_wins) > 0 else 0
        uml_cond = uml_wins / (fml_wins + uml_wins) if (fml_wins + uml_wins) > 0 else 0

        # Over/Under - correct calculation
        undr = sum([df.loc[fav][i] * df.loc[udog][j]
                    for i in range(10)
                    for j in range(10)
                    if i + j < ovrLine])

        ovr[fav] = {
            'spreadLine': self.convertPercentToOdds(fsp),
            'spreadProb': round(fsp, 3),
            'ml': self.convertPercentToOdds(fml_cond),  # Use conditional
            'mlWinProb': round(fml_wins, 3),  # Unconditional for Kelly
            'mlCondProb': round(fml_cond, 3)  # For comparing to book
        }

        ovr[udog] = {
            'spreadLine': self.convertPercentToOdds(usp),
            'spreadProb': round(usp, 3),
            'ml': self.convertPercentToOdds(uml_cond),  # Use conditional
            'mlWinProb': round(uml_wins, 3),  # Unconditional for Kelly
            'mlCondProb': round(uml_cond, 3)  # For comparing to book
        }

        ovr['combined'] = {
            'pushProb': round(push, 3),
            'underProb': round(undr, 3),
            'underMl': self.convertPercentToOdds(undr),
            'overProb': round(1 - undr, 3),
            'overMl': self.convertPercentToOdds(1 - undr)
        }

        return pd.DataFrame(ovr).T