import json
import requests
import re
import os
import pandas as pd
import numpy as np
import datetime as dt
from itertools import combinations


from betting.constants import books
from collections import Counter, defaultdict
'''
Creating general betting functions that will be shared between NFL and NBA
'''
class odds():

    def __init__(self,configPath=os.path.join(os.path.dirname(__file__), 'config.json')):
        self.dct = json.loads(open(configPath,'r').read())
        self.freeApi = self.dct.get('oddsApi').get('free')
        self.paid = self.dct.get('oddsApi').get('paid')
        self.nbaEvents = 'https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        self.nflEvents = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        self.todayISO = (dt.datetime.now()).strftime('%Y-%m-%dT%H:%M:00Z')
        self.tomorISO = (dt.datetime.now() + dt.timedelta(1)).strftime('%Y-%m-%dT%H:%M:00z')
        self.budget = 1000
        self.kellyVal = .125
        self.parlayBudget = 750
        self.unit = self.budget * .01
        self.base_url = 'https://api.the-odds-api.com/v4/sports/{sport}/events/{{}}/odds?apiKey={{}}&regions=us&markets={markets}&dateFormat=iso&oddsFormat=american&bookmakers={books}'
        self.market_vars = {
            'threes': {
                'sport':'basketball_nba',
                'markets': 'player_threes,player_threes_alternate',
                'eventURL':self.nbaEvents},
            'points':{
                'sport': 'basketball_nba',
                'markets': 'player_points,player_points_alternate',
                'eventURL':self.nbaEvents}
        }
    def oddsData(self, eventURL,usePaid=False):
        '''
        ISO Formatted dates for today and tomorrow returns the games that will be played today ids for odds pulls
        Inputs: isoformatted dates for today and tomorrow
        Output: list of game ids
        '''
        if int(requests.get(eventURL.format(self.freeApi,self.todayISO,self.tomorISO)).headers['x-requests-used'])>=490 or usePaid:
            print('Free is out')
            r = requests.get(eventURL.format(self.paid,self.todayISO,self.tomorISO))
            print(r.headers)
            key = self.paid
        else:
            r = requests.get(eventURL.format(self.freeApi,self.todayISO,self.tomorISO))
            print('Free:',r.headers)
            key = self.freeApi

        return [d['id'] for d in r.json()],key


    def kellyCrit(self,prob,odds,show=True):
        '''
        Calculates the kelly criterion to help determin betting size, will give as a percentage of bankroll to use
        Inputs: your probability of the event winning, odds (American) that you will be paid out if the win occurs
        Outputs: a float with the amount of your bankroll
        '''
        if abs(prob) > .9999999:
            prob = self.convertOddsToPercent(prob)
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
        overs = finalo.join(preds[ids]).melt(id_vars = ['name','team','player_id','over_under'],value_vars = preds.columns, var_name = 'number',value_name = 'model_prob')
        unders = finalu.join(preds[ids]).melt(id_vars = ['name','team','player_id','over_under'],value_vars = preds.columns, var_name = 'number',value_name = 'model_prob')
        final = pd.concat([overs, unders])
        final['number'] = np.where(final.over_under=='Over',final['number']-.5, final['number'] +.5)
        final = final[final['number']>0]
        return final

    def fetch_odds(self,market):
        url = self.build_odds_url(market)
        eventURL = self.market_vars.get(market).get('eventURL')
        df = pd.DataFrame()
        events, akey = self.oddsData(eventURL, usePaid=True)
        l = []
        for event in events:
            r = requests.get(url.format(event, akey))
            game = r.json()
            for key in game.get('bookmakers'):
                bk = key.get('title')
                for mrkt in key.get('markets'):
                    temp = pd.DataFrame(mrkt.get('outcomes'))
                    temp.columns = ['over_under', 'name', 'price', 'number']
                    temp['book'] = bk
                    l.append(temp)
        pd.concat(l)
        odf = df.pivot_table(index=['name', 'number', 'over_under'], columns=['book']).reset_index()
        odf.columns = [c[1] if c[1] != '' else c[0] for c in odf.columns]
        return odf

    def bet_table(self, lines, odf, sportsbooks=None):
        bks = sportsbooks or ['draftkings', 'fanduel', 'espnbet']

        active = {k: v for k, v in books.items() if k in bks}
        final = lines.merge(odf, how='left', on=['name', 'number', 'over_under'])
        final['model_line'] = [self.convertPercentToOdds(x) for x in final.model_prob]
        for book, meta in active.items():
            odds_col = meta['odds_col']
            prefix = meta['col_prefix']
            kelly = [self.kellyCrit(p, odd, False) for p, odd in zip(final.model_prob, final[odds_col])]
            final['{}EV'.format(prefix)] = [self.ev(p, odd) for p, odd in
                                            zip(final.model_prob, final[odds_col].replace(0, 1))]
            final['{}Amount'.format(prefix)] = [round(x * self.budget * self.kellyVal, 2) for x in kelly]
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

    def build_odds_url(self, market, sportsbooks=None):
        bks = list(books.keys()) if sportsbooks is None else sportsbooks
        return self.base_url.format(
            sport=self.market_vars[market]['sport'],
            markets=self.market_vars[market]['markets'],
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
        wp = self.convertOddsToPercent(winProb) if abs(winProb) > 0 else winProb
        l = 1 - wp
        print(mult,wp)
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