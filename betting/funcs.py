import json
import requests
import pandas as pd
import numpy as np
from itertools import combinations
import os
import datetime as dt
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
        self.market_vars =  {
        'threes': {'url':'https://api.the-odds-api.com/v4/sports/basketball_nba/events/{}/odds?apiKey={}&regions=us&markets=player_threes,player_threes_alternate&dateFormat=iso&oddsFormat=american&bookmakers=draftkings%2Cfanduel%2cespnbet',
                   'col_name':'threesMade'
                   },
        'points': {'url':'https://api.the-odds-api.com/v4/sports/basketball_nba/events/{}/odds?apiKey={}&regions=us&markets=player_points,player_points_alternate&dateFormat=iso&oddsFormat=american&bookmakers=draftkings%2Cfanduel%2cespnbet',
                   'col_name':'pointsScored'
        }
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
        return kv

    def accumulateOdds(self,df,order,convert=True):
        final = pd.DataFrame(np.array(
            [self.convertPercentToOdds(v) if convert else v for r in df[order].cumsum(axis=1).values for v in r])
            .reshape(df[order].shape),columns=order, index=df.index)
        return final

    def oddsTable(self,preds,idInfo,col):
        finalo = self.accumulateOdds(preds,preds.columns[::-1])
        finalu = self.accumulateOdds(preds,preds.columns)
        overs = idInfo.join(finalo.filter(preds.columns)).melt(id_vars = ['name','team'],value_vars = preds.columns, var_name = col)
        unders = idInfo.join(finalu.filter(preds.columns)).melt(id_vars = ['name','team'],value_vars = preds.columns, var_name = col)
        unders['over_under'] = 'Under'
        overs['over_under'] = 'Over'
        final = pd.concat([overs, unders])
        final.threesMade = np.where(final.over_under=='Over',final[col]-.5, final[col] +.5)
        final = final[final[col]>0]
        return final

    def fetch_odds(self,market):
        url = self.market_vars.get(market).get('url')
        col = self.market_vars.get(market).get('col_name')
        df = pd.DataFrame()
        events, akey = self.oddsData(self.nbaEvents, usePaid=True)
        for event in events:
            r = requests.get(url.format(event, akey))
            game = r.json()
            for key in game.get('bookmakers'):
                bk = key.get('title')
                for mrkt in key.get('markets'):
                    temp = pd.DataFrame(mrkt.get('outcomes'))
                    temp.columns = ['over_under', 'name', 'price', self.market_vars.get(market).get('col_name')]
                    temp['book'] = bk
                    df = pd.concat([temp, df])
        odf = df.pivot_table(index=['name', col, 'over_under'], columns=['book']).reset_index()
        odf.columns = [col[1] if col[1] != '' else col[0] for col in odf.columns]
        return odf

    def bet_table(self,overs, odf, val_col):
        '''
        expects a dataframe with espn(theScore Bet), Draftkings and FanDuel lines and a dataframe with predicted values.
        Creates a merged dataframe that gives bet amount and EV
        '''
        final = odf.merge(overs, how='left', on=['name', val_col, 'over_under'])
        final['prob'] = np.where(final.value < 0, round(abs(final.value) / (abs(final.value) + 100), 4),
                                 round(100 / (final.value + 100), 4))
        espnKelly = [self.kellyCrit(p,odd,False) for p,odd in zip(final.prob,final['theScore Bet'])]
        fdKelly = [self.kellyCrit(p,odd,False) for p,odd in zip(final.prob,final['FanDuel'])]
        dkKelly = [self.kellyCrit(p,odd,False) for p,odd in zip(final.prob,final['DraftKings'])]
        final['DKEV'] = ['{:.2%}'.format(self.ev(p, odd)) for p, odd in zip(final.prob, final.DraftKings.replace(0, 1))]
        final['FanDuelEV'] = ['{:.2%}'.format(self.ev(p, odd)) for p, odd in
                              zip(final.prob, final.FanDuel.replace(0, 1))]
        # final['BetMGMEV'] = ['{:.2%}'.format(self.ev(p, odd))  for p,odd in zip(final.prob,final.BetMGM)]
        final['espnEV'] = ['{:.2%}'.format(self.ev(p, odd)) for p, odd in zip(final.prob, final['theScore Bet'])]
        # first row needs to be the tiebreaker row
        final['FanDuelAmount'] = [round(x * self.budget * self.kellyVal, 2) for x in fdKelly]
        final['DraftKingsAmount'] = [round(x * self.budget * self.kellyVal, 2) for x in dkKelly]
        # final['BetMGMAmount'] = [round(x * self.budget * self.kellyVal,2) for x in final.BetMGMKelly.values]
        final['theScore BetAmount'] = [round(x * self.budget * self.kellyVal, 2) for x in espnKelly]
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


    @staticmethod
    def ev(winProb, odds):
        '''
        Need the probability your bet wins and given odds.  Will caluclate the effective value by this formula
        winProb * odds/100 - (1-winProb)
        '''
        mult = odds / 100 if odds > 0 else 100/abs(odds)
        l = 1 - winProb
        return winProb * mult - l



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