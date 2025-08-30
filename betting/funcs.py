import json
import requests
from itertools import combinations
'''
Creating general betting functions that will be shared between NFL and NBA
'''
class odds():
    
    def __init__(self,configPath='config.json'):
        #self.dct = json.loads(open(configPath,'r').read())
        #self.freeApi = dct.get('oddsApi').get('free')
        #self.paid = dct.get('oddsApi').get('paid')
        #self.nbaEvents = 'https://api.the-odds-api.com/v4/sports/basketball_nba/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        #self.nflEvents = self.events = 'https://api.the-odds-api.com/v4/sports/americanfootball_nfl/events?apiKey={}&dateFormat=iso&commenceTimeFrom={}&commenceTimeTo={}'
        #self.todayISO = (dt.datetime.now()).strftime('%Y-%m-%dT%H:%M:00Z')
        #self.tomorISO = (dt.datetime.now() + dt.timedelta(1)).strftime('%Y-%m-%dT%H:%M:00z')

    def oddsData(self, eventURL):
        '''
        ISO Formatted dates for today and tomorrow returns the games that will be played today ids for odds pulls
        Inputs: isoformatted dates for today and tomorrow
        Output: list of game ids
        '''
        if int(requests.get(eventURL.format(self.free,self.todayISO,self.tomorISO)).headers['x-requests-used'])==500:
            print('Free is out')
            r = requests.get(eventURL.format(self.paid,self.todayISO,self.tomorISO))
            print(r.headers)
            key = self.paid
        else:
            r = requests.get(eventURL.format(self.free,self.todayISO,self.tomorISO))
            print('Free:',r.headers)
            key = self.free
        
        return [d['id'] for d in r.json()],key
    
    @staticmethod
    def kellyCrit(prob,odds):
        '''
        Calculates the kelly criterion to help determin betting size, will give as a percentage of bankroll to use
        Inputs: your probability of the event winning, odds (American) that you will be paid out if the win occurs
        Outputs: a float with the amount of your bankroll
        '''
        if odds > 0:
            return prob - (1-prob) / (odds / 100)
        else:
            return prob - (1-prob) / (100/np.abs(odds))
    
    @staticmethod
    def convertPercentToOdds(x):
        '''
        Take a decimal value and convert that into a US betting odd
        input: float value
        Output: int 
        '''
        if x < .5:
            if x<=.01:
                return 9900
            else:
                return int(100/x -100)
        
        else:
            if x >= .99:
                return -9900
            else:
                return int(1 -(100/(1-x) - 100))
    
    @staticmethod
    def ev(winProb,odds):
        '''
        Need the probability your bet wins and given odds.  Will caluclate the effective value by this formula
        winProb * odds/100 - (1-winProb)
        '''
        l = 1 - winProb
        mult = odds / 100 if odds > 0 else odds/(100+odds)
        return winProb * mult - l
    
    @staticmethod
    def convertOddsToPercent(x):
        '''
        give American odds and will convert into a percentage
        Input: Integer
        Output: float
        '''
        return 100 if x > 0 else x / (x+100)
    
    @staticmethod
    def zeroSumOdds(df):
        '''
        Takes Dataframe.  Assumes series is exhausitve will calculate the implied probability of each id, giving the id with its implied probability.  Also will write out vig on the bet.
        Inputs: DataFrame - 2xN - second columnn to be values
        Output: Text with vig and dataframe with implied probabilities
        '''                
        total = sum([convertOddsToPercent(x) for x in df.iloc[:,1].values])
        df['implied'] = [convertOddsToPercent(x)/total for x in  df.iloc[:,1].values]
        print("The total vig on this bet is: {:.2%}".format(1-total))
        return df
    
    def parlayOdds(df):
        '''
        This will take a dataframe that has name, book odds, best odds/model odds and calulcate the price for each combination
        input: Dataframe with three columns
        ouptut: Dataframe
        '''


#	def twoWayOdds(df):
#		'''
#		
#		'''
