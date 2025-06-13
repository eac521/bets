import pandas as pd
import numpy as np
import time
from tqdm import tqdm
from .nba import base

'''
This class is focused on what is already load via the nba api and stored in our database.
Focus of this class is related to data for modeling. 
Will have functions around:
- loading in model data
- Feature creation for models
- EDA charting functions
- Game-by-game functions for player/team information

'''

class data(base):
	def __init__(self):
		super().__init__()
		
		
	def threeData(self,file='./data/sql/thrq.sql'):
        '''Will generate the data needed to generate predictions for threes
        Inputs: str for game date formatted as YYYY-MM-DD
        Output: DataFrame of your X values
        '''
        thr = pd.read_sql(open(file,'r').read(),self.conn)
        tmsa = self.rolling_team_sa()
        plsh = self.rolling_player_shot(thr)
        final = thr.merge(plsh,how='left',on=['player_id','game_date']).merge(tmsa,how='left',on=['game_date','opp_id'])
        #dropping the fga columns as they have been used for shots and shots allowed
        final.drop(['ra_fga','mid_fga','paint_fga'],axis=1,inplace=True)
        #fill na for first game/opp game of season, adding in a 9 as this is similar to the all-star break 
        final.daysBetweenGames.fillna(9,inplace = True)
        final.oppDaysLastGame.fillna(9,inplace=True)
        # final = final[final.game_date==date]
        return final
        

    def rolling_team_sa(self):
        '''
        Setting up a rolling distribution of each shot location that each team allows. 
        Inputs: dataframe with team_id, game_date and at least one fgallowed column
        output: DataFrame with six percentiles for each fgallowed column
        '''
        sa = '''
    SELECT team_id as opp_id,game_date ,ra_fga as ra_fgallowed, paint_fga as paint_fgallowed, mid_fga as mid_fgallowed, 
    coalesce(lc_fga,0) + coalesce(rc_fga,0) as crn_fgallowed, abv_fga as abv_fgallowed,season
    from team_def
    order by opp_id,game_date
    '''
        df = pd.read_sql(sa,self.conn)
        #quants = ([1/6,1/3,.5,2/3,5/6,1],['st','nd','rd','rth','fth','xth'])
        shots = [col for col in df.columns if re.search('_fgallowed$',col)!=None]
        shotdf = pd.DataFrame()
        pdf = df[['opp_id','game_date']]
        for col in shots:
           
            skew = df.groupby(['opp_id'])[col].rolling(15,closed='left',min_periods=5).skew()
            kurt = df.groupby(['opp_id'])[col].rolling(15,closed='left',min_periods=5).kurt()
            pdf = pdf.join(skew.reset_index(name='{}skew'.format(col)).set_index('level_1').drop('opp_id',axis=1))
            pdf = pdf.join(kurt.reset_index(name='{}kurt'.format(col)).set_index('level_1').drop('opp_id',axis=1))
        
        return pdf

    def rolling_player_shot(self,df):
        '''
        Setting up a rolling distribution of each shot location for player.  This will take in the threes query
        Inputs: dataframe with player_id, game_date and at least one fga column
        output: DataFrame with six percentiles for each fga column
        '''
        # quants = ([1/6,1/3,.5,2/3,5/6,1],['st','nd','rd','rth','fth','xth'])
        shots = [col for col in df.columns if re.search('_fga$',col)!=None]
        shotdf = pd.DataFrame()
        pdf = df[['game_date','player_id']]
        for col in shots:
            skew = df.groupby(['player_id'])[col].rolling(15,closed='left',min_periods=5).skew()
            kurt = df.groupby(['player_id'])[col].rolling(15,closed='left',min_periods=5).kurt()
            pdf = pdf.join(skew.reset_index(name='{}skew'.format(col)).set_index('level_1').drop('player_id',axis=1))
            pdf = pdf.join(kurt.reset_index(name='{}kurt'.format(col)).set_index('level_1').drop('player_id',axis=1))

        return pdf

    def naInfo(self,df,uniqueCol = None):
        naCols = df.columns[df.isna().any()].tolist()
        print('{} columns have nans'.format(len(naCols)))
        for col in naCols:
            if uniqueCol != None:
                print("{}:\nmissing:{:,}-{:.2%}\nunique:{:,}-{:.2%}\n\t".format(col,
                df[col].isna().sum(),df[col].isna().sum()/len(df[col]),df[df[col].isna()][uniqueCol].nunique(),
                                                                    df[df[col].isna()][uniqueCol].nunique()/df[col].isna().sum()))
            else:
                print("{}:\nmissing:{:,}-{:.2%}".format(col,df[col].isna().sum()))
        return naCols

    def cleanNaThr(self,data):
        '''
        specifically to replace nans in the threes dataset
        Inputs: the completed Threes data set with distibuition information
        Ouput: dataframe removing nans
        '''
        lgAvgs = pd.read_sql('''select season, sum(open_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga))  open_fg3aSeason,
                    sum(wide_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga))  wide_fg3aSeason,
                    avg(pace) as paceSeason,
                    avg(def_rate) as def_rateSeason



                from team_def
                group by season
                HAVING season is not Null
            ''' ,self.conn).set_index('season').reset_index().shift()
        data.mvAvgThrees = np.where(data.mvAvgThrees.isna(),data.careerAvgThrees,data.mvAvgThrees)
        data.mvAvgUsage = np.where(data.mvAvgUsage.isna(),data.careerUsage,data.mvAvgUsage)
        data.mvAvgOffRating = np.where(data.mvAvgOffRating.isna(),data.careerOffRating,data.mvAvgOffRating)
        data.mvAvgFtPrct = np.where(data.mvAvgFtPrct.isna(),data.careerFtPrct,data.mvAvgFtPrct)
        data.mvAvgThrPtPrct = np.where(data.mvAvgThrPtPrct.isna(),data.careerThrPtPrct,data.mvAvgThrPtPrct)
        data.seasonUsage = np.where(data.seasonUsage.isna(),data.careerUsage,data.seasonUsage)
        data.seasonOffRating = np.where(data.seasonOffRating.isna(),data.careerOffRating,data.seasonOffRating)
        data.seasonFtPrct = np.where(data.seasonFtPrct.isna(),data.careerFtPrct,data.seasonFtPrct)
        data.seasonThrPtPrct = np.where(data.seasonThrPtPrct.isna(),data.careerThrPtPrct,data.seasonThrPtPrct)
        idx = data.index
        
        data = data.merge(lgAvgs,how='left',on=['season'])
        data.index = idx
        data.mvAvgOppPace = np.where(data.mvAvgOppPace.isna(),data.paceSeason,data.mvAvgOppPace)
        data.seasonOppPace = np.where(data.seasonOppPace.isna(),data.paceSeason,data.seasonOppPace)
        data.mvAvgOppOpen3 = np.where(data.mvAvgOppOpen3.isna(),data.open_fg3aSeason,data.mvAvgOppOpen3)
        data.seasonOppOpen3 = np.where(data.seasonOppOpen3.isna() ,data.open_fg3aSeason,data.seasonOppOpen3)
        data.seasonOppWide3 = np.where(data.seasonOppWide3.isna(),data.wide_fg3aSeason,data.seasonOppWide3)
        data.mvAvgOppWide3 = np.where(data.mvAvgOppWide3.isna(),data.wide_fg3aSeason,data.mvAvgOppWide3)
        data.mvAvgOppDefRating = np.where(data.mvAvgOppDefRating.isna(),data.def_rateSeason,data.mvAvgOppDefRating)
        data.seasonOppDefRating = np.where(data.seasonOppDefRating.isna(),data.def_rateSeason,data.seasonOppDefRating)
        data.drop(lgAvgs.columns,axis=1,inplace=True)
        data.fillna(0,inplace=True)
        return data


            

	
        