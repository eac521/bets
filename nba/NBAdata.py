import pandas as pd
import numpy as np
import time
import re
from tqdm import tqdm
from .NBAbase import base

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

    ### Data change / updates

    def change_games(self, gid, newDate):
        self.conn.execute("UPDATE teamLog SET game_date = '{}'  WHERE game_id = '{}'".format(newDate, gid))
        self.conn.execute("UPDATE plyrLogs SET game_date = '{}' WHERE game_id = '{}'".format(newDate, gid))
        self.conn.execute("UPDATE shotsAllowed SET game_date = '{}' WHERE game_id = '{}'".format(newDate, gid))
        self.conn.commit()
        print('Game updated')

    def update_schedule(self, adds):
        '''
		Give a data frame with the game_date, game_id, team_id and home, then this will update the schedule to change the game for those teams
		'''
        tcols = pd.read_sql('select * from teamLog limit 1', self.conn).columns
        # adds = season[(~season.game_id.isin(gids.GAME_ID)) & (season.game_date<=today)]
        for col in tcols:
            if col not in adds.columns:
                adds[col] = None
        self.insert_data(adds.filter(tcols), 'teamLog')
        # updating playerlogs
        pcols = pd.read_sql('select * from plyrLogs limit 1', self.conn).columns
        plyrs = pd.read_sql("select teamId as team_id, playerId as player_id from rosters where endDate is Null ",
                            self.conn)
        df = adds.merge(plyrs, how='left', on=['team_id'])
        for col in pcols:
            if col not in df:
                df[col] = None
        self.insert_data(df.filter(pcols), 'plyrLogs')
        # updating shotsallowed
        saCols = pd.read_sql('select * from shotsAllowed limit 1', self.conn).columns
        # sadf = season[(~season.game_date.isin(saGames)) & (season.game_date<=today)]
        for col in saCols:
            if col not in adds:
                adds[col] = 0
        self.insert_data(adds.filter(saCols), 'shotsAllowed')

    def trade_update(self, df):
        '''dataframe with player_id,name, trade_date and new_team.
		Will update rosters with the new players and print out a statement stating how many rows have been changed for each player
		Inputs: dataframe; with columns: player_id, newTeam, tradeDate
		Output: None, print statement for update
		'''
        date = df.tradeDate.unique()[0]
        pids = ','.join(df.player_id.unique())
        self.cur.execute("DELETE FROM plyrLogs WHERE player_id in ({})  and game_date >= '{}' ".format(pids, date))
        self.conn.commit()
        # update player's schedule with new team
        lst = df.filter(['player_id', 'newTeam', 'tradeDate']).values
        for l in lst:
            q = "select '{}' as player_id,team_id,game_date,game_id from teamLog where team_id = '{}' and game_date > '{}'".format(
                *l)
            new = pd.read_sql(q, self.conn)
            pcols = pd.read_sql('select * from plyrLogs limit 1', self.conn).columns
            for col in pcols:
                if col not in new.columns:
                    new[col] = None
            self.insert_data(new.filter(pcols), 'plyrLogs')

    def threes_pipe(self,df):
        '''Will generate the data needed to generate predictions for threes
		Inputs: str for game date formatted as YYYY-MM-DD
		Output: DataFrame of your X values
		'''
        #team defense needs to be done here for all moving averages/coeff vars as it moves to player level after this.
        tmsa = self.rolling_team_sa()
        tmsa = tmsa.join(data.weighted_moving_avg(tmsa, 5, 15, 'crn_fgallowed', 'opp_id'))
        tmsa = tmsa.join(data.weighted_moving_avg(tmsa,5,15,'abv_fgallowed','opp_id'))
        tmsa = tmsa.join(data.rolling_coeffecient_var(tmsa,5,15,'wide_fg3allowed','opp_id'))
        plsh = self.rolling_player_shot(df)
        final = df.merge(plsh, how='left', on=['player_id', 'game_date']).merge(tmsa, how='left',
                                                                                 on=['game_date', 'opp_id'])
        final['abv_kurtSkew'] = final.abv_fgakurt * final.abv_fgaskew
        final['crn_kurtSkew'] = final.crn_fgakurt * final.crn_fgaskew
        final = final.join(data.weighted_moving_avg(final, 5, 15, 'crn_fga', 'player_id'))
        final = final.join(data.weighted_moving_avg(final, 5, 15, 'abv_fga', 'player_id'))
        final = final.join(data.rolling_coeffecient_var(final,5,15,'minFirst','player_id'))
        final = final.join(data.rolling_coeffecient_var(final, 5, 15, 'abvFgaFirst', 'player_id'))
        final = final.join(data.rolling_coeffecient_var(final, 5, 15, 'crnFgaFirst', 'player_id'))

        # dropping the fga columns as they have been used for shots and shots allowed
        final.drop(['ra_fga', 'mid_fga', 'paint_fga'], axis=1, inplace=True)
        # fill na for first game/opp game of season, adding in a 9 as this is similar to the all-star break
        final.daysBetweenGames.fillna(9, inplace=True)
        final.oppDaysLastGame.fillna(9, inplace=True)
        # final = final[final.game_date==date]
        return final

    def rolling_team_sa(self):
        '''
		Setting up a rolling distribution of each shot location that each team allows.
		Inputs: dataframe with team_id, game_date and at least one fgallowed column
		output: DataFrame with six percentiles for each fgallowed column
		'''
        sa = ''' \
             SELECT team_id                                   as opp_id, \
                    game_date, \
                    ra_fga                                    as ra_fgallowed, \
                    paint_fga                                 as paint_fgallowed, \
                    mid_fga                                   as mid_fgallowed, \
                    coalesce(lc_fga, 0) + coalesce(rc_fga, 0) as crn_fgallowed, \
                    abv_fga                                   as abv_fgallowed,
                     wide_fg3a                                as wide_fg3allowed,\
                    season \
             from team_def \
             order by opp_id, game_date \
			 '''
        df = pd.read_sql(sa, self.conn)
        # quants = ([1/6,1/3,.5,2/3,5/6,1],['st','nd','rd','rth','fth','xth'])
        shots = [col for col in df.columns if re.search('_fgallowed$', col) != None]
        pdf = df[['opp_id', 'game_date','wide_fg3allowed','crn_fgallowed','abv_fgallowed']]
        for col in shots:
            skew = df.groupby(['opp_id'])[col].rolling(15, closed='left', min_periods=5).skew()
            kurt = df.groupby(['opp_id'])[col].rolling(15, closed='left', min_periods=5).kurt()
            pdf = pdf.join(skew.reset_index(name='{}skew'.format(col)).set_index('level_1').drop('opp_id', axis=1))
            pdf = pdf.join(kurt.reset_index(name='{}kurt'.format(col)).set_index('level_1').drop('opp_id', axis=1))

        return pdf

    def rolling_player_shot(self, df):
        '''
		Setting up a rolling distribution of each shot location for player.  This will take in the threes query
		Inputs: dataframe with player_id, game_date and at least one fga column
		output: DataFrame with six percentiles for each fga column
		'''
        # quants = ([1/6,1/3,.5,2/3,5/6,1],['st','nd','rd','rth','fth','xth'])
        shots = [col for col in df.columns if re.search('_fga$', col) != None]

        pdf = df[['game_date', 'player_id']]
        for col in shots:
            skew = df.groupby(['player_id'])[col].rolling(15, closed='left', min_periods=5).skew()
            kurt = df.groupby(['player_id'])[col].rolling(15, closed='left', min_periods=5).kurt()
            pdf = pdf.join(skew.reset_index(name='{}skew'.format(col)).set_index('level_1').drop('player_id', axis=1))
            pdf = pdf.join(kurt.reset_index(name='{}kurt'.format(col)).set_index('level_1').drop('player_id', axis=1))

        return pdf

    def naInfo(self, df, uniqueCol=None):
        naCols = df.columns[df.isna().any()].tolist()
        print('{} columns have nans'.format(len(naCols)))
        for col in naCols:
            if uniqueCol != None:
                print("{}:\nmissing:{:,}-{:.2%}\nunique:{:,}-{:.2%}\n\t".format(col,
					df[col].isna().sum(),
					df[col].isna().sum() / len(df[col]),
					df[df[col].isna()][uniqueCol].nunique(),
					df[df[col].isna()][
						uniqueCol].nunique() / df[
						col].isna().sum()))
            else:
                print("{}:\nmissing:{:,}-{:.2%}".format(col, df[col].isna().sum()))
        return naCols
#General functions for different transformations
    @staticmethod
    def weighted_moving_avg( df, min, periods, col, grping):
        '''
		Is an exponential moving average of a grouped feature
		Inputs: dataframe, minimum number of periods to cover, typical number of periods, Column for exp weighting and the group by
		Output: Dataframe with new column
		'''
        mvgAvg = df.groupby(grping)[col].rolling(periods, closed='left', min_periods=min,
                                                   win_type='exponential').mean()
        return mvgAvg.reset_index(name='{}Mv'.format(col)).set_index('level_1').drop([grping], axis=1)
    @staticmethod
    def rolling_coeffecient_var(df,min,periods,col,grping):
        '''
        Create a rolling coeffecient of variation grouped feature
        Inputs: dataframe, minimum number of periods to cover, typical number of periods, Column for exp weighting and the group by
        Output: Dataframe with new column
        '''
        df = df.fillna(0)
        coefVar = (df.groupby(grping)[col].rolling(periods, closed='left', min_periods=min,
                                               win_type='exponential').std()
            /df.groupby(grping)[col].rolling(periods, closed='left', min_periods=min,
                                                   win_type='exponential').mean())
        return coefVar.reset_index(name='{}coefVar'.format(col)).set_index('level_1').drop([grping], axis=1)

    @staticmethod
    def stand_scaler(X,scaler=None):
        '''
        Will do a standard scaling on your data based on a dictionary provided that has the features, mean and std for each feature.
        Inputs: DataFrame and dictionary
        Output: New scaled DataFrame
        '''
        for col in scaler.keys():
           X[col] = (X[col] - scaler.get(col).get('center')) / scaler.get(col).get('scale')
        return X