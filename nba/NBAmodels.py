import pickle
import pandas as pd
import numpy as np
import datetime as dt
from .NBAbase import base
from .NBAdata import data

## to be instantiated for each model individually
class models(base):
    def __init__(self,mod_name):
        super().__init__()
        models_configs = {
            'threes': {
                'model_path': '../nba/data/model/ThrMod2.pkl',
                'scaler_path': '../nba/data/model/scalValsThrees.pkl',
                'data_path': '../nba/data/sql/thrTesting.sql'
            },
            'points': {
                'model_path': '../nba/data/model/pointsModel.pkl',
                'scaler_path': '../nba/data/model/scalValsPoints.pkl',
                'data_path': '../nba/data/sql/query.sql'
            },
            'spread': {
                'model_path': '../nba/data/model/spreadModel.pkl',
                'scaler_path': '../nba/data/model/scalValsSpread.pkl',
                'data_path': '../nba/data/sql/query.sql'
            }
        }

        self.name = mod_name
        self.data = pd.read_sql(open(models_configs[self.name]['data_path'],'r').read(),self.conn)
        self.scaler = self.load(models_configs[self.name]['scaler_path'])
        self.model = self.load(models_configs[self.name]['model_path'])
        self.



    def load(self, path):
        """simple logic to load file"""
        with open(path, 'rb') as f:
            return pickle.load(f)

    ##### Testing Metrics #####
    @staticmethod
    def brier_scores(actuals, preds, cumlative=False):
        '''Will create a brier score to compare models
        Inputs: an array of actuals one hot encoded, a data frame of predictions from model
        Outputs: score as a float
        '''
        if cumlative:
            c = sorted(preds.columns, reverse=True)
            preds = preds.filter(c).cumsum(axis=1).filter(preds.columns)
            bscore = (preds.sub(models.oheOvers(actuals)) ** 2).sum(axis=1).mean()
        else:
            bscore = (preds.sub(models.oheActuals(actuals)) ** 2).sum(axis=1).mean()
        print('{}brier score of:  {:.3f}'.format('cumlative ' if cumlative else '', bscore))
        return bscore



    ##### model runs #####
    def model_data(self,model, trainData, splitDate, yCol, yMax = None):
        '''
        This will do the preprocessing needed to train and test the threes Model
        Inputs: Complete training dataset as a dataframe, date to split the data
        Output: X train, y train, X test, y test
        '''
        ##getting the splits based on time
        yst = (dt.datetime.today() + pd.to_timedelta(-1, unit='day')).strftime(format='%Y-%m-%d')
        trainData = getattr(data,'{}_pipe'.format(model.name))(df = trainData)
        X = trainData[trainData.game_date.between(min(trainData.game_date), splitDate)]
        Xtest = trainData[trainData.game_date.between(splitDate, yst)]
        y = trainData[trainData.game_date.between(min(trainData.game_date), splitDate)][yCol].values
        yTest = Xtest[Xtest.game_date.between(splitDate, yst)][yCol].values
        if yMax is not None:
            y = [yMax if val>=yMax else val for val in y]
            yTest = [yMax if val>=yMax else val for val in yTest]
        else:
            pass
        ##data preprocessing based on model
        X = getattr(model,'clean_na_{}'.format(model.name))(X)
        Xtest = getattr(model,'clean_na_{}'.format(model.name))(Xtest)

        X = self.stand_scaler(X,model)
        Xtest = self.stand_scaler(Xtest,model)

        return X, y, Xtest, yTest

    def clean_na_threes(self, df):
        '''
        specifically to replace nans in the threes dataset
        Inputs: the completed Threes data set with distibuition information
        Ouput: dataframe removing nans
        '''
        lgAvgs = pd.read_sql('''select season,
                                       sum(open_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga)) open_fg3aSeason,
                                       sum(wide_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga)) wide_fg3aSeason,
                                       avg(pace)     as                                                  paceSeason,
                                       avg(def_rate) as                                                  def_rateSeason
                                from team_def
                                group by season
                                HAVING season is not Null
                             ''', self.conn).set_index('season').reset_index().shift()

        df.loc[:,'mvAvgThrees'] = df['mvAvgThrees'].fillna(df['careerAvgThrees'])
        df.loc[:,'mvAvgUsage'] = df['mvAvgUsage'].fillna(df['careerUsage'])
        df.loc[:,'mvAvgOffRating'] = df['mvAvgOffRating'].fillna(df['careerOffRating'])
        df.loc[:,'mvAvgFtPrct'] = df['mvAvgFtPrct'].fillna(df['careerFtPrct'])
        df.loc[:,'mvAvgThrPtPrct'] = df['mvAvgThrPtPrct'].fillna(df['careerThrPtPrct'])
        df.loc[:,'seasonUsage'] = df['seasonUsage'].fillna(df['careerUsage'])
        df.loc[:,'seasonOffRating'] = df['seasonOffRating'].fillna(df['careerOffRating'])
        df.loc[:,'seasonFtPrct'] = df['seasonFtPrct'].fillna(df['careerFtPrct'])
        df.loc[:,'seasonThrPtPrct'] = df['seasonThrPtPrct'].fillna(df['careerThrPtPrct'])
        idx = df.index

        df = df.merge(lgAvgs, how='left', on=['season'])
        df.index = idx
        df['mvAvgOppPace'] = df['mvAvgOppPace'].fillna(df['paceSeason'])
        df['seasonOppPace'] = df['seasonOppPace'].fillna(df['paceSeason'])
        df['mvAvgOppOpen3'] = df['mvAvgOppOpen3'].fillna(df['open_fg3aSeason'])
        df['seasonOppOpen3'] = df['seasonOppOpen3'].fillna(df['open_fg3aSeason'])
        df['seasonOppWide3'] = df['seasonOppWide3'].fillna(df['wide_fg3aSeason'])
        df['mvAvgOppWide3'] = df['mvAvgOppWide3'].fillna(df['wide_fg3aSeason'])
        df['mvAvgOppDefRating'] = df['mvAvgOppDefRating'].fillna(df['def_rateSeason'])
        df['seasonOppDefRating'] = df['seasonOppDefRating'].fillna(df['def_rateSeason'])
        df.drop(lgAvgs.columns, axis=1, inplace=True)
        df.fillna(0, inplace=True)
        return df


    ## simple calculations for the brier score above
    @staticmethod
    def ohe_actuals(actuals):
        mx = max(actuals)
        return np.array([[0] * y + [1] + [0] * (mx - y) for y in actuals])

    @staticmethod
    def ohe_overs(actuals):
        mx = max(actuals)
        return np.array([[1] * (y + 1) + [0] * (mx - y) for y in actuals])