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
                'model_path': '../nba/data/model/2025-26Run/threeModel.pkl',
                'scaler_path': '../nba/data//model/2025-26Run/scaler.pkl',
                'data_path': '../nba/data/sql/threeRunQ.sql',
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
        self.features = self.model.params.index.tolist()



    def load(self, path):
        """simple logic to load file"""
        with open(path, 'rb') as f:
            return pickle.load(f)

    ##### Testing Metrics #####

    def brier_scores(self,actuals, preds, cumlative=False):
        '''Will create a brier score to compare models
        Inputs: an array of actuals one hot encoded, a data frame of predictions from model
        Outputs: score as a float
        '''
        if cumlative:
            c = sorted(preds.columns, reverse=True)
            preds = preds.filter(c).cumsum(axis=1).filter(preds.columns)
            bscore = (preds.sub(self.ohe_overs(actuals)) ** 2).sum(axis=1).mean()
        else:
            bscore = (preds.sub(self.ohe_actuals(actuals)) ** 2).sum(axis=1).mean()
        print('{}brier score of:  {:.3f}'.format('cumlative ' if cumlative else '', bscore))
        return bscore



    ##### model runs #####
    def model_data(self, trainData, startDate,endDate, yCol, yMax = None):
        '''
        This will do the preprocessing needed to train and test the threes Model#this#
        Inputs: Complete training dataset as a dataframe, date to split the data
        Output: X train, y train, X test, y test
        '''
        ##getting the splits based on time
        yst = '2025-10-01'
        X = trainData[trainData.game_date.between(startDate, endDate)]
        Xtest = trainData[trainData.game_date.between(endDate, yst)]
        y = trainData[trainData.game_date.between(startDate, endDate)][yCol].values
        yTest = Xtest[Xtest.game_date.between(endDate, yst)][yCol].values
        if yMax is not None:
            y = [yMax if val>=yMax else val for val in y]
            yTest = [yMax if val>=yMax else val for val in yTest]
        else:
            pass


        return X, y, Xtest, yTest


    def standRobust_scaler(self, df, scaler=None):
        '''
        Will do a standard scaling on your data based on a dictionary provided that has the features, mean and std for each feature.
        Inputs: DataFrame and dictionary
        Output: New scaled DataFrame
        '''
        for col in self.features[1:]:
            try:
                df[col] = (df[col] - self.scaler.get(col).get('center')) / self.scaler.get(col).get('var')
            except AttributeError:
                pass
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

