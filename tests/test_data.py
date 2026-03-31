import pytest
import os
import pandas as pd
import datetime as dt
from nba.NBAetl import etl as NBAetl
from betting import funcs
data_dir = os.path.join(os.path.dirname(__file__), 'data')

bo = funcs.odds()
@pytest.mark.integration
def test_api_call():
    '''
    Testing when we call the nba_api the needed columns are available
    '''
    etl = NBAetl()
    yst = (dt.datetime.today() + pd.to_timedelta(-1,unit='day')).strftime(format='%Y-%m-%d')
    df = etl.update_shots_allowed([yst])
    required  = [col for col in pd.read_sql('select * from shotsAllowed limit 1',etl.conn).columns]
    assert set(required) == set(df.columns)

def test_no_null_targets():
    '''
    Checking for null values in data for last n days at team level
    '''
    n=10
    etl = NBAetl()
    end_date = (dt.datetime.today() + pd.to_timedelta(-1,unit='day')).strftime(format='%Y-%m-%d')
    start_date = (dt.datetime.today() + pd.to_timedelta(-n,unit='day')).strftime(format='%Y-%m-%d')
    df  = pd.read_sql("select * from teamLog where game_date between '{}' AND '{}' ".format(start_date,end_date), etl.conn)
    assert df.notna().all().all()

def test_preds_sum_100():
    '''
    Simple check to ensure our predicitons sum to 100% by row
    '''
    preds = pd.read_csv(os.path.join(data_dir, 'test_preds.csv'))
    #test each row sums to 100
    assert (preds.sum(axis=1) - 1.0 < 1e-6).all()

def test_features():
    '''
    Checking to ensure all columns but daysBetweenGames get standardized properly
    '''
    feats =  pd.read_csv(os.path.join(data_dir, 'test_features.csv'))
    assert feats.drop(columns=['daysBetweenGames']).apply(lambda x: x.between(-5,5)).all().all()

def test_kelly_fraction_bounded():
    """Kelly should never recommend betting > 100% of bankroll."""
    fraction = bo.kellyCrit(.5,200)
    assert 0 <= fraction <= 1.0
