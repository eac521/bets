import pytest
import os
import pandas as pd
import datetime as dt
from nba.NBAetl import etl as NBAetl
from betting import funcs
import logging
logger = logging.getLogger(__name__)
data_dir = os.path.join(os.path.dirname(__file__), 'data')

@pytest.fixture(scope="module")
def etl():
    return NBAetl()
@pytest.fixture(scope="module")
def bo():
    return funcs.odds()

@pytest.mark.integration
def test_api_call(etl):
    '''
    Testing when we call the nba_api the needed columns are available
    '''
    yst = (dt.datetime.today() + pd.to_timedelta(-1,unit='day')).strftime(format='%Y-%m-%d')
    df = etl.get_open_shot_allowed([yst])
    required = ['TEAM_ID',"GAME_DATE","WIDEOPEN_FG3A","OPEN_FG3M"]
    assert all([a in df.columns for a in required])

@pytest.mark.integration
def test_no_null_targets(etl):
    '''
    Checking for null values in data for last n days at team level
    '''
    n=15
    end_date = (dt.datetime.today() + pd.to_timedelta(-2,unit='day')).strftime(format='%Y-%m-%d')
    start_date = (dt.datetime.today() + pd.to_timedelta(-12,unit='day')).strftime(format='%Y-%m-%d')
    df  = pd.read_sql('''SELECT game_id,team_id,wide_fg2a,wide_fg3m,open_fg3m,open_fg3a
                      FROM shotsAllowed WHERE game_date BETWEEN '{}' AND '{}' '''.format(start_date,end_date),
                      etl.conn)
    assert df.notna().all().all()

def test_preds_sum_100():
    '''
    Simple check to ensure our predicitons sum to 100% by row
    '''
    preds = pd.read_csv(os.path.join(data_dir, 'test_preds.csv'))
    #test each row sums to 100
    assert (abs(preds.sum(axis=1) - 1.0) < 1e-6).all()

def test_features():
    '''
    Checking to ensure all columns but daysBetweenGames get standardized properly
    '''
    feats =  pd.read_csv(os.path.join(data_dir, 'test_features.csv'))
    assert feats.drop(columns=['daysBetweenGames']).apply(lambda x: x.between(-5,5)).all().all()

def test_kelly_fraction_bounded(bo):
    """Kelly should never recommend betting > 100% of bankroll."""
    fraction = bo.kellyCrit(.5,200)
    assert 0 <= fraction <= 1.0

@pytest.mark.integration
def test_log_counts(etl):
    end_date = (dt.datetime.today() + pd.to_timedelta(-2, unit='day')).strftime(format='%Y-%m-%d')
    start_date = (dt.datetime.today() + pd.to_timedelta(-12, unit='day')).strftime(format='%Y-%m-%d')
    shots = pd.read_sql('''SELECT game_id,team_id
                       FROM shotsAllowed WHERE game_date BETWEEN '{}' AND '{}' '''.format(start_date, end_date),
                     etl.conn)
    team = pd.read_sql('''SELECT game_id,team_id
                       FROM teamLog WHERE game_date BETWEEN '{}' AND '{}' '''.format(start_date, end_date),
                     etl.conn)
    player = pd.read_sql('''SELECT distinct game_id,team_id
                       FROM plyrLogs WHERE game_date BETWEEN '{}' AND '{}' '''.format(start_date, end_date),
                     etl.conn)
    assert len(shots) == len(team) == len(player)