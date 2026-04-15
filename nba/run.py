import logging
logging.basicConfig(
    filename='nba_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
import datetime as dt
import time
import statsmodels.api as sm
import argparse
import pandas as pd
import numpy as np
from nba import NBAbase, NBAetl, NBAdata, NBAmodels
etl = NBAetl.etl()
data = NBAdata.data()
from betting import funcs
od = funcs.odds()

pipes = {'threes':data.threes_pipe}

def data_pull(run_date=None):
    '''
    Our Daily run to pull in all game data for date requested.  Typically the prior day but can be adjusted for missing data.
    this is for a single day update, run individual functions for bulk loading
    inputs: Optional date, defaults to yesterday
    Outputs: None; provides print statements with updates, logging on errors with tracking data and will refresh opponent data table.
    '''
    run_date = run_date or (dt.datetime.today() + pd.to_timedelta(-1, unit='day')).strftime(format='%Y-%m-%d')
    gids = etl.get_games(run_date,run_date)
    print('Updating for {}'.format(run_date))
    etl.update_player_log([run_date])
    time.sleep(np.random.randint(5,15))
    etl.update_shots_allowed([run_date])
    time.sleep(np.random.randint(5,15))
    etl.update_teamLog(gids.GAME_ID.unique())
    data.refresh_opp_data()


def run_model(model_name,date=None):
    '''
    Run function for the model, this will get the model and produce the predictions both in a long and wide format
    inputs: model name, date that defaults to today
    ouput: long and wide dataframes of predicitons for the day
    '''
    date = date or dt.datetime.today().strftime('%Y-%m-%d')
    model = NBAmodels.models(model_name)
    pipe = pipes.get(model_name)
    td = pipe(model.data)
    td = data.clean_na(td)
    td = td[td.game_date == date]
    td = model.standRobust_scaler(td)
    preds = model.model.predict(sm.add_constant(td.filter(model.features), has_constant='add'))
    idInfo = model.data[model.data.game_date == date][['name','team','game_id']]
    idInfo.name = data.standardize_names(idInfo.name)
    return od.oddsTable(preds, idInfo,od.market_vars.get(model_name).get('col_name')),idInfo


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', required=True)
    args = parser.parse_args()

    data_pull()
    overs, idInfo = run_model(args.model)
    odf = od.fetch_odds(args.model)
    final = od.bet_table(overs, odf, od.market_vars.get(args.model).get('col_name'))