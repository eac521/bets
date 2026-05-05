import logging
logging.basicConfig(
    filename='nba_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

import time
import subprocess
import argparse
import pandas as pd
import datetime as dt
import statsmodels.api as sm
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
    idInfo = model.data[model.data.game_date == date][['name','player_id','team','game_id']].copy()
    idInfo['name'] = data.standardize_names(idInfo['name'])
    df = od.oddsTable(preds, idInfo)
    df['date'] = date
    df['market'] = model.name
    etl.insert_data(df.rename(columns={"value":"model_lines"}),'predictions',sort=True)
    df.drop(['date','market'],axis=1,inplace=True)
    return df, idInfo

#I dont know that this is needed because we are going to use run model and then I dont want all the pieces connected here
def run_pipeline(model_name):
    data_pull()
    result = subprocess.run(['pytest', 'tests/', '-v'], capture_output=True)
    if result.returncode != 0:
        logger.error('Tests failed — skipping predictions\n{}'.format(result.stdout.decode()))
        return None
    lines, idInfo = run_model(model_name)
    odf = od.fetch_odds(model_name)
    final = od.bet_table(lines, odf)
    return final

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', required=True)
    args = parser.parse_args()
    run_pipeline(args.model)
