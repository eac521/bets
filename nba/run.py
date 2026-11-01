import logging
import os
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/nba_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import time
import subprocess
import sys
import argparse
import pandas as pd
import datetime as dt
import statsmodels.api as sm
import numpy as np

from nba import NBAbase, NBAetl, NBAdata, NBAmodels
etl = NBAetl.etl()
data = NBAdata.data()
from betting import funcs, odds_live
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
    if gids.empty:
        return logger.info('No games on {}, skipping data pull'.format(run_date))
    print('Updating for {}'.format(run_date))
    etl.update_player_log([run_date])
    time.sleep(np.random.randint(5,15))
    etl.update_shots_allowed([run_date])
    time.sleep(np.random.randint(5,15))
    etl.update_teamLog(gids.GAME_ID.unique())
    data.refresh_materialized_tables()


def fill_missing(days=7, today=None):
    '''
    Re-pulls any of the last `days` dates where a scheduled game is missing box scores or team logs, and fills Q1 logs.
    The daily run only pulls yesterday, so a date that failed both the 5:30 and 11:30 runs is otherwise never retried.
    A game still missing box scores 2+ days later is logged as a warning (postponed, or the source is down).
    inputs: number of days to look back, optional today (date) for testing
    output: list of dates re-pulled
    '''
    today = today or dt.date.today()
    start = (today - dt.timedelta(days=days)).strftime('%Y-%m-%d')
    end = (today - dt.timedelta(days=1)).strftime('%Y-%m-%d')
    gaps = etl.find_gaps(start, end)
    redo = sorted(gaps.loc[(gaps.box == 0) | (gaps.team == 0), 'd'].unique())
    for d in redo:
        logger.info('Re-pulling {}: {} game(s) missing box scores or team logs'.format(
            d, ((gaps.d == d) & ((gaps.box == 0) | (gaps.team == 0))).sum()))
        try:
            data_pull(d)
        except Exception:
            logger.exception('Re-pull failed for {}'.format(d))
    if ((gaps.q1 == 0) & (gaps.box == 1)).any() or redo:
        etl.update_period_log(sorted({etl.derive_season(d) for d in gaps.d}), qtr=1)
    track = etl.find_tracking_gaps(start, end)
    for d in track:  # one date at a time, so a date whose tracking isn't posted yet doesn't block the others
        logger.info('Pulling player tracking for {}'.format(d))
        try:
            etl.get_tracking_data([d])
        except Exception:
            logger.exception('Tracking pull failed for {}'.format(d))
    after = etl.find_gaps(start, end)
    stale = after[(after.box == 0) & (after.d <= (today - dt.timedelta(days=2)).strftime('%Y-%m-%d'))]
    for _, g in stale.iterrows():
        logger.warning('Game {} on {} still has no box score'.format(g.game_id, g.d))
    if not redo and stale.empty:
        logger.info('No missing games {} to {}'.format(start, end))
    return redo


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
    td = td[(td.game_date == date) & (td.eligible == 1)]
    if td.empty:
        return logger.info('No eligible players on {}, no predictions made'.format(date))
    td = model.standRobust_scaler(td)
    preds = model.model.predict(sm.add_constant(td.filter(model.features), has_constant='add'))
    idInfo = model.data[(model.data.game_date == date) & (model.data.eligible == 1)][['name','player_id','team','game_id']].copy()
    idInfo['name'] = data.standardize_names(idInfo['name'])
    df = od.oddsTable(idInfo[['name','player_id','team']].join(preds))
    preds['date'] = date
    preds['market'] = model.name
    final = idInfo.join(preds).melt(id_vars=['market', 'player_id', 'date'],
         value_vars=[col for col in preds if isinstance(col, int)],
         value_name='model_prob',
         var_name='number').round(6)
    df.rename(columns={'value':'model_prob'}, inplace=True)
    etl.insert_data(final,'predictions',sort=True)
    return df

#I dont know that this is needed because we are going to use run model 
# and then I dont want all the pieces connected here
def run_pipeline(model_name, run_date=None):
    try:
        data_pull(run_date)
    except Exception:
        # late games often aren't posted by the morning run; predict on what loaded and let the 11:30 retry fill in
        logger.exception('Data pull failed; refreshing tables and predicting on the data available')
        data.refresh_materialized_tables()
    if run_date is None:
        try:
            fill_missing()
        except Exception:
            logger.exception('Missing-game check failed')
    result = subprocess.run([sys.executable, '-m', 'pytest', 'tests/', '-m', 'not integration', '-q'], capture_output=True)
    if result.returncode != 0:
        logger.warning('Validation tests failed; continuing with predictions')
    lines = run_model(model_name, run_date)
    if run_date is None:
        # opening odds snapshot after the morning run; the 11:30 run skips it once today's open exists
        try:
            odds_live.pull_open(od)
        except Exception:
            logger.exception('Open odds pull failed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='threes')
    parser.add_argument('--date', default=None)
    parser.add_argument('--step', default='all', choices=['data', 'model', 'fill', 'all'])
    parser.add_argument('--days', type=int, default=7, help='lookback for --step fill')
    args = parser.parse_args()
    if args.step == 'data':
        data_pull(args.date)
    elif args.step == 'model':
        run_model(args.model, args.date)
    elif args.step == 'fill':
        print(fill_missing(args.days))
    else:
        run_pipeline(args.model, args.date)
