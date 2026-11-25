'''
Shadow threes model: production's pipeline and eligibility with its own model and scaler, fit with crn_interaction /
abv_interaction in place of crn_fgaMv / abv_fgaMv (model files in nba/data/model/2026-27Run_interaction, fit in the
retrain notebook). Writes the predictions table's long format (market, player_id, date, number, model_prob) to
nba/data/shadow/threes_interaction/{date}.parquet; a later run the same day overwrites it, like the table. Never writes
to the database.

    python -m nba.shadow                                        # today
    python -m nba.shadow --start 2026-10-20 --end 2026-11-23    # backfill
'''
import argparse
import datetime as dt
import logging
import os
import pickle

import pandas as pd
import statsmodels.api as sm

from . import NBAmodels
from .NBAdata import data as Data
from .constants import _BASE_DIR

logger = logging.getLogger(__name__)
MARKET = 'threes_interaction'   # its own market, so the rows could load into `predictions` later without colliding
MODEL_DIR = os.path.join(_BASE_DIR, 'data', 'model', '2026-27Run_interaction')
OUT_DIR = os.path.join(_BASE_DIR, 'data', 'shadow', MARKET)


def run_shadow(dates=None):
    '''Shadow PMFs for each date (default: today). Output: list of files written.'''
    dates = dates or [dt.datetime.today().strftime('%Y-%m-%d')]
    data, prod = Data(), NBAmodels.models('threes')
    with open(os.path.join(MODEL_DIR, 'threeModel.pkl'), 'rb') as f:
        model = pickle.load(f)
    with open(os.path.join(MODEL_DIR, 'scaler.pkl'), 'rb') as f:
        scaler = pickle.load(f)
    feats = model.params.index.tolist()
    td = data.clean_na(data.threes_pipe(prod.data))
    td = td[td.game_date.isin(dates) & (td.eligible == 1)].copy()
    for col in feats[1:]:
        if col in scaler:
            td[col] = (td[col] - scaler[col]['center']) / scaler[col]['var']
    os.makedirs(OUT_DIR, exist_ok=True)
    written = []
    for date, day in td.groupby('game_date'):
        p = model.predict(sm.add_constant(day.filter(feats), has_constant='add')).to_numpy()
        pm = pd.DataFrame(p, columns=range(p.shape[1])).assign(player_id=day.player_id.values)
        out = pm.melt(id_vars='player_id', var_name='number', value_name='model_prob')
        out = out.assign(market=MARKET, date=date, number=out.number.astype(float))[['market', 'player_id', 'date', 'number', 'model_prob']].round(6)
        path = os.path.join(OUT_DIR, f'{date}.parquet')
        out.to_parquet(path, index=False)
        written.append(path)
        logger.info(f'Shadow {MARKET} {date}: {day.player_id.nunique()} players')
    if not written:
        logger.info(f'Shadow {MARKET}: no eligible players for {dates[0]} to {dates[-1]}')
    return written


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    ap = argparse.ArgumentParser()
    ap.add_argument('--start')
    ap.add_argument('--end')
    a = ap.parse_args()
    run_shadow([d.strftime('%Y-%m-%d') for d in pd.date_range(a.start, a.end or a.start)] if a.start else None)
