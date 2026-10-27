'''
Scheduled live odds snapshots (threes, points, PRA; main and alternate; 10 books).
  open    first pull of the day, run by run_pipeline after the morning model run (skipped if today's open exists)
  midday  14:00 ET, same time as the historical backfill's midday pull
  close   checked every 5 minutes; pulls each game once when its tip is 15-25 minutes away
Manual pulls come from the app's Refresh Lines button (pull_sequence = manual).
Files: nba/data/odds_live/{season}/{pull_sequence}/{date}_{HHMMSS}.parquet

python -m betting.odds_live --pull midday
python -m betting.odds_live --pull close
'''
import os
os.makedirs('logs', exist_ok=True)
import logging
logging.basicConfig(
    filename='logs/odds_live.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
import argparse
import pandas as pd

from betting import funcs
from betting.funcs import ET


def pull_open(od, force=False):
    if not force and not od.read_snapshots(pull_sequence='open').empty:
        return logger.info('Open already pulled today, skipping')
    return od.pull_snapshot('open')


def pull_close(od, lo=15, hi=25):
    done = od.read_snapshots(pull_sequence='close')
    done = set(done.event_id) if not done.empty else set()
    now = pd.Timestamp.now(tz=ET)
    due = lambda events: [e for e in events
                          if lo < (pd.Timestamp(e['commence_time']) - now).total_seconds() / 60 <= hi
                          and e['id'] not in done]
    return od.pull_snapshot('close', select=due)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--pull', required=True, choices=['open', 'midday', 'close'])
    parser.add_argument('--force', action='store_true', help='pull open even if today already has one')
    args = parser.parse_args()
    od = funcs.odds()
    try:
        if args.pull == 'open':
            pull_open(od, args.force)
        elif args.pull == 'midday':
            od.pull_snapshot('midday')
        else:
            pull_close(od)
    except Exception:
        logger.exception('{} odds pull failed'.format(args.pull))
        raise
