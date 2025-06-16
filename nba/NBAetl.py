import pandas as pd
import numpy as np
import time
import re
from tqdm import tqdm
from .NBAbase import base
import requests
from nba_api.stats.endpoints import (
	BoxScoreAdvancedV3, PlayByPlayV2, BoxScoreSummaryV2,
	LeagueDashTeamShotLocations, LeagueDashOppPtShot,
	LeagueDashPlayerShotLocations, PlayerGameLogs, TeamInfoCommon,
	leaguegamefinder, LeagueDashPtStats, PlayerIndex,
	CommonPlayerInfo, PlayerAwards, GameRotation, LeagueDashPlayerPtShot
)

class etl(base):
	
	def __init__(self):
		super().__init__()
		
	def update_player_log(self, game_dates, seasons=None, insert=True, qtr=0):
		'''Pull in prior days game log information for each player.'''
		# get the individual dataframes
		strD = ','.join(["'{}'".format(d) for d in game_dates])
		log = self.get_logs(game_dates, seasons, qtr)
		bskt = self.get_first_buckets(game_dates)
		rbs = self.get_rebounds(game_dates, qtr)
		adv = self.get_advanced_box(game_dates, qtr)
		shts = self.get_player_shot_spots(game_dates, qtr)
		
		# merge dataframes together
		logrbs = log.merge(rbs, how='left', on=['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'GAME_DATE']).fillna(0)
		logRbsSht = logrbs.merge(shts, how='left', on=['TEAM_ID', 'PLAYER_ID', 'GAME_DATE'])
		advBskt = adv.merge(bskt, how='left', on=['PLAYER_ID', 'GAME_ID'])
		
		# final dataframe
		final = logRbsSht.merge(advBskt, how='left', on=['PLAYER_ID', 'GAME_ID'])
		# ... rest of method
		
	def get_rebounds(self, game_dates, qtr=0):
		'''Get rebounding information at the player level'''
		print('started rebounds at {}'.format(time.strftime('%H:%M')))
		minDate, maxDate = min(game_dates), max(game_dates)
		games = self.get_games(minDate, maxDate)
		# ... rest of method