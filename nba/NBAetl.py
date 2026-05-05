import pandas as pd
import numpy as np
import time
import re
from tqdm import tqdm
import requests
from itertools import permutations
import random
from requests.exceptions import HTTPError
import logging
logger = logging.getLogger(__name__)
from nba_api.stats.endpoints import (
	BoxScoreAdvancedV3, PlayByPlayV3, BoxScoreSummaryV2,
	LeagueDashTeamShotLocations, LeagueDashOppPtShot,
	LeagueDashPlayerShotLocations, PlayerGameLogs, TeamInfoCommon,
	leaguegamefinder, LeagueDashPtStats, PlayerIndex,
	CommonPlayerInfo, PlayerAwards, GameRotation, LeagueDashPlayerPtShot
)
from nba_api.live.nba.endpoints import BoxScore
from .NBAbase import base
class etl(base):
	
	def __init__(self):
		super().__init__()


	def reload_player_log(self, game_dates, seasons, qtr=0):
		'''This output what is the entire df for plyrlogs, can then be inserted if needed
		Need to have a season to get the
		'''
		log = pd.DataFrame()
		adv = pd.DataFrame()
		shts = pd.DataFrame()
		rbs = pd.DataFrame()
		for season in seasons:
			reg = PlayerGameLogs(season_nullable=season).get_data_frames()[0]
			time.sleep(30)
			aTemp = PlayerGameLogs(season_nullable=season, measure_type_player_game_logs_nullable='Advanced').get_data_frames()[0]
			log = pd.concat([log, reg])
			adv = pd.concat([adv, aTemp])
        
		logrbs = log.merge(rbs, how='left', on=['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'GAME_DATE']).fillna(0)
		logRbsSht = logrbs.merge(shts, how='left', on=['TEAM_ID', 'PLAYER_ID', 'GAME_DATE'])
		advBskt = adv.merge(fbckt, how='left', on=['PLAYER_ID', 'GAME_ID'])
		final = logRbsSht.merge(advBskt, how='left', on=['PLAYER_ID', 'GAME_ID'])
		final['Starter'] = 0

		final.columns = ['player_id', 'team_id', 'game_id', 'game_date', 'min', 'ftm', 'fta', 'reb', 'ast', 'tov',
						 'stl', 'blk', 'blka', 'pf',
						 'pfd', 'pts', 'plus_minus', 'dd2', 'td3', 'oreb', 'oreb_contest', 'oreb_chances',
						 'oreb_chance_defer', 'avg_oreb_dist', 'dreb',
						 'dreb_contest', 'dreb_chances', 'dreb_chance_defer', 'avg_dreb_dist', 'ra_fgm', 'ra_fga',
						 'paint_fgm', 'paint_fga', 'mid_fgm',
						 'mid_fga', 'lc_fgm', 'lc_fga', 'rc_fgm', 'rc_fga', 'abv_fgm', 'abv_fga', 'offensiveRating',
						 'defensiveRating',
						 'usagePercentage', 'pace', 'possessions', 'team_first', 'game_first', 'Starter']


	def update_player_log(self, game_dates, seasons=None, insert=True, qtr=0):
		'''Pull in prior days game log information for each player.  
		Will pull in the log, first basket, rebounds and shooting stats for each player.
		Inputs: list of game_dates, optionally can add the season to pull if many dates
		Output: DataFrame with player logs, first buckets, rebounds, shooting locations and advanced stats, also will message plyrLogs updated.'''
		#get the individual dataframes
		table = 'plyrLogs' if qtr == 0 else 'plyrQ{}Logs'.format(qtr)
		strD = ','.join(["'{}'".format(d) for d in game_dates])
		log = self.get_logs(game_dates,seasons) #has all 4
		bskt = self.get_first_buckets(game_dates) #playerid,gameid
		time.sleep(np.random.randint(1,15))
		rbs = self.get_rebounds(game_dates) #all 4
		adv = self.get_advanced_box(game_dates) # gameid,playerid
		time.sleep(np.random.randint(1,15))
		shts = self.get_player_shot_spots(game_dates) #has playerid,gamedate,teamid
		#merge dataframes together
		logrbs = log.merge(rbs,how='left',on=['PLAYER_ID','GAME_ID','TEAM_ID','GAME_DATE']).fillna(0)
		logRbsSht = logrbs.merge(shts,how='left',on=['TEAM_ID','PLAYER_ID','GAME_DATE'])
		advBskt = adv.merge(bskt,how='left',on = ['PLAYER_ID','GAME_ID'])
		#final dataframe
		final = logRbsSht.merge(advBskt,how='left',on=['PLAYER_ID','GAME_ID'])
		final.columns = ['player_id','team_id','game_id','game_date','min','ftm','fta','reb','ast','tov','stl','blk','blka','pf',
			'pfd','pts','plus_minus','dd2','td3','oreb','oreb_contest','oreb_chances','oreb_chance_defer','avg_oreb_dist','dreb',
			'dreb_contest','dreb_chances','dreb_chance_defer','avg_dreb_dist','ra_fgm','ra_fga', 'paint_fgm', 'paint_fga','mid_fgm',
			'mid_fga', 'lc_fgm','lc_fga', 'rc_fgm','rc_fga','abv_fgm', 'abv_fga', 'offensiveRating','defensiveRating',
			'usagePercentage', 'pace', 'possessions','Starter','team_first', 'game_first']
		final = final.filter(pd.read_sql('select * from plyrLogs limit 1',self.conn).columns.values)
		if (pd.read_sql("select count(*) as ct  from {} where game_date in ({})".format(table,strD),self.conn).sum()>0).all():
			self.conn.execute("DELETE FROM {} where game_date in ({})".format(table,strD))
			self.conn.commit()
		self.insert_data(final,table)
		if np.random.randint(0,100) % 7==0:
			time.sleep(np.random.randint(30,120))
		#return final
		
	def update_teamLog(self,game_ids):
		strD = ','.join(["'{}'".format(d) for d in game_ids])
		df = self.get_summary(game_ids)
		if (pd.read_sql("select count(*) as ct  from teamLog where game_id in ({})".format(strD),self.conn).sum()>0).all():
			self.conn.execute("DELETE FROM teamLog where game_id in ({})".format(strD))
			self.conn.commit()
		
		self.insert_data(df,'teamLog')

	def update_shots_allowed(self,game_dates):
		'''Pull in prior day's data for team's shot types allowed. 
		Will need the data and will use the get_opp_dribble_shots and get_opp_op_shot,
		merge them and insert the new data in the database
		Inputs: game dates as a list
		Output: A message stating that the shots allowed data in the database has been updated.
		'''
		strD = ','.join(["'{}'".format(d) for d in game_dates])
		sqlOrd = ['TEAM_ID', 'GAME_DATE', 'GAME_ID', 'Restricted_Area_OPP_FGM', 'Restricted_Area_OPP_FGA',
			'In_The_Paint_(Non_RA)_OPP_FGM', 'In_The_Paint_(Non_RA)_OPP_FGA',
			'Mid_Range_OPP_FGM', 'Mid_Range_OPP_FGA', 'Left_Corner_3_OPP_FGM',
			'Left_Corner_3_OPP_FGA', 'Right_Corner_3_OPP_FGM',
			'Right_Corner_3_OPP_FGA', 'Above_the_Break_3_OPP_FGM',
			'Above_the_Break_3_OPP_FGA', 'Corner_3_OPP_FGM', 'Corner_3_OPP_FGA', 'WIDEOPEN_FG2M', 'WIDEOPEN_FG2A',
			'WIDEOPEN_FG3M', 'WIDEOPEN_FG3A', 'OPEN_FG2M', 'OPEN_FG2A', 'OPEN_FG3M', 'OPEN_FG3A',
			'0_Dribbles_FG2M', '0_Dribbles_FG2A', '0_Dribbles_FG3M',
			'0_Dribbles_FG3A', '1_Dribble_FG2M', '1_Dribble_FG2A', '1_Dribble_FG3M',
			'1_Dribble_FG3A', '2_Dribbles_FG2M', '2_Dribbles_FG2A',
			'2_Dribbles_FG3M', '2_Dribbles_FG3A', '3-6_Dribbles_FG2M',
			'3-6_Dribbles_FG2A', '3-6_Dribbles_FG3M', '3-6_Dribbles_FG3A',
			'7+_Dribbles_FG2M', '7+_Dribbles_FG2A', '7+_Dribbles_FG3M',
			'7+_Dribbles_FG3A'
		]
		games = self.get_games(min(game_dates),max(game_dates))
		#drb = self.get_opp_dribble_shot(game_dates)
		#print('Completed dribble data')
		spots = self.get_opp_shot_spot(game_dates)
		op = self.get_open_shot_allowed(game_dates)
		#sht = spots.merge(drb,how='left',on=['GAME_DATE','TEAM_ID'])
		if len(op) != 0:
			final = spots.merge(op,how='left',on=['GAME_DATE','TEAM_ID'])
			final = final.merge(games,how='left', on=['GAME_DATE','TEAM_ID'])
		else:
			final = spots.merge(games,how='left', on=['GAME_DATE','TEAM_ID'])
		for col in sqlOrd:
			if col not in final.columns:
				final[col] = np.nan
		final = final.filter(sqlOrd)

		if (pd.read_sql("select count(*) as ct  from shotsAllowed where game_date in ({})".format(strD),self.conn).sum()>0).all():
			self.conn.execute("DELETE FROM shotsAllowed where game_date in ({})".format(strD))
			self.conn.commit()
		self.insert_data(final,'shotsAllowed')

	def get_roster(self,seasons):
		'''Expected Input: a list of seasons formatted as YYYY-YY
		Returns: A DataFrame that has the first and last game played for the player on each team they played that season
		'''
		df = pd.DataFrame()
		for season in seasons:
			log = PlayerGameLogs(season_nullable=season).get_data_frames()[0]
			roster = log.groupby(['TEAM_ID', 'SEASON_YEAR', 'PLAYER_ID', ]).GAME_DATE.agg(
				firstGame=min, lastGame=max).reset_index()
			df = pd.concat([roster, df])
		df['firstGame'] = [x[:x.find('T')]  for x in df.firstGame]
		df['lastGame'] = [x[:x.find('T')]  for x in df.lastGame]
		return df

	def get_rebounds(self,game_dates,qtr=0):
		'''
		Get rebounding information at the player level
		Inputs: list of game dates you are running for
		Output: DataFrame with rebounding related columns
		'''
		print('started rebounds at {}'.format(time.strftime('%H:%M')))
		minDate,maxDate = min(game_dates),max(game_dates)
		games = self.get_games(minDate,maxDate)
		rbsCols = ['PLAYER_ID','TEAM_ID','OREB','OREB_CONTEST','OREB_CHANCES','OREB_CHANCE_DEFER','AVG_OREB_DIST',
			'DREB','DREB_CONTEST','DREB_CHANCES','DREB_CHANCE_DEFER','AVG_DREB_DIST']
		rbs = LeagueDashPtStats(pt_measure_type='Rebounding',player_or_team='Player',
			date_from_nullable = minDate,
			date_to_nullable = maxDate,timeout=60
		).get_data_frames()[0][rbsCols]
		final = rbs.merge(games ,how='left',on=['TEAM_ID'])
		
		print('ended rebounds at {}'.format(time.strftime('%H:%M')))
		return final

	def get_logs(self,game_dates,seasons=None,qtr=0):
		'''Expected Input: a list of seasons formatted as YYYY-YY
		Returns: A DataFrame that has each game played by that player and the team, will be used as a base for our gamestats
		'''
		print('started player logs at {}'.format(time.strftime('%H:%M')))
		logCols = ['PLAYER_ID','TEAM_ID','GAME_ID','GAME_DATE','MIN','FTM','FTA','REB',
			'AST','TOV','STL','BLK','BLKA','PF','PFD','PTS','PLUS_MINUS', 'DD2','TD3']
		final = pd.DataFrame()
		minDate = min(game_dates)
		maxDate = max(game_dates)
		minD = pd.to_datetime(minDate)
		maxD = pd.to_datetime(maxDate)
		if seasons is not None:
			for season in seasons:
				seasonLog = PlayerGameLogs(season_nullable = season,period_nullable=qtr).get_data_frames()[0][logCols]
				seasonLog = seasonLog.filter(logCols)
				seasonLog.GAME_DATE = seasonLog.GAME_DATE.apply(lambda x: x[:10])
				final = pd.concat([final,seasonLog])
		else:
			season = '{}-{}'.format(minD.year,str(minD.year+1)[-2:]) if minD.month>=10 else '{}-{}'.format(minD.year-1,str(minD.year)[-2:])
			seasonLog = PlayerGameLogs(date_from_nullable = minD.strftime('%m/%d/%Y'),
				date_to_nullable = maxD.strftime('%m/%d/%Y'),
				season_nullable = season, period_nullable = qtr,league_id_nullable='00'
									   ,timeout=60).get_data_frames()[0][logCols]
			seasonLog = seasonLog.filter(logCols)
			seasonLog.GAME_DATE = seasonLog.GAME_DATE.apply(lambda x: x[:10])
			seasonLog = seasonLog[seasonLog.GAME_DATE.isin(game_dates)]
			return seasonLog

	def get_schedule(self,season):
		'''No Built in functionality to pull upcoming season, this will get upcoming games
		Inputs: Season as YYYY-YY
		Output: Dataframe with columns game_date, game_id, team_id,home
		'''
		sDate = season[:4]
		startDate = '{}-10-20'.format(sDate)
		r = requests.get("http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{}/league/00_full_schedule.json".format(sDate))
		h = [[v['gdte'], v['gid'],str(v['h']['tid']),1] for k in r.json()['lscd'] for v in k['mscd']['g'] if v['gdte']>startDate]
		a = [[v['gdte'], v['gid'],str(v['v']['tid']),0] for k in r.json()['lscd'] for v in k['mscd']['g'] if v['gdte']>startDate]
		df = pd.DataFrame(data=h+a, columns = ['game_date','game_id','team_id','home'])
		return df

	def get_opp_shot_spot(self, game_dates):
		'''get the type of shots (ranges) that a team allows, will also get the number of wide-open and open 2 and 3pt looks a team allows.  This needs to be done day-by-day as the granularity is only by team, so we can not get game-by-game information.
        Inputs: needs a list of game dates
        output: DataFrame containing each game and the number of open 2pt/3pt shots, number of wide open 2pt/3pt shots and the shot distribution by ranges
        '''
		# team defense shooting
		final = pd.DataFrame()
		for ct, date in enumerate(tqdm(game_dates)):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
																									   str(d.year)[-2:])
			oppShots = LeagueDashTeamShotLocations(measure_type_simple='Opponent',
												   date_from_nullable=date,
												   date_to_nullable=date,
												   season=season,timeout=60
												   ).get_data_frames()[0]
			oppShots.columns = ['{}_{}'.format(re.sub(' |-', '_', a), b) if a != '' else b for a, b in oppShots.columns]
			oppShots = oppShots.filter([col for col in oppShots.columns if re.search('_PCT$|NAME', col) == None])
			oppShots['GAME_DATE'] = date
			final = pd.concat([oppShots, final])
			time.sleep(np.random.choice(range(4, 10)))
		return final

	def get_opp_dribble_shot(self, game_dates):
		'''will be used to create shots allowed by team
        Input(s): list of game dates
        Output  : dataframe at the the level of each team game, will have columns for each area for each dribble type 0,1,2,3-6 and 7+ dribbles
        '''
		drib = ['0 Dribbles', '1 Dribble', '2 Dribbles', '3-6 Dribbles', '7+ Dribbles']
		final = pd.DataFrame()
		for ct, date in enumerate(tqdm(game_dates)):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
																									   str(d.year)[-2:])
			drb = pd.DataFrame()
			for dribbleCount in drib:
				drbShots = LeagueDashOppPtShot(date_from_nullable=date,
											   date_to_nullable=date,
											   season=season,
											   dribble_range_nullable=dribbleCount
											   ,timeout=60).get_data_frames()[0]
				df = drbShots.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$', col) != None])
				df.columns = [
					'{}_{}'.format(dribbleCount.replace(' ', '_'), col) if re.search('ID$', col) == None else col for
					col in df.columns]
				drb = pd.concat([drb, df])
				drb['GAME_DATE'] = date
			drb = drb.groupby(['TEAM_ID', 'GAME_DATE']).sum().reset_index()
			final = pd.concat([final, drb])
			if len(final) ==0:
				print('Missing dribble data for {}'.format(date))
		return final

	def get_plyr_drb_shots(self,game_dates,addSleep=False):
		'''will be used to create shots allowed by team
		Input(s): list of game dates
		Output  : dataframe at the the level of each team game, will have columns for each area for each dribble type 0,1,2,3-6 and 7+ dribbles 
		'''
		drib = ['0 Dribbles','1 Dribble','2 Dribbles','3-6 Dribbles','7+ Dribbles']
		final = pd.DataFrame()
		for ct,date in enumerate(tqdm(game_dates)):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
			drb = pd.DataFrame()
			for dribbleCount in drib:
				drbShots = LeagueDashPlayerPtShot(date_from_nullable = date,
					date_to_nullable = date,
					season=season,
					dribble_range_nullable=dribbleCount
					,timeout=60).get_data_frames()[0]
				time.sleep(np.random.randint(2,8))
				df = drbShots.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$',col)!=None])
				df.columns = ['{}_{}'.format(dribbleCount.replace(' ','_'),col) if re.search('ID$',col)==None else col for col in df.columns]
				drb = pd.concat([drb,df])
				drb['GAME_DATE'] = date
			drb = drb.groupby(['PLAYER_ID','GAME_DATE','PLAYER_LAST_TEAM_ID']).sum().reset_index()
			final = pd.concat([final,drb])
			if np.random.randint(0,100) % 6 == 0:
				time.sleep(np.random.randint(25,95))
		return final




	def get_player_shot_spots(self,game_dates,qtr=0):
		'''Expected Input: list of Dates of the game being played
		Returns: a dataframe containing the player id, game date and their shot attempts and makes from each designated area
		'''
		final = pd.DataFrame()
		print('start player shots at {}'.format(time.strftime('%H:%M')))
		for date in tqdm(game_dates):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
			sht = LeagueDashPlayerShotLocations(date_from_nullable = date,
				date_to_nullable = date,
				season=season,period = qtr,timeout=60).get_data_frames()[0]
			#op =
			df = self.clean_shotcolumns(sht)
			df['GAME_DATE'] = date
			final = pd.concat([final,df])
			time.sleep(np.random.choice(range(1,5)))
			if np.random.randint(0,100) % 5 == 0:
				time.sleep(np.random.choice(range(7,37)))
		print('completed player shots at {}'.format(time.strftime('%H:%M')))
		return final

	def get_advanced_box(self,game_dates,qtr=None):
		'''will get the pace, possesions, off/def rating and usage
		Inputs: will need a list of dates
		output: dataframe at the player/game level
		'''
		print('starting advanced box at {}'.format(time.strftime('%H:%M')))
		advcols = ['GAME_ID','PLAYER_ID','offensiveRating','defensiveRating','usagePercentage','pace','possessions']
		games = self.get_games(min(game_dates),max(game_dates))
		if ~(pd.Series(game_dates).is_monotonic_increasing) & ~(pd.Series(game_dates).is_monotonic_decreasing):
			games = games[games.GAME_DATE.isin(game_dates)]   
		df = pd.DataFrame() 
		for gid in tqdm(games.GAME_ID.unique()):
			if qtr is None:
				advbox = BoxScoreAdvancedV3(gid,timeout=60).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
			else:
				advbox = BoxScoreAdvancedV3(gid,
				start_period=qtr,end_period=qtr,timeout=60).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
			advbox = advbox.filter(advcols)
			advbox.drop(advbox[advbox.possessions==0].index,inplace=True)
			temp = pd.concat(GameRotation(gid).get_data_frames())
			lst = temp[temp.IN_TIME_REAL==0].PERSON_ID.values.tolist()
			df = pd.concat([df,advbox])
			df['Starter'] = np.where(df.PLAYER_ID.isin(lst),1,0)
			time.sleep(np.random.choice(range(1,5)))
		print('completed adv box at {}'.format(time.strftime('%H:%M')))
		return df

	def get_games(self,minDate,maxDate,add_season=False):
		'''Get a dataframe containing the game date, game_id and team_id, will get two rows for each game, one for each team.
		Input(s): minDate is the start date, maxDate is the end date, these dates are inclusive
		output: DataFrame with columns: GAME_DATE, TEAM_ID, GAME_ID
		'''
		
		gamefinder = leaguegamefinder.LeagueGameFinder(league_id_nullable = '00',
			date_from_nullable = pd.to_datetime(minDate).strftime('%m/%d/%Y'),
			date_to_nullable = pd.to_datetime(maxDate).strftime('%m/%d/%Y'),
			season_type_nullable = 'Regular Season',timeout=60
		)
		games = gamefinder.get_data_frames()[0][['GAME_DATE','TEAM_ID','GAME_ID']]
		if add_season == False:
			return games
		else:
			games['season'] =   ['{}-{}'.format(x[:4],int(x[2:4])+1) if int(x[5:7]) > 9 else '{}-{}'.format(int(x[:4])-1,x[2:4]) for x in games.GAME_DATE]
			return games


	def create_opp_games(self,game_dates):
		games = self.get_games(min(game_dates), max(game_dates))
		df = pd.DataFrame(columns=['GAME_ID','GAME_DATE' ,'TEAM_ID', 'OPP_ID'])
		for gid in games.GAME_ID.unique():
			perms = list(permutations(games[games.GAME_ID == gid].TEAM_ID.unique()))
			for p in perms:
				df.loc[len(df)] = [gid,games[games.GAME_ID==gid].GAME_DATE.values[0]] + list(p)
		return df

	def get_open_player_shots(self,game_dates):
		openness = ['6+ Feet - Wide Open','4-6 Feet - Open','2-4 Feet - Tight','0-2 Feet - Very Tight']
		final = pd.DataFrame()
		for ct, date in enumerate(tqdm(game_dates)):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
																		   str(d.year)[-2:])
			shots = pd.DataFrame()
			for op in openness:
				temp = LeagueDashPlayerPtShot(date_from_nullable=date,
												  date_to_nullable=date,
												  season=season,
												  dribble_range_nullable=op
												  , timeout=60).get_data_frames()[0]
				time.sleep(np.random.randint(2, 8))
				df = temp.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$', col) != None])
				df.columns = [
					'{}_{}'.format(op.replace(' ', '_'), col) if re.search('ID$', col) == None else col for
					col in df.columns]
				shots = pd.concat([drb, df])
				shots['GAME_DATE'] = date
			shots = shots.groupby(['PLAYER_ID', 'GAME_DATE', 'PLAYER_LAST_TEAM_ID']).sum().reset_index()
			final = pd.concat([final, drb])
			if np.random.randint(0, 100) % 6 == 0:
				time.sleep(np.random.randint(25, 95))
		return final

	def get_open_shot_allowed(self,game_dates):
		'''get the type of shots (ranges) that a team allows, will also get the number of wide-open and open 2 and 3pt looks a team allows.  This needs to be done day-by-day as the granularity is only by team, so we can not get game-by-game information.
		Inputs: needs a list of game dates
		output: DataFrame containing each game and the number of open 2pt/3pt shots, number of wide open 2pt/3pt shots and the shot distribution by ranges
		'''

		games = []
		for ct, date in enumerate(tqdm(game_dates)):
			try:
				d = pd.to_datetime(date)
				season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
					str(d.year)[-2:])
				wide = LeagueDashOppPtShot(date_from_nullable=date,
					date_to_nullable=date, season=season,
					close_def_dist_range_nullable='6+ Feet - Wide Open',timeout=60).get_data_frames()[0]
				wide = wide.filter([col for col in wide.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA', col) == None) &
					(wide[col].dtype != object)])
				wide.columns = [col if re.search('FG', col) == None else 'WIDEOPEN_{}'.format(col) for col in wide.columns]
				wide['GAME_DATE'] = date

				op = LeagueDashOppPtShot(date_from_nullable=date,
					date_to_nullable=date, season=season,
					close_def_dist_range_nullable='4-6 Feet - Open',timeout=60).get_data_frames()[0]
				op = op.filter([col for col in op.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA', col) == None) &
					(op[col].dtype != object)])
				op.columns = [col if re.search('FG', col) == None else 'OPEN_{}'.format(col) for col in op.columns]
				op['GAME_DATE'] = date
				df = wide.merge(op, how='left', on=['TEAM_ID', 'GAME_DATE'])
				games.append(df)

			except requests.exceptions.Timeout as e:
				logger.warning("{}: API Timeout - skipping".format(date))
			except (KeyError, ValueError) as e:
				logger.warning("{}: merge failed, missing tracking data - {}".format(date,e))
			except requests.exceptions.HTTPError as e:
				logger.warning("{}: HTTP error - {}".format(date,e))
		final = pd.concat(games) if games else pd.DataFrame()
		return final

	def get_summary(self,game_ids):
		'''Will get information for each game related to the team, pts per quarter, team advance stats, inactive players and home team
		Inputs: List of game_ids
		Output: write to sqlite table and a df
		'''

		df = pd.DataFrame()
		cols = pd.read_sql('select * from teamLog limit 1',self.conn).columns
		# Constants
		REQUESTS_PER_ITERATION = 2  # BoxScoreSummaryV2 + BoxScoreAdvancedV3
		SAFE_REQUESTS_PER_MINUTE = 550  # Conservative buffer below ~600 limit
		MIN_DELAY = 60 / (SAFE_REQUESTS_PER_MINUTE / REQUESTS_PER_ITERATION)  # ~0.22 seconds

		for ct, gameid in enumerate(tqdm(game_ids)):
			retry_count = 0
			max_retries = 3

			while retry_count < max_retries:
				try:
					bx = BoxScore(game_id=gameid)
					aLine = {'q{}_pts'.format(d.get('period')) if d.get('period') < 5 else 'ot{}_pts'.format(
						d.get('period') - 4): d.get('score') for d in bx.away_team_stats.data.get('periods')}
					a = {'team_id': bx.away_team.data.get('teamId'),
						 'game_id': bx.game_details.data.get('gameId'),
						 'game_date': bx.game_details.data.get('gameTimeLocal')[:10],
						 'home': 0,
						 'attendance': 0,
						 'win': 1 if bx.away_team.data.get('score') > bx.home_team.data.get('score') else 0,
						 'opp_id': bx.home_team.data.get('teamId'),
						 'inactive': [p.get('personId') for p in bx.away_team.data.get('players') if
									  p.get('status') == 'INACTIVE'],
						 'count_inactive': len([p.get('personId') for p in bx.away_team.data.get('players') if
												p.get('status') == 'INACTIVE']),
						 'bench_points': bx.away_team_stats.data.get('statistics').get('benchPoints'),
						 'bp_allowed': bx.home_team_stats.data.get('statistics').get('benchPoints'),
						 'points_off_turnovers': bx.away_team_stats.data.get('statistics').get('pointsFromTurnovers'),
						 'to_points_allowed': bx.home_team_stats.data.get('statistics').get('pointsFromTurnovers'),
						 'points_fast_break': bx.away_team_stats.data.get('statistics').get('pointsFastBreak'),
						 'fb_points_allowed': bx.home_team_stats.data.get('statistics').get('pointsFastBreak'),
						 'second_chance_points': bx.away_team_stats.data.get('statistics').get(
							 'secondChancePointsMade'),
						 'sc_points_allowed': bx.home_team_stats.data.get('statistics').get('secondChancePointsMade'),
						 'biggest_lead': bx.away_team_stats.data.get('statistics').get('biggestLead'),
						 'biggest_deficit': bx.home_team_stats.data.get('statistics').get('biggestLead'),
						 'biggest_run': bx.away_team_stats.data.get('statistics').get('biggestScoringRun'),
						 'biggest_run_allowed': bx.home_team_stats.data.get('statistics').get('biggestScoringRun'),
						 'time_leading': bx.away_team_stats.data.get('statistics').get('timeLeading'),
						 'times_tied': bx.away_team_stats.data.get('statistics').get('timesTied'),
						 }
					hLine = {'q{}_pts'.format(d.get('period')) if d.get('period') < 5 else 'ot{}_pts'.format(
						d.get('period') - 4): d.get('score') for d in bx.home_team.data.get('periods')}
					h = {'team_id': bx.home_team.data.get('teamId'),
						 'game_id': bx.game_details.data.get('gameId'),
						 'game_date': bx.game_details.data.get('gameTimeLocal')[:10],
						 'home': 1,
						 'attendance': bx.game_details.data.get('attendance'),
						 'win': 1 if bx.home_team.data.get('score') > bx.away_team.data.get('score') else 0,
						 'opp_id': bx.away_team.data.get('teamId'),
						 'inactive': [p.get('personId') for p in bx.home_team.data.get('players') if
									  p.get('status') == 'INACTIVE'],
						 'count_inactive': len([p.get('personId') for p in bx.home_team.data.get('players') if
												p.get('status') == 'INACTIVE']),
						 'bench_points': bx.home_team.data.get('statistics').get('benchPoints'),
						 'bp_allowed': bx.away_team_stats.data.get('statistics').get('benchPoints'),
						 'points_off_turnovers': bx.home_team_stats.data.get('statistics').get('pointsFromTurnovers'),
						 'to_points_allowed': bx.away_team_stats.data.get('statistics').get('pointsFromTurnovers'),
						 'points_fast_break': bx.home_team_stats.data.get('statistics').get('pointsFastBreak'),
						 'fb_points_allowed': bx.away_team_stats.data.get('statistics').get('pointsFastBreak'),
						 'second_chance_points': bx.home_team_stats.data.get('statistics').get(
							 'secondChancePointsMade'),
						 'sc_points_allowed': bx.away_team_stats.data.get('statistics').get('secondChancePointsMade'),
						 'biggest_lead': bx.home_team.data.get('statistics').get('biggestLead'),
						 'biggest_deficit': bx.away_team.data.get('statistics').get('biggestLead'),
						 'biggest_run': bx.home_team.data.get('statistics').get('biggestScoringRun'),
						 'biggest_run_allowed': bx.away_team.data.get('statistics').get('biggestScoringRun'),
						 'time_leading': bx.home_team.data.get('statistics').get('timeLeading'),
						 'times_tied': bx.home_team.data.get('statistics').get('timesTied'),

						 }
					h.update(hLine)
					a.update(aLine)
					bxScore = pd.concat(
						[pd.DataFrame.from_dict(h, orient='index').T, pd.DataFrame.from_dict(a, orient='index').T])
					adv = BoxScoreAdvancedV3(gameid).get_data_frames()[1].filter(
						['gameId', 'teamId', 'assistPercentage', 'offensiveRating', 'defensiveRating', 'pace',
						 'possessions', 'offensiveReboundPercentage', 'defensiveReboundPercentage']).rename(
						columns={'gameId': 'game_id', 'teamId': 'team_id', 'assistPercentage': 'assist_percentage',
								 'offensiveRating': 'offensive_rating', 'defensiveRating': 'defensive_rating',
								 'offensiveReboundPercentage': 'off_rb_pct',
								 'defensiveReboundPercentage': 'def_rb_pct'})
					final = bxScore.merge(adv, how='left', on=['game_id', 'team_id'])
					final['season'] = self.derive_season(bx.game_details.data.get('gameTimeLocal')[:10])

					# for col in pd.read_sql('select * from teamLog limit 1', self.conn).columns:
					# 	if col not in final.columns:
					# 		final[col] = None

					# Success - break retry loop
					df = pd.concat([df,final])
					break

				except HTTPError as e:
					if e.response.status_code == 429:  # Rate limit hit
						retry_count += 1
						wait_time = (2 ** retry_count) * 5 + random.uniform(0, 5)  # Exponential backoff
						print(f"Rate limit hit. Waiting {wait_time:.1f}s (attempt {retry_count}/{max_retries})")
						time.sleep(wait_time)
					else:
						raise

			# Consistent small delay between requests
			time.sleep(MIN_DELAY + random.uniform(0, 0.1))

			# Extra buffer every 100 requests
			if ct > 0 and ct % 100 == 0:
				time.sleep(random.uniform(2, 5))
		for col in cols:
			if col not in df.columns:
				df[col] = None
		df['inactive'] = [','.join([str(y) for y in x])  for x in df.inactive]
		return df.filter(cols)

	def get_player_info(self,pid):
		pinfo = ['PERSON_ID','DISPLAY_FIRST_LAST','HEIGHT','WEIGHT','POSITION','DRAFT_YEAR','DRAFT_NUMBER','BIRTHDATE']
		pin = CommonPlayerInfo(pid).get_data_frames()[0][pinfo]
		pin['HEIGHT'] = [int(ft)*12 + int(inch) for ft,inch in pin.HEIGHT.str.split('-')]
		pin['BIRTHDATE'] = pin.BIRTHDATE.apply(lambda x: x[:x.find('T')])
		return pin

	def clean_lgdashoppcolumns(self,df,dribble):
		df = df.filter([col for col in df.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA',col)==None) &
			(df[col].dtype!=object)] )
		if dribble != False:
			df.columns = ['{}_{}'.format(col,dribble.replace(' ','_')) if col!='TEAM_ID' else col for col in df.columns] 
		return df

	def clean_shotcolumns(self,df):
		df.columns = ['{}_{}'.format(re.sub(' |-','_',a),b) if a!='' else b for a,b in df.columns]
		df = df.filter([col for col in df.columns if (re.search('ID$|^Restricted|^Mid|^In_The|^Right|^Left|^Above',col)!=None) &
			(re.search('PCT$',col)==None)])
		df.columns = ['PLAYER_ID','TEAM_ID','ra_fgm','ra_fga','paint_fgm','paint_fga','mid_fgm','mid_fga','lc_fgm',
			'lc_fga','rc_fgm','rc_fga','abv_fgm','abv_fga']
		return df

	def update_player_info(self,seasons):
		'''Need to run this when getting new players as well
		Inputs: list of season
		Output: Update of how many rows where added
		'''
		curdb = [x[0] for x in self.conn.execute('select distinct player_id from players').fetchall()]
		final = pd.DataFrame()
		plyers = []
		sqlord = ['PERSON_ID','DISPLAY_FIRST_LAST','HEIGHT','WEIGHT','POSITION','DRAFT_YEAR','DRAFT_NUMBER','BIRTHDATE',
			'NBA All-Star', 'All-NBA1','All-NBA2','All-NBA3','All-Defensive Team1', 'All-Defensive Team2',
			'NBA Most Improved Player(null)','NBA Defensive Player of the Year','NBA Most Valuable Player',
			'NBA Finals Most Valuable Player']
		for season in seasons:
			pi = PlayerIndex(season=season).get_data_frames()[0]
			pi['PERSON_ID'] = pi.PERSON_ID.astype(str)
			pi = pi[~pi.PERSON_ID.isin(curdb)]
			plyers.extend(pi.PERSON_ID.values)
		plyers = list(set(plyers))
		print('Need to get {} new players'.format(len(plyers)))
		for ct,pid in enumerate(tqdm(plyers)):
			pin = self.get_player_info(pid)
			#award = self.get_awards(pid)
			#ply = pin.merge(award,how='left',on='PERSON_ID')
			final = pd.concat([final,pin])

			time.sleep(np.random.choice(range(2,5)))
			if ct % 50==0 and ct != 0:
				time.sleep(300)
			try:
				if ct % 25 ==0 and ct != 0:
					final.to_pickle('data/pickle/awards.pkl')
			except:
				print('couldnt find file')
		for col in sqlord:
			if col not in final.columns:
				final[col] = None
		self.insert_data(final.filter(sqlord),'players')
		return final

	def get_first_buckets(self,game_dates):
		'''
		Goes through the play-by-play data and grabs the first basket by the home and away team and puts down the first team and game bucket
		Input: list of game dates - can be the min/max if doing more than two
		Output: dataframe
		'''
		l = []
		print('getting first buckets : at {}'.format(time.strftime('%H:%M')))
		games = self.get_games(min(game_dates),max(game_dates))
		if len(game_dates)>3:
			gameids = games[games.GAME_DATE.isin(game_dates)].GAME_ID.unique()
		else:
			gameids = games.GAME_ID.unique() 
		for ct,gameid in enumerate(tqdm(gameids)):
			df = PlayByPlayV3(gameid).get_data_frames()[0]
			try:
				aind = df[(df.actionType == 'Made Shot') & (df.location == 'v')].personId.values[0]
				aev = df[(df.actionType == 'Made Shot') & (df.personId == aind)].actionNumber.min()
				hind = df[(df.actionType == 'Made Shot') & (df.location == 'h')].personId.values[0]
				hev = df[(df.actionType == 'Made Shot') & (df.personId == hind)].actionNumber.min()
				gd = {'gameid':gameid,'homePlayer':hind,
					'awayPlayer':aind,
					'firstPlayer':aind if aev < hev else hind}
				bskts = set([(gd['gameid'],v,1,1)  if list(gd.values()).count(v) ==2 else (gd['gameid'],v,1,0) for k,v in gd.items() if k!='gameid'])
			except IndexError:
				gd = {'gameid':gameid,'homePlayer':'999',
					'awayPlayer':'999',
					'firstPlayer':'999'}
				bskts = set([(gd['gameid'],v,1,1)  if list(gd.values()).count(v) ==2 else (gd['gameid'],v,1,0) for k,v in gd.items() if k!='gameid'])
			#bdf = pd.DataFrame(bskts,columns = ['GAME_ID','PLAYER_ID','teamFirst','gameFirst'])
			l.append(bskts)
			time.sleep(np.random.choice(range(1,5)))

		df = pd.DataFrame([x for y in l for x in y],columns = ['GAME_ID','PLAYER_ID','team_first','game_first'])
		print('\tcompleted at {}'.format(time.strftime('%H:%M')))
		return df

	def get_tracking_data(self,game_dates):
		try:
			dCols = ['PLAYER_ID', 'TEAM_ID', 'DRIVE_FGM', 'DRIVE_FGA', 'DRIVE_PASSES', 'DRIVE_AST', 'DRIVE_TOV', 'DRIVE_PF']
			puCols = ['PLAYER_ID', 'TEAM_ID', 'PULL_UP_FGM', 'PULL_UP_FGA', 'PULL_UP_FG3M', 'PULL_UP_FG3A', ]
			csCols = ['PLAYER_ID', 'TEAM_ID', 'CATCH_SHOOT_FGM', 'CATCH_SHOOT_FGA', 'CATCH_SHOOT_FG3M', 'CATCH_SHOOT_FG3A']
			passCols = ['PLAYER_ID', 'TEAM_ID', 'PASSES_MADE', 'PASSES_RECEIVED', 'FT_AST', 'SECONDARY_AST', 'POTENTIAL_AST', 'AST_PTS_CREATED',
						'AST_ADJ']
			ord =['PLAYER_ID', 'GAME_ID', 'TEAM_ID', 'OPP_ID', 'GAME_DATE', 'DRIVE_FGM',
		   'DRIVE_FGA', 'DRIVE_PASSES', 'DRIVE_AST', 'DRIVE_TOV', 'DRIVE_PF',
		   'PULL_UP_FG2M', 'PULL_UP_FG2A',
		   'PULL_UP_FG3M', 'PULL_UP_FG3A', 'CATCH_SHOOT_FG2M', 'CATCH_SHOOT_FG2A',
		   'CATCH_SHOOT_FG3M', 'CATCH_SHOOT_FG3A', 'PASSES_MADE',
		   'PASSES_RECEIVED', 'FT_AST', 'SECONDARY_AST', 'POTENTIAL_AST',
		   'AST_PTS_CREATED', 'AST_ADJ']
			mergeCols = ['PLAYER_ID','TEAM_ID']
			l = []
			for date in tqdm(game_dates):

				season = self.derive_season(date)
				games = self.create_opp_games([date])
				drives = LeagueDashPtStats(date_from_nullable = date,date_to_nullable = date,season=season,
						pt_measure_type = 'Drives', player_or_team = 'Player',timeout=60).get_data_frames()[0]
				drives = drives.filter(dCols)
				time.sleep(.8)
				pullups = LeagueDashPtStats(date_from_nullable=date, date_to_nullable=date,
										   pt_measure_type='PullUpShot', player_or_team='Player'
											,timeout=60).get_data_frames()[0]
				pullups = pullups.filter(puCols)
				pullups['PULL_UP_FG2M'] = pullups['PULL_UP_FGM'] - pullups['PULL_UP_FG3M']
				pullups['PULL_UP_FG2A'] = pullups['PULL_UP_FGA'] - pullups['PULL_UP_FG3A']
				time.sleep(.8)
				catchSht = LeagueDashPtStats(date_from_nullable=date, date_to_nullable=date,
										   pt_measure_type='CatchShoot', player_or_team='Player',
											 timeout=60).get_data_frames()[0]
				catchSht = catchSht.filter(csCols)
				catchSht['CATCH_SHOOT_FG2M'] =  catchSht['CATCH_SHOOT_FGM'] - catchSht['CATCH_SHOOT_FG3M']
				catchSht['CATCH_SHOOT_FG2A'] = catchSht['CATCH_SHOOT_FGA'] - catchSht['CATCH_SHOOT_FG3A']
				time.sleep(.8)
				passes = LeagueDashPtStats(date_from_nullable=date, date_to_nullable=date,
										   pt_measure_type='Passing', player_or_team='Player'
										   ,timeout=60).get_data_frames()[0]
				passes = passes.filter(passCols)
				games = games.merge(drives,how='left',on='TEAM_ID')
				games = games.merge(pullups,how='left',on=mergeCols)
				games = games.merge(catchSht,how='left',on=mergeCols)
				games = games.merge(passes,how='left',on=mergeCols)
				l.append(games)
			final = pd.concat(l)
			logger.info("{}: loaded {} player tracking rows".format(date, len(games)))

		except (KeyError, ValueError) as e:
			logger.warning("{}: tracking data unavailable - {}".format(date, e))
		return final.filter(ord)


