import pandas as pd
import numpy as np
import time
import re
from tqdm import tqdm
import requests
from nba_api.stats.endpoints import (
	BoxScoreAdvancedV3, PlayByPlayV2, BoxScoreSummaryV2,
	LeagueDashTeamShotLocations, LeagueDashOppPtShot,
	LeagueDashPlayerShotLocations, PlayerGameLogs, TeamInfoCommon,
	leaguegamefinder, LeagueDashPtStats, PlayerIndex,
	CommonPlayerInfo, PlayerAwards, GameRotation, LeagueDashPlayerPtShot
)
from .NBAbase import base

class etl(base):
	
	def __init__(self):
		super().__init__()

	def reindex(self):
		'''This will get all the existing indices and re-index them in the database
		'''
		q = '''SELECT name
			FROM sqlite_master 
			WHERE type = 'index' 
			AND tbl_name IN ('plyrLogs', 'teamLog', 'players', 'teams','shotsAllowed')
			AND name not like '%_autoindex_%'
			ORDER BY tbl_name, name;'''
		[self.cur.execute('REINDEX {}'.format(r[0])) for r in self.cur.execute(q).fetchall()]
		return print('Updated indices')


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
			#aTemp =
			PlayerGameLogs(season_nullable=season, measure_type_player_game_logs_nullable='Advanced').get_data_frames()[
				0]
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
		time.sleep(np.random.randint(30,120))
		rbs = self.get_rebounds(game_dates) #all 4
		adv = self.get_advanced_box(game_dates) # gameid,playerid
		time.sleep(np.random.randint(30,120))
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
		if (pd.read_sql("select count(*) as ct  from {} where game_date in ({})".format(strD,table,strD),self.conn).sum()>0).all():
			self.conn.execute("DELETE FROM {} where game_date in ({})".format(strD,table))
			self.conn.commit()
		self.insert_data(final,table)
		if np.random.randit % 7==0:
			time.sleep(np.random.randit(30,120))
		#return final
		
	def update_teamLog(self,game_ids):
		strD = ','.join(["'{}'".format(d) for d in game_ids])
		df = self.get_summary(game_ids)
		sqlorder = self.cur.execute('select * from teamLog limit 1').description
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
		drb = self.get_opp_dribble_shot(game_dates)
		print('Completed dribble data')
		spots = self.get_opp_shot_spot(game_dates)
		print('Completed spot data')
		op = self.get_open_shot_allowed((game_dates))
		print('Completed open shot data')
		
		sht = spots.merge(drb,how='left',on=['GAME_DATE','TEAM_ID'])
		if len(op) != 0:
			final = sht.merge(op,how='left',on=['GAME_DATE','TEAM_ID'])
			final = final.merge(games,how='left', on=['GAME_DATE','TEAM_ID'])
		else:
			final = sht.merge(games,how='left', on=['GAME_DATE','TEAM_ID'])
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
		Inputs: game dates you are running for
		Output: DataFrame with rebounding related columns
		'''
		print('started rebounds at {}'.format(time.strftime('%H:%M')))
		minDate,maxDate = min(game_dates),max(game_dates)
		games = self.get_games(minDate,maxDate)
		rbsCols = ['PLAYER_ID','TEAM_ID','OREB','OREB_CONTEST','OREB_CHANCES','OREB_CHANCE_DEFER','AVG_OREB_DIST',
			'DREB','DREB_CONTEST','DREB_CHANCES','DREB_CHANCE_DEFER','AVG_DREB_DIST']
		rbs = LeagueDashPtStats(pt_measure_type='Rebounding',player_or_team='Player',
			date_from_nullable = minDate,
			date_to_nullable = maxDate,
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
				season_nullable = season, period_nullable = qtr).get_data_frames()[0][logCols]
			seasonLog = seasonLog.filter(logCols)
			seasonLog.GAME_DATE = seasonLog.GAME_DATE.apply(lambda x: x[:10])
			seasonLog = seasonLog[seasonLog.GAME_DATE.isin(game_dates)]
			return seasonLog

	def get_schedule(self,season):
		'''No Built in functionality to pull upcoming season, this will get upcoming games
		Inputs: Season as YYYY-YY
		Output: Dataframe with columns game_date, game_id, team_id,home
		'''
		startDate = '{}-10-21'.format(season[:4])
		r = requests.get("http://data.nba.com/data/10s/v2015/json/mobile_teams/nba/{}/league/00_full_schedule.json".format(season))
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
												   season=season
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
											   dribble_range_nullable=dribbleCount).get_data_frames()[0]
				df = drbShots.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$', col) != None])
				df.columns = [
					'{}_{}'.format(dribbleCount.replace(' ', '_'), col) if re.search('ID$', col) == None else col for
					col in df.columns]
				drb = pd.concat([drb, df])
				drb['GAME_DATE'] = date
			drb = drb.groupby(['TEAM_ID', 'GAME_DATE']).sum().reset_index()
			final = pd.concat([final, drb])
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
					dribble_range_nullable=dribbleCount).get_data_frames()[0]
				if addSleep:
					time.sleep(np.random.randint(2,8))
				df = drbShots.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$',col)!=None])
				df.columns = ['{}_{}'.format(dribbleCount.replace(' ','_'),col) if re.search('ID$',col)==None else col for col in df.columns]
				drb = pd.concat([drb,df])
				drb['GAME_DATE'] = date
			drb = drb.groupby(['PLAYER_ID','GAME_DATE','PLAYER_LAST_TEAM_ID']).sum().reset_index()
			final = pd.concat([final,drb])
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
				season=season,period = qtr).get_data_frames()[0]
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
				advbox = BoxScoreAdvancedV3(gid).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
			else:
				advbox = BoxScoreAdvancedV3(gid,start_period=qtr,end_period=qtr).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
			advbox = advbox.filter(advcols)
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
			season_type_nullable = 'Regular Season'
		)  
		games = gamefinder.get_data_frames()[0][['GAME_DATE','TEAM_ID','GAME_ID']]
		if add_season == False:
			return games
		else:
			games['season'] =   ['{}-{}'.format(x[:4],int(x[2:4])+1) if int(x[5:7]) > 9 else '{}-{}'.format(int(x[:4])-1,x[2:4]) for x in games.GAME_DATE]
			return games

	def get_open_shot_allowed(self,game_dates):
		'''get the type of shots (ranges) that a team allows, will also get the number of wide-open and open 2 and 3pt looks a team allows.  This needs to be done day-by-day as the granularity is only by team, so we can not get game-by-game information.
		Inputs: needs a list of game dates
		output: DataFrame containing each game and the number of open 2pt/3pt shots, number of wide open 2pt/3pt shots and the shot distribution by ranges
		'''
		final = pd.DataFrame()
		for ct, date in enumerate(tqdm(game_dates)):
			d = pd.to_datetime(date)
			season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
				str(d.year)[-2:])
			wide = LeagueDashOppPtShot(date_from_nullable=date,
				date_to_nullable=date, season=season,
				close_def_dist_range_nullable='6+ Feet - Wide Open').get_data_frames()[0]
			wide = wide.filter([col for col in wide.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA', col) == None) &
				(wide[col].dtype != object)])
			wide.columns = [col if re.search('FG', col) == None else 'WIDEOPEN_{}'.format(col) for col in wide.columns]
			wide['GAME_DATE'] = date

			op = LeagueDashOppPtShot(date_from_nullable=date,
				date_to_nullable=date, season=season,
				close_def_dist_range_nullable='4-6 Feet - Open').get_data_frames()[0]
			op = op.filter([col for col in op.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA', col) == None) &
				(op[col].dtype != object)])
			op.columns = [col if re.search('FG', col) == None else 'OPEN_{}'.format(col) for col in op.columns]
			op['GAME_DATE'] = date
			try:
				df = wide.merge(op, how='left', on=['TEAM_ID', 'GAME_DATE'])
				final = pd.concat([final,df])
			except:
				print('{} no distance from shooter data'.format(date))
		return final

	def get_summary(self,game_ids):
		'''Will get information for each game related to the team, pts per quarter, team advance stats, inactive players and home team
		Inputs: List of game_ids
		Output: write to sqlite table and a df
		'''
		
		sqlorder = ['GAME_ID','GAME_DATE','TEAM_ID','inactive','count_inactive','assistPercentage','offensiveRating',
			'defensiveRating','pace','possessions','offensiveReboundPercentage','defensiveReboundPercentage',
			'PTS_2ND_CHANCE','PTS_FB','TEAM_TURNOVERS','PTS_OFF_TO','home','PTS_QTR1','PTS_QTR2','PTS_QTR3',
			'PTS_QTR4','PTS_OT1','PTS_OT2','PTS_OT3','PTS_OT4','wins','season']
		df = pd.DataFrame()
		for ct,gameid in enumerate(tqdm(game_ids)):
			b = BoxScoreSummaryV2(game_id=gameid).get_data_frames()
			gameDate = b[0].GAME_DATE_EST.str[:10][0]
			d = pd.to_datetime(gameDate)
			season = '{}-{}'.format(d.year, str(d.year + 1)[-2:]) if d.month >= 10 else '{}-{}'.format(d.year - 1,
				str(d.year)[-2:])
			teamStats = b[1][['TEAM_ID','PTS_2ND_CHANCE','PTS_FB','TEAM_TURNOVERS','PTS_OFF_TO']]
			inAct = b[3].groupby(['TEAM_ID']).PLAYER_ID.agg([list,'count']).reset_index()
			inAct['list'] = inAct.list.apply(lambda x:','.join([str(ply) for ply in x]))
			inAct = inAct.rename(columns = {'list':'inactive','count':'count_inactive'})
			adv = BoxScoreAdvancedV3(gameid).get_data_frames()[1].filter(['gameId', 'teamId','assistPercentage','offensiveRating','defensiveRating','pace','possessions','offensiveReboundPercentage',
				'defensiveReboundPercentage']).rename(columns={'gameId':'GAME_ID','teamId':'TEAM_ID'})
			scoringdf = b[5].filter([col for col in b[5].columns if (col.find('PTS_')>-1) & (b[5][col].sum()!=0)]+['GAME_ID','TEAM_ID'])
			home = b[7].filter(['GAME_ID','HOME_TEAM_ID'])
			
			advHome = adv.merge(home,how='left',on='GAME_ID')
			scoringdf = scoringdf.merge(teamStats,how='left',on='TEAM_ID')
			advHome.HOME_TEAM_ID = np.where(advHome.HOME_TEAM_ID == advHome.TEAM_ID,1,0)
			advHome = advHome.rename(columns = {'HOME_TEAM_ID':'home'})
			advInact = inAct.merge(advHome,how='right',on=['TEAM_ID'])
			final = advInact.merge(scoringdf,how='left',on=['TEAM_ID','GAME_ID'])
			final['GAME_DATE'] = gameDate
			df = pd.concat([df,final])
			df['Total'] = df.filter([col for col in df.columns if col.find('PTS_') > -1]).sum(axis=1)
			df['wins'] = np.where(df.groupby('GAME_ID').Total.transform('max') == df.Total, 1, 0)
			df['season'] = season
			
			df.drop(['Total'],axis=1,inplace=True)
			for col in sqlorder:
				if col not in df.columns:
					df[col] = None
			if ct % 15 == 0:
				time.sleep(np.random.choice(range(2,10)))
			#df.to_pickle('nba/data/pickle/teamlog5.pkl')
			if ct % 150 == 0 and ct != 0 :
				time.sleep(np.random.choice(range(10,30)))
		return df.filter(sqlorder)

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
			award = self.get_awards(pid)
			ply = pin.merge(award,how='left',on='PERSON_ID')
			final = pd.concat([final,ply])

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
			df = PlayByPlayV2(gameid).get_data_frames()[0]
			try:
				aind = df[(df.EVENTMSGTYPE==1) & (df.HOMEDESCRIPTION.notna())].PLAYER1_ID.values[0]
				aev = df[(df.EVENTMSGTYPE==1) & (df.PLAYER1_ID==aind)].EVENTNUM.min()
				hind = df[(df.EVENTMSGTYPE==1) & (df.VISITORDESCRIPTION.notna())].PLAYER1_ID.values[0]
				hev = df[(df.EVENTMSGTYPE==1) & (df.PLAYER1_ID==hind)].EVENTNUM.min()
				gd = {'gameid':gameid,'homePlayer':hind,
					'awayPlayer':aind,
					'firstPlayer':aind if aev < hev else hind}
				bskts = set([(gd['gameid'],v,1,1)  if list(gd.values()).count(v) ==2 else (gd['gameid'],v,1,0) for k,v in gd.items() if k!='gameid'])
			except:
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