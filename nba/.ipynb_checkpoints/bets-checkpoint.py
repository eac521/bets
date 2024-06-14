import sqlite3
import pandas as pd
import numpy as np
import re
from nba_api.stats.endpoints import BoxScoreAdvancedV3,PlayByPlayV2,BoxScoreSummaryV2,LeagueDashTeamShotLocations,LeagueDashOppPtShot,LeagueDashPlayerShotLocations,PlayerGameLogs,TeamInfoCommon,leaguegamefinder,LeagueDashPtStats
from nba_api.stats.static import teams
class nba:

    def __init__(self):
        self.db = './nba/data/database/nba.db'
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        self.showTables = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table';",self.conn)
        
    def get_start_dates(self):
        return ['2017-10-17','2018-10-16','2019-10-22','2021-10-19','2022-10-18','2023-10-24']
        
    def get_last_game(self,table):
        '''This will get the last game loaded in the table
        '''
        return pd.read_sql('select max(game_date) as date from {}'.format(table),db).date[0]
        
        
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
        return df
    
    def get_first_buckets(self,gameids):
        l = []
        for gameid in gameids:
            df = PlayByPlayV2(gameid).get_data_frames()[0]
            aind = df[(df.EVENTMSGTYPE==1) & (df.HOMEDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            hind = df[(df.EVENTMSGTYPE==1) & (df.VISITORDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            gd = {'gameid':gameid,'homePlayer':df.iloc[hind].PLAYER1_ID,
                  'awayPlayer':df.iloc[aind].PLAYER1_ID,
                  'firstPlayer':df.iloc[min([aind,hind])].PLAYER1_ID}
            bskts = set([(gd['gameid'],v,1,1)  if list(gd.values()).count(v) ==2 else (gd['gameid'],v,1,0) for k,v in gd.items() if k!='gameid'])
            #bdf = pd.DataFrame(bskts,columns = ['GAME_ID','PLAYER_ID','teamFirst','gameFirst'])
            l.append(bskts)
        df = pd.DataFrame([x for y in l for x in y],columns = ['GAME_ID','PLAYER_ID','team_first','game_first'])
        return df 

        
    def get_summary(self,gameid):
        b= BoxScoreSummaryV2(game_id=gameid).get_data_frames()
        df1 = b[5]
        df1 = df1.filter([col for col in df1.columns if (df1[col].dtype!=object) & (df1[col].sum()!=0) & (col!='GAME_SEQUENCE') ])
        df = df1.merge(b[1][['TEAM_ID','LARGEST_LEAD','PTS_FB','PTS_2ND_CHANCE','TOTAL_TURNOVERS']])
        df['GAME_ID'] = gameid
        l.append(df.to_dict(orient='records'))
        return l
        
    def get_opp_open_shot(self,date):
        #team defense shooting

        t = time.time()
        print('starting at {}'.format(time.strftime('%H:%M')))
        final = pd.DataFrame()
        for ct,date in enumerate(missing):
            d = pd.to_datetime(date)
            season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
            oppShots = LeagueDashTeamShotLocations(measure_type_simple='Opponent',
                                              date_from_nullable = date,
                                               date_to_nullable = date,
                                           season=season
                               ).get_data_frames()[0]
            oppShots.columns = ['{}_{}'.format(re.sub(' |-','_',a),b) if a!='' else b for a,b in oppShots.columns]
            oppShots = oppShots.filter([col for col in oppShots.columns if re.search('_PCT$',col)==None])
            oppShots['GAME_DATE'] = date
            wide =  LeagueDashOppPtShot(date_from_nullable = date,
                              date_to_nullable = date, season=season,
                                  close_def_dist_range_nullable = '6+ Feet - Wide Open').get_data_frames()[0]
            wide = wide.filter([col for col in wide.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA',col)==None) &
                                                        (wide[col].dtype!=object)] )
            wide.columns = [col if re.search('FG',col)==None else 'WIDEOPEN_{}'.format(col) for col in wide.columns]
            wide['GAME_DATE'] = date
    
            op = LeagueDashOppPtShot(date_from_nullable = date,
                              date_to_nullable = date,season=season,
                                  close_def_dist_range_nullable = '4-6 Feet - Open').get_data_frames()[0]
            op = op.filter([col for col in op.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA',col)==None) &
                                                        (op[col].dtype!=object)] )
            op.columns = [col if re.search('FG',col)==None  else 'OPEN_{}'.format(col) for col in op.columns]
            op['GAME_DATE'] = date
            df = oppShots.merge(wide.merge(op,how='left',on=['TEAM_ID','GAME_DATE']),how='left',on=['TEAM_ID','GAME_DATE'])
        final = pd.concat([df,final])
        return final
            
    def get_games(self):
        gamefinder = leaguegamefinder.LeagueGameFinder(league_id_nullable = '00',
                                               date_from_nullable = '10/01/2023',
                                               season_type_nullable = 'Regular Season'
                                                
                                               )  
        games = gamefinder.get_data_frames()[0][['GAME_DATE','TEAM_ID','GAME_ID']]
        return games

    def get_advanced_box(self,game_ids):
        '''will get the pace, possesions, off/def rating and 
        '''
        advcols = ['GAME_ID','PLAYER_ID','offensiveRating','defensiveRating','usagePercentage','pace','possessions']
        df = pd.DataFrame()
        for gid in game_ids:
            advbox = BoxScoreAdvancedV3(gid).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
            advbox = advbox.filter(advcols)
            df = pd.concat([df,advbox])
        return df
        
    def get_opp_dribble_shot(self,dates):
        final = pd.DataFrame()
        drib = ['0 Dribbles','1 Dribble','2 Dribbles','3-6 Dribbles','7+ Dribbles']
        t = time.time()
        games = gamefinder.get_data_frames()[0][['GAME_DATE','TEAM_ID','GAME_ID']]
        games.GAME_DATE = pd.to_datetime(games.GAME_DATE)
        print('starting at {}'.format(time.strftime('%H:%M')))
        
        for ct,date in enumerate(game_dates):
            d = pd.to_datetime(date)
            season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
            drb = pd.DataFrame()
            for dribbleCount in drib:
                drbShots = LeagueDashOppPtShot(date_from_nullable = date,
                                    date_to_nullable = date,
                                               season=season,
                                    dribble_range_nullable=dribbleCount).get_data_frames()[0]
                df = drbShots.filter([col for col in drbShots.columns if re.search('[2-3][A|M]$|ID$',col)!=None])
                df.columns = ['{}_{}'.format(dribbleCount.replace(' ','_'),col) if re.search('ID$',col)==None else col for col in df.columns]
                drb = pd.concat([drb,df])
            drb = drb.merge(games[games.GAME_DATE==d],how='left',on=['TEAM_ID'])
            drb = drb.groupby(['TEAM_ID','GAME_ID','GAME_DATE']).sum().reset_index()
            final = pd.concat([drb,final])
            return final

    
    def get_player_shot_spots(self,dates):
        '''Expected Input: list of Dates of the game being played
           Returns: a dataframe containing the player id, game date and their shot attempts and makes from each designated area
        '''
        for date in dates:
            d = pd.to_datetime(date)
            season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
            df = LeagueDashPlayerShotLocations(date_from_nullable = date,
                                         date_to_nullable = date,
                                         season=season).get_data_frames()[0]
            df = shotcolumns(df)
            df['GAME_DATE'] = date
            temp = temp[temp.GAME_DATE==date]
            final = df.merge(temp,how='left',on=['GAME_DATE','TEAM_ID'])
        return final
    
    def get_logs(self,dates):
        '''Expected Input: a list of seasons formatted as YYYY-YY
           Returns: A DataFrame that has each game played by that player and the team, will be used as a base for our gamestats
        '''
        min_date = min(
        logCols = ['SEASON_YEAR','PLAYER_ID','TEAM_ID','GAME_ID','GAME_DATE','MIN','FTM','FTA','AST','TOV','STL','BLK','BLKA','PF','PFD','PTS','PLUS_MINUS',
 'DD2','TD3']
        
        seasonLog = PlayerGameLogs(date_from_nullable=d).get_data_frames()[0][logCols]
        seasonLog = seasonLog.filter(logCols)

        
        final = seasonLog.merge(advbsk,how='left',on = ['GAME_ID','PLAYER_ID'])
            
        final.to_sql('logStage',db,if_exists='append',index=False)
        
        return df
    def get_rebounds(self,game_dates):
        games = self.get_games()
        min_date = min(game_dates)
        max_date = max(game_dates)
        #d = pd.to_datetime(date)
        #season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
    
        rbs = LeagueDashPtStats(pt_measure_type='Rebounding',player_or_team='Player',
                               date_from_nullable = min_date,
                               date_to_nullable = max_date,
                               season = season
                               ).get_data_frames()[0][['PLAYER_ID','TEAM_ID','OREB','OREB_CONTEST','OREB_CHANCES','OREB_CHANCE_DEFER','AVG_OREB_DIST',
                             'DREB','DREB_CONTEST','DREB_CHANCES','DREB_CHANCE_DEFER','AVG_DREB_DIST']]
        final = rbs.merge(games[games.GAME_DATE==date],how='left',on=['TEAM_ID'])
    return final


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
    
    def get_team_info(self):
        '''This can just run called as all team_ids will be loaded when the module is instantiated
        '''
        df = pd.DataFrame()
        for teamId in self.teams:
            t = TeamInfoCommon(teamId)
            t = t.get_data_frames()[0][['TEAM_ID','TEAM_ABBREVIATION','TEAM_CITY','TEAM_CONFERENCE','TEAM_DIVISION']]
            df = pd.concat([df,t])
        df['lat'] = None
        df['long'] = None
        return df.to_dict(orient='records')

    def insert_data(self,data,table):
        cols = pd.read_sql('''
        PRAGMA table_info({})
        '''.format(table),self.conn).name.values
        rows = data.shape[0]
        v = '?' + ',?' * (len(cols)-1)
        self.cur.executemany('''insert into {t} values ({v})'''.format(t=table,v=v),data.values.tolist())
        self.conn.commit()
        return print('{} has been updated with {:,} rows'.format(table,rows))
            
    def update_shots_allowed(self,dates):
        '''Pull in prior day's data for team's shot types allowed.  Will need the data and will use the get_opp_dribble_shots and get_opp_op_shot,
        merge them and insert the new data in the database
        '''
        sqlCols = ['team_id','ra_fgm','ra_fga','mid_fgm','mid_fga','lc_fgm','lc_fga','rc_fgm','rc_fga','abv_fgm','abv_fga','bck_fgm','bck_fga',
 'crn_fgm','crn_fga','game_date','wide_fg2m','wide_fg2a','wide_fg3m','wide_fg3a','open_fg2m','open_fg2a', 'open_fg3m','open_fg3a','game_id',
 'drb0_fg2m','drb0_fg2a','drb0_fg3m','drb0_fg3a','drb1_fg2m','drb1_fg2a','drb1_fg3m','drb1_fg3a','drb2_fg2m','drb2_fg2a','drb2_fg3m','drb2_fg3a',
 'drb36_fg2m', 'drb36_fg2a', 'drb36_fg3m', 'drb36_fg3a', 'drb7_fg2m','drb7_fg2a','drb7_fg3m','drb7_fg3a']
        sqlOrd = ['team_id','game_date','game_id','ra_fgm','ra_fga','mid_fgm','mid_fga','lc_fgm','lc_fga','rc_fgm','rc_fga','abv_fgm',
                  'abv_fga','bck_fgm','bck_fga','crn_fgm','crn_fga','wide_fg2m','wide_fg2a','wide_fg3m','wide_fg3a','open_fg2m','open_fg2a',
                  'open_fg3m','open_fg3a','drb0_fg2m','drb0_fg2a','drb0_fg3m','drb0_fg3a','drb1_fg2m','drb1_fg2a','drb1_fg3m','drb1_fg3a',
                  'drb2_fg2m','drb2_fg2a','drb2_fg3m','drb2_fg3a','drb36_fg2m', 'drb36_fg2a', 'drb36_fg3m', 'drb36_fg3a','drb7_fg2m',
                  'drb7_fg2a','drb7_fg3m','drb7_fg3a']
        drb = self.get_opp_dribble_shot(dates)
        sht = self.get_opp_open_shot(dates)
        final = sht.merge(drb,how='inner',on=['GAME_ID','TEAM_ID'])
        if final.shape[0]!=drb.shape[0] | final.shape[0]!=sht.shape[0]:
            print('Error in merging')
        else:
            final.columns = sqlCols
            final = final.filter(sqlOrd)
            self.insert_data(final,'shotsAllowed')
    def update_player_log(self,game_dates):
        '''Pull in prior days game log information for each player.  Will pull in the log, first basket, rebounds and shooting stats for each   player. '''
        log = self.get_logs(game_dates)
        bskt = self.get_first_buckets(games[games.GAME_DATE==date])
        rbs = self.get_rebounds(game_dates)
            
        
        