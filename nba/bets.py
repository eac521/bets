import sqlite3
import pandas as pd
import numpy as np
import re
from nba_api.stats.endpoints import BoxScoreAdvancedV3,PlayByPlayV2,BoxScoreSummaryV2,LeagueDashTeamShotLocations,LeagueDashOppPtShot,LeagueDashPlayerShotLocations,PlayerGameLogs,TeamInfoCommon,leaguegamefinder,LeagueDashPtStats,PlayerIndex,CommonPlayerInfo, PlayerAwards
from nba_api.stats.static import teams
import time
from tqdm import tqdm

class nba:

    def __init__(self):
        self.db = './nba/data/database/nba.db'
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        self.showTables = pd.read_sql("SELECT * FROM sqlite_master WHERE type='table';",self.conn)
        self.headers = {
                 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
}
    def get_start_dates(self):
        return ['2017-10-17','2018-10-16','2019-10-22','2021-10-19','2022-10-18','2023-10-24']
        
    def get_last_game(self,table):
        '''This will get the last game loaded in the table
        '''
        return pd.read_sql('select max(game_date) as date from {}'.format(table),db).date[0]
        
    def get_awards(self,pid):
        '''Get the Most Improved, MVP, DPOY, All-NBA, All-D and All-stars appearances, will get one row per player with each appearance listed
        Inputs: player id
        output: dataframe row with awards and each value a list of appearances
        '''
        awards = PlayerAwards(pid).get_data_frames()[0]
        awards['DESCRIPTION'] = awards.DESCRIPTION + awards.fillna('').ALL_NBA_TEAM_NUMBER
        df = pd.pivot(awards[awards.DESCRIPTION.str.contains('All-[NBA|Star]|Most|Defensive')].groupby(['PERSON_ID','DESCRIPTION']).agg({'SEASON':[list]}).reset_index(),
         index=['PERSON_ID'],columns = 'DESCRIPTION').reset_index()
        df.columns = ['{}'.format(col[2]) if col[2]!='' else col[0] for col in df.columns ]

        df = df.filter(['PERSON_ID','NBA All-Star','All-NBA1','All-NBA2',
          'All-NBA3','All-Defensive Team1','All-Defensive Team2','NBA Most Improved Player(null)',
          'NBA Defensive Player of the Year', 'NBA Most Valuable Player','NBA Finals Most Valuable Player'])
        df = df.fillna('None')
        for col in df.columns:
            if col!='PERSON_ID':
                df[col] = [','.join(x) if x!='None' else x for x in df[col]]
        time.sleep(np.random.choice(range(2,5)))
        return df
    
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
        curdb = [x[0] for x in self.conn.execute('select distinct player_id from players').fetchall()]
        final = pd.DataFrame()
        plyers = []
        sqlord = ['PERSON_ID','DISPLAY_FIRST_LAST','HEIGHT','WEIGHT','POSITION','DRAFT_YEAR','DRAFT_NUMBER','BIRTHDATE',
                  'NBA All-Star', 'All-Defensive Team1', 'All-Defensive Team2', 'All-NBA1','All-NBA2','All-NBA3',
                  'NBA Most Valuable Player', 'NBA Finals Most Valuable Player','NBA Defensive Player of the Year',
                  'NBA Most Improved Player(null)']
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
            if ct % 500==0 and ct != 0:
                time.sleep(300)
            if ct % 250 ==0 and ct != 0:
                final.to_pickle('nba/data/pickle/awards.pkl')
        for col in sqlord:
            if col not in final.columns:
                final[col] = None
        
        self.insert_data(final.filter(sqlord),'players')
        return final
            
    def get_first_buckets(self,game_dates):
        l = []
        print('getting first buckets : at {}'.format(time.strftime('%H:%M')))
        games = self.get_games(min(game_dates),max(game_dates))
        if len(game_dates)>3:
               gameids = games[games.GAME_DATE.isin(game_dates)].GAME_ID.unique()
        else:
               gameids = games.GAME_ID.unique() 
        for ct,gameid in enumerate(tqdm(gameids)):
            df = PlayByPlayV2(gameid).get_data_frames()[0]
            aind = df[(df.EVENTMSGTYPE==1) & (df.HOMEDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            hind = df[(df.EVENTMSGTYPE==1) & (df.VISITORDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            gd = {'gameid':gameid,'homePlayer':df.iloc[hind].PLAYER1_ID,
                  'awayPlayer':df.iloc[aind].PLAYER1_ID,
                  'firstPlayer':df.iloc[min([aind,hind])].PLAYER1_ID}
            bskts = set([(gd['gameid'],v,1,1)  if list(gd.values()).count(v) ==2 else (gd['gameid'],v,1,0) for k,v in gd.items() if k!='gameid'])
            #bdf = pd.DataFrame(bskts,columns = ['GAME_ID','PLAYER_ID','teamFirst','gameFirst'])
            l.append(bskts)
            time.sleep(np.random.choice(range(1,5)))

        df = pd.DataFrame([x for y in l for x in y],columns = ['GAME_ID','PLAYER_ID','team_first','game_first'])
        print('\tcompleted at {}'.format(time.strftime('%H:%M')))
        return df 

        
    def get_summary(self,game_ids):
        '''Will get information for each game related to the team, pts per quarter, team advance stats, inactive players and home team
        Inputs: List of game_ids
        Output: write to sqlite table and a df
        '''
        missing = []
        sqlorder = ['GAME_ID','TEAM_ID','inactive','count_inactive','assistPercentage','offensiveRating',
                    'defensiveRating','pace','possessions','offensiveReboundPercentage','defensiveReboundPercentage','home',
                   'PTS_QTR1','PTS_QTR2','PTS_QTR3','PTS_QTR4']
        df = pd.DataFrame()
        for ct,gameid in enumerate(tqdm(game_ids)):
            try:
                b = BoxScoreSummaryV2(game_id=gameid).get_data_frames()
                inAct = b[3].groupby(['TEAM_ID']).PLAYER_ID.agg([list,'count']).reset_index()
                inAct['list'] = inAct.list.apply(lambda x:','.join([str(ply) for ply in x]))
                inAct = inAct.rename(columns = {'list':'inactive','count':'count_inactive'})
                adv = BoxScoreAdvancedV3(gameid).get_data_frames()[1].filter(['gameId', 'teamId','assistPercentage','offensiveRating','defensiveRating','pace','possessions','offensiveReboundPercentage',
                'defensiveReboundPercentage']).rename(columns={'gameId':'GAME_ID','teamId':'TEAM_ID'})
                scoringdf = b[5].filter([col for col in b[5].columns if (col.find('PTS_')>-1) & (b[5][col].sum()!=0)]+['GAME_ID','TEAM_ID'])
                home = b[7].filter(['GAME_ID','HOME_TEAM_ID'])
                advHome = adv.merge(home,how='left',on='GAME_ID')

                advHome.HOME_TEAM_ID = np.where(advHome.HOME_TEAM_ID == advHome.TEAM_ID,1,0)
                advHome = advHome.rename(columns = {'HOME_TEAM_ID':'home'})
                advInact = inAct.merge(advHome,how='left',on=['TEAM_ID'])
                final = advInact.merge(scoringdf,how='left',on=['TEAM_ID','GAME_ID'])
                df = pd.concat([df,final])
                if ct % 10 == 0:
                    time.sleep(np.random.choice(range(2,25)))
                df.to_pickle('nba/data/pickle/teamlog5.pkl')
                if ct % 150 == 0 and ct != 0 :
                    time.sleep(155+ct)
            except:
                missing.append(gameid)
        print(missing)
        self.insert_data(df.filter(sqlorder),'teamLog')
        return final
        
    def get_opp_open_shot(self,game_dates):
        '''get the type of shots (ranges) that a team allows, will also get the number of wide-open and open 2 and 3pt looks a team allows.  This needs to be done day-by-day as the granularity is only by team, so we can not get game-by-game information.
        Inputs: needs a list of game dates
        output: DataFrame containing each game and the number of open 2pt/3pt shots, number of wide open 2pt/3pt shots and the shot distribution by ranges
        '''
        #team defense shooting
        final = pd.DataFrame()
        for ct,date in enumerate(game_dates):
            d = pd.to_datetime(date)
            season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
            oppShots = LeagueDashTeamShotLocations(measure_type_simple='Opponent',
                                              date_from_nullable = date,
                                               date_to_nullable = date,
                                           season=season
                               ).get_data_frames()[0]
            oppShots.columns = ['{}_{}'.format(re.sub(' |-','_',a),b) if a!='' else b for a,b in oppShots.columns]
            oppShots = oppShots.filter([col for col in oppShots.columns if re.search('_PCT$|NAME|^Backcourt',col)==None])
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
        return final.reset_index(drop=True)
            
    def get_games(self,minDate,maxDate):
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
        return games

    def get_advanced_box(self,game_dates):
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
            advbox = BoxScoreAdvancedV3(gid).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
            advbox = advbox.filter(advcols)
            df = pd.concat([df,advbox])
            time.sleep(np.random.choice(range(1,5)))
        print('completed adv box at {}'.format(time.strftime('%H:%M')))
        return df
        
    def get_opp_dribble_shot(self,game_dates):
        '''will be used to create shots allowed by team
        Input(s): list of game dates
        Output  : dataframe at the the level of each team game, will have columns for each area for each dribble type 0,1,2,3-6 and 7+ dribbles 
        '''
        
        
        drib = ['0 Dribbles','1 Dribble','2 Dribbles','3-6 Dribbles','7+ Dribbles']
        games = self.get_games(min(game_dates),max(game_dates))
        final = pd.DataFrame()
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
            drb = drb.merge(games,how='left',on=['TEAM_ID'])
            drb = drb.groupby(['TEAM_ID','GAME_ID','GAME_DATE']).sum().reset_index()
            final = pd.concat([final,drb])
        return final

    
    def get_player_shot_spots(self,game_dates):
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
                                         season=season).get_data_frames()[0]
            df = self.clean_shotcolumns(sht)
            df['GAME_DATE'] = date
            final = pd.concat([final,df])
            #temp = temp[temp.GAME_DATE==date]
            #final = df.merge(temp,how='left',on=['GAME_DATE','TEAM_ID'])
        print('completed player shots at {}'.format(time.strftime('%H:%M')))
        return final
    
    def get_logs(self,game_dates,seasons=None):
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
                seasonLog = PlayerGameLogs(season_nullable = season).get_data_frames()[0][logCols]
                seasonLog = seasonLog.filter(logCols)
                seasonLog.GAME_DATE = seasonLog.GAME_DATE.apply(lambda x: x[:10])
                final = pd.concat([final,seasonLog])
        else:
            
            season = '{}-{}'.format(minD.year,str(minD.year+1)[-2:]) if minD.month>=10 else '{}-{}'.format(minD.year-1,str(minD.year)[-2:])
            seasonLog = PlayerGameLogs(date_from_nullable = minD.strftime('%m/%d/%Y'),
                                       date_to_nullable = maxD.strftime('%m/%d/%Y'),
                                      season_nullable = season).get_data_frames()[0][logCols]
            seasonLog = seasonLog.filter(logCols)
            seasonLog.GAME_DATE = seasonLog.GAME_DATE.apply(lambda x: x[:10])
            seasonLog = seasonLog[seasonLog.GAME_DATE.isin(game_dates)]
            return seasonLog
        
         
        print('ended player logs at {}'.format(time.strftime('%H:%M')))
        return final
    
    def get_rebounds(self,game_dates):
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
            
    def update_shots_allowed(self,game_dates):
        '''Pull in prior day's data for team's shot types allowed.  Will need the data and will use the get_opp_dribble_shots and get_opp_op_shot,
        merge them and insert the new data in the database
        '''
        sqlCols = ['team_id','ra_fgm','ra_fga','paint_fgm','paint_fga','mid_fgm','mid_fga','lc_fgm','lc_fga','rc_fgm','rc_fga','abv_fgm','abv_fga','bck_fgm','bck_fga','crn_fgm','crn_fga','game_date','wide_fg2m','wide_fg2a','wide_fg3m','wide_fg3a','open_fg2m','open_fg2a', 'open_fg3m','open_fg3a','game_id','drb0_fg2m','drb0_fg2a','drb0_fg3m','drb0_fg3a','drb1_fg2m','drb1_fg2a','drb1_fg3m','drb1_fg3a','drb2_fg2m','drb2_fg2a','drb2_fg3m','drb2_fg3a','drb36_fg2m', 'drb36_fg2a', 'drb36_fg3m', 'drb36_fg3a','drb7_fg2m','drb7_fg2a', 'drb7_fg3m','drb7_fg3a']
        sqlOrd = ['team_id','game_date','game_id','ra_fgm','ra_fga','paint_fgm','paint_fga','mid_fgm','mid_fga','lc_fgm','lc_fga','rc_fgm','rc_fga','abv_fgm','abv_fga','bck_fgm','bck_fga','crn_fgm','crn_fga','wide_fg2m','wide_fg2a','wide_fg3m','wide_fg3a','open_fg2m', 'open_fg2a','open_fg3m','open_fg3a','drb0_fg2m','drb0_fg2a','drb0_fg3m','drb0_fg3a','drb1_fg2m','drb1_fg2a','drb1_fg3m', 'drb1_fg3a','drb2_fg2m','drb2_fg2a','drb2_fg3m','drb2_fg3a','drb36_fg2m', 'drb36_fg2a','drb36_fg3m','drb36_fg3a','drb7_fg2m',
    'drb7_fg2a','drb7_fg3m','drb7_fg3a']
        drb = self.get_opp_dribble_shot(game_dates)
        sht = self.get_opp_open_shot(game_dates)
        final = sht.merge(drb,how='inner',on=['GAME_DATE','TEAM_ID'])

        if final.shape[0]!=drb.shape[0] | final.shape[0]!=sht.shape[0]:
            print('Error in merging')
        else:
            final.columns = sqlCols
            final = final.filter(sqlOrd)
            self.insert_data(final,'shotsAllowed')
            
            
    def update_player_log(self,game_dates,seasons=None):
        '''Pull in prior days game log information for each player.  Will pull in the log, first basket, rebounds and shooting stats for each   player. '''
        #get the individual dataframes
        log = self.get_logs(game_dates,seasons) #has all 4
        bskt = self.get_first_buckets(game_dates) #playerid,gameid
        rbs = self.get_rebounds(game_dates) #all 4
        adv = self.get_advanced_box(game_dates) # gameid,playerid
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
         'usagePercentage', 'pace', 'possessions','team_first', 'game_first']
        final = final.filter(pd.read_sql('select * from plyrLogs limit 1',self.conn).columns.values)
        self.insert_data(final,'plyrLogs')
        
        return final

        
        