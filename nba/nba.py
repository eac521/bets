import sqlite3
import pandas as pd
import numpy as np
import re
from nba_api.stats.endpoints import BoxScoreAdvancedV3,PlayByPlayV2,BoxScoreSummaryV2,LeagueDashTeamShotLocations,LeagueDashOppPtShot,LeagueDashPlayerShotLocations,PlayerGameLogs,TeamInfoCommon
from nba_api.stats.static import teams
class data:

    def __init__(self):
        self.db = './nba/data/database/nba.db'
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        self.teams = [d['id'] for d in teams.get_teams()]
        
    def clean_lgdashoppcolumns(df,dribble):
        df = df.filter([col for col in df.columns if (re.search('_FREQUENCY$|PCT$|^G|FGM|FGA',col)==None) &
                                                        (df[col].dtype!=object)] )
        if dribble != False:
            df.columns = ['{}_{}'.format(col,dribble.replace(' ','_')) if col!='TEAM_ID' else col for col in df.columns] 
        return df
    
    def clean_shotcolumns(df):
        df.columns = ['{}_{}'.format(re.sub(' |-','_',a),b) if a!='' else b for a,b in df.columns]
        df = df.filter([col for col in df.columns if (re.search('ID$|^Restricted|^Mid|^In_The|^Right|^Left|^Above',col)!=None) &
                                    (re.search('PCT$',col)==None)])
        return df
    
    #def get_box(self,)
    
    def get_advBox(self,gameids):
        l = []
        for gameid in gameids:
            box = BoxScoreAdvancedV3(game_id = gameid).get_data_frames()[0]
            d = box.filter([col for col in box.columns if (box[col].dtype!=object)])
            d['game_id'] = gameid
            l.append(d.to_dict(orient='records'))
        df = pd.DataFrame([y for x in l for y in x])
        df = df.filter(['personId','game_id','usagePercentage','pace','possessions','offensiveRating','defensiveRating'])
        df = df.rename({'personId':'playerId','game_id':'gameId'})
        return df 

    
    def get_first_buckets(self,gameids):
        l = []
        for gameid in gameids:
            df = PlayByPlayV2(gameid).get_data_frames()[0]
            aind = df[(df.EVENTMSGTYPE==1) & (df.HOMEDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            hind = df[(df.EVENTMSGTYPE==1) & (df.VISITORDESCRIPTION.notna())].PLAYER1_ID.idxmin()
            gd = {'gameid':gameid,'homeFirst':1,'homePlayer':df.iloc[hind].PLAYER1_ID,'awayFirst':1,
                  'awayPlayer':df.iloc[aind].PLAYER1_ID,'first':1,
                  'firstPlayer':df.iloc[min([aind,hind])].PLAYER1_ID}
            l.append(gd)
        temp = pd.DataFrame(l)
        test['teamFirst'] = np.where(temp.homeFirst!=temp['first'],temp.homeFirst,temp.awayFirst)
        test['gameFirst'] = np.where(temp.homeFirst==temp['first'],temp.homeFirst,temp.awayFirst)
        game = temp.filter(['gameid','gameFirst'])
        game['gameFirstBasket'] = 1
        team = temp.filter(['gameid','teamFirst'])
        team['teamFirstBasket'] = 1
        df = pd.concat([game,team])
        df = df.rename(columns = {'gameid':'gameId'})
        df['playerId'] = df['gameFirst'].fillna(df.teamFirst)
        df[['gameId','playerId','gameFirstBasket','teamFirstBasket']]
        return df 

        
    def get_summary(self,gameid):
        b= BoxScoreSummaryV2(game_id=gameid).get_data_frames()
        df1 = b[5]
        df1 = df1.filter([col for col in df1.columns if (df1[col].dtype!=object) & (df1[col].sum()!=0) & (col!='GAME_SEQUENCE') ])
        df = df1.merge(b[1][['TEAM_ID','LARGEST_LEAD','PTS_FB','PTS_2ND_CHANCE','TOTAL_TURNOVERS']])
        df['GAME_ID'] = gameid
        l.append(df.to_dict(orient='records'))
        
    def get_opp_open_shot(self,date):
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
        final = oppShots.merge(wide.merge(op,how='left',on=['TEAM_ID','GAME_DATE']),how='left',on=['TEAM_ID','GAME_DATE'])
        l.append(final.to_dict(orient='records'))
        
        
    def get_opp_dribble_shot(self,dates):
        for date in dates:
            drib = ['0 Dribbles','1 Dribble','2 Dribbles','3-6 Dribbles','7+ Dribbles']
            d = pd.to_datetime(date)
            season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
            drb = pd.DataFrame()
            for dribbleCount in drib:

                drbShots = LeagueDashOppPtShot(date_from_nullable = date,
                                    date_to_nullable = date,
                                               season=season,
                                    dribble_range_nullable=dribbleCount).get_data_frames()[0]
                df = lgDashOppColums(drbShots,dribbleCount)
                df['GAME_DATE'] = date
                temp = gamefinder.get_data_frames()[0][['GAME_DATE','TEAM_ID','GAME_ID']]
                temp = temp[temp.GAME_DATE==date]
                temp = df.merge(temp,how='left',on=['GAME_DATE','TEAM_ID'])
                drb = pd.concat([drb,temp])
                drb = drb.groupby(['TEAM_ID','GAME_ID','GAME_DATE']).sum().reset_index()
        return drb
        
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
    
    def get_logs(self,season):
        '''Expected Input: a list of seasons formatted as YYYY-YY
           Returns: A DataFrame that has each game played by that player and the team, will be used as a base for our gamestats
        '''
        log = PlayerGameLogs(season_nullable=season).get_data_frames()[0]
        srchStr = 'SEASON_YEAR|MATCHUP|^TEAM_ABB|_PCT$|TEAM_NAME|^FG|REB$|_RANK$|WL|NICKNAME|_PTS$'
        df = log.filter([col for col in log.columns if re.search(srchStr,col)==None])
        return df
    
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
        return print('{} has been updated with {:,} rows'.format(table,rows))
            
            
            
        