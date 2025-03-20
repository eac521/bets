##Purpose of file is to keep a running doc of functions used in this environment that are not kept in the notebook/scripts but can be useful in the future
def update_player_shots():
    shotFiles = glob.glob('data/pickle/plyrShots20*')
    cols = ['Restricted_Area_FGM', 'Restricted_Area_FGA',
       'In_The_Paint_(Non_RA)_FGM', 'In_The_Paint_(Non_RA)_FGA',
       'Mid_Range_FGM', 'Mid_Range_FGA', 'Left_Corner_3_FGM',
       'Left_Corner_3_FGA', 'Right_Corner_3_FGM', 'Right_Corner_3_FGA',
       'Above_the_Break_3_FGM', 'Above_the_Break_3_FGA','PLAYER_ID','GAME_DATE']
    missing = []
    for seas in msShots.season.unique():
        print('going through {}'.format(seas))
        temp = msShots[msShots.season==seas][['player_id','game_date']].values
        pfile = pd.DataFrame([y for x in pd.read_pickle([f for f in shotFiles if f.find(seas)>-1][0]) for y in x])
        for vals in temp:
            upload = pfile[(pfile.PLAYER_ID.astype(str)==vals[0]) & (pfile.GAME_DATE.astype(str)==vals[1])][cols].values
            try:
                nba.conn.execute('''UPDATE plyrLogs SET ra_fgm = ?, ra_fga = ?, paint_fgm = ?,paint_fga = ?, mid_fgm = ?, mid_fga =?, 
                                lc_fgm = ?, lc_fga = ?, rc_fgm = ?, rc_fga = ?, abv_fgm = ?, abv_fga = ?
                                where player_id = ? and game_date = ?''',*upload)
                nba.conn.commit()
            except:
                print('\t{} on {} not in dataset'.format(*vals))
                missing.append(vals)


def reloadPlayerData
'''Have accidently deleted player data go through this process.  Usually just need to set a filter to today to not get insertion error.
Starter has been skipped because it needs updated, will have corrected in future.
need to run some individually because running for all games is very excessive.  
can copy and paste this in to use again, some manual processing with this 
'''
from nba_api.stats.endpoints import BoxScoreAdvancedV3
temp = pd.DataFrame()
advcols = ['GAME_ID','PLAYER_ID','offensiveRating','defensiveRating','usagePercentage','pace','possessions']
for gid in tqdm(gd.GAME_ID.unique()):
    advbox = BoxScoreAdvancedV3(gid).get_data_frames()[0].rename(columns={'gameId':'GAME_ID','personId':'PLAYER_ID'})
    advbox = advbox.filter(advcols)
    temp = pd.concat([temp,advbox])
shts = pd.DataFrame()
for date in tqdm(gds.GAME_DATE.unique()):
    d = pd.to_datetime(date)
    season = '{}-{}'.format(d.year,str(d.year+1)[-2:]) if d.month>=10 else '{}-{}'.format(d.year-1,str(d.year)[-2:])
    tempsht = LeagueDashPlayerShotLocations(date_from_nullable = date,
                                 date_to_nullable = date,
                                     season=season).get_data_frames()[0]
    
    tempsht = nba.clean_shotcolumns(tempsht)
    tempsht['GAME_DATE'] = date
    shts = pd.concat([shts,tempsht])
    if np.random.randint(0,100) % 5 == 0:
        time.sleep(np.random.choice(range(7,22)))

l = []
for ct,gameid in enumerate(tqdm(gd.GAME_ID.unique())):
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

fbckt = pd.DataFrame([x for y in l for x in y],columns = ['GAME_ID','PLAYER_ID','team_first','game_first'])
#log = nba.get_logs(gd,['2021-22','2022-23','2023-24','2024-25']) #has all 4

#rbs = nba.get_rebounds(gd) #all 4

#merge dataframes together
logrbs = log.merge(rbs,how='left',on=['PLAYER_ID','GAME_ID','TEAM_ID','GAME_DATE']).fillna(0)
logRbsSht = logrbs.merge(shts,how='left',on=['TEAM_ID','PLAYER_ID','GAME_DATE'])
advBskt = adv.merge(fbckt,how='left',on = ['PLAYER_ID','GAME_ID'])
final = logRbsSht.merge(advBskt,how='left',on=['PLAYER_ID','GAME_ID'])
final['Starter'] = 0

final.columns = ['player_id','team_id','game_id','game_date','min','ftm','fta','reb','ast','tov','stl','blk','blka','pf',
 'pfd','pts','plus_minus','dd2','td3','oreb','oreb_contest','oreb_chances','oreb_chance_defer','avg_oreb_dist','dreb',
 'dreb_contest','dreb_chances','dreb_chance_defer','avg_dreb_dist','ra_fgm','ra_fga', 'paint_fgm', 'paint_fga','mid_fgm',
 'mid_fga', 'lc_fgm','lc_fga', 'rc_fgm','rc_fga','abv_fgm', 'abv_fga', 'offensiveRating','defensiveRating',
 'usagePercentage', 'pace', 'possessions','team_first', 'game_first','Starter']
final = final[(final.game_date<='2025-02-02') & (final.player_id.isin([1631108, 203076, 1629029, 1628467]))].filter(pd.read_sql('select * from plyrLogs limit 1',nba.conn).columns.values)
# if (pd.read_sql("select count(*) as ct  from plyrLogs where game_date in ({})".format(strD,strD),nba.conn).sum()>0).all():
#     nba.conn.execute("DELETE FROM plyrLogs where game_date in ({})".format(strD))
#     nba.conn.commit()
nba.insert_data(final,'plyrLogs')


### find what wasserstein scoring looks like for first 5,10,15,20 games compared to the preseason historicals
def wasDistance()
    from collections import defaultdict
    wassDict = defaultdict(dict)
    testSeason = ['2021-22','2022-23','2023-24']
    gmct = [6,11,16,21]
    final = pd.DataFrame()
    for seas in testSeason:
        plist = thr[thr.season==seas].player_id.unique()
        hist = thr[(thr.season<seas) & (thr.player_id.isin(plist))].groupby('player_id').abv_fga.quantile(quants,'midpoint').to_frame('abv_hist')
        lst60 = thr[(thr.season<seas) & (thr.player_id.isin(plist)) & (thr.plyrGameCt>10)].groupby('player_id').abv_fga.quantile(quants,'midpoint').to_frame('abv_remain')
        for gm in gmct:
            hist['abv_curr_{}'.format(gm)] = thr[(thr.season==seas) & (thr.plyrGameCt < gm)].groupby('player_id').abv_fga.quantile(quants,'midpoint')
        temp = hist.reset_index().drop(['level_1'],axis=1)
        temp['season'] = seas
        temp = temp.set_index('player_id')
        temp = temp.join(lst60.reset_index().drop(['level_1'],axis=1).set_index('player_id'))
        for col in hist.columns:
            vl = []
            for idx in temp.index:
                vl.append([wasserstein_distance(temp.loc[idx]['abv_remain'].values,temp.loc[idx][col].values)])
            temp['wasScore_{}'.format(col)] = np.array(vl).flatten()
            temp['relWasScore_{}'.format(col)] = temp['wasScore_{}'.format(col)] / temp.groupby('player_id')[col].agg(np.ptp)
        final = pd.concat([final,temp])
    final = final.reset_index().drop_duplicates(subset=['player_id','season'])
def award_counts(demo,cols=None):
    '''
    Inputs: DF at season level, will be using season and the columns to count awards, will default to all awards
    Output: new dataframe with count columns replacings the columns for each instance
    '''
    awards = ['allstars', 'allnba_first','allnba_second', 'allnba_third', 'alld_first', 'alld_second', 'mip',
       'dpoy', 'mvp', 'finals_mvp']
    demo = demo.fillna('N/A')
    if cols == None:
        cols = awards
    lasts = ['allstars', ['allnba_first','allnba_second', 'allnba_third'],['alld_first', 'alld_second'],'mvp','dpoy']
    #This is checking to find the last time they were all-star, all-nba, all-d, mvp or dpoy, goes first to use years
    for lst in lasts:
        if isinstance(lst,str):
            demo['last_'+lst] = [1 if int(season[:4]) - int(max([x for x in team.split(',') if x < season]+['2000-01'])[:4]) < 3 else 0
            for  season,team in zip(demo.season,demo[lst])]
        else:
            name = lst[0][:lst[0].find('_')]
            grp = zip(demo.season,[','.join(x) for x in demo.filter(lst).values])
            demo['last_'+name] = [1 if int(season[:4]) - int(max([x for x in team.split(',') if x < season]+['2000-01'])[:4]) < 3 else 0
            for  season,team in grp]
    #This goes through every award and will update the column with a count instead of the string of instances
    for col in cols:
        demo[col] = [len([a for a in awrd.split(',') if a < season]) for season,awrd in zip(demo.season,demo[col])]
    
    return demo

def update_demo_data(plog):
    plog['season_age'] = [(dt.datetime(int(season[:4]),10,1) - pd.to_datetime(bday)).days/ 365.25 for season,bday in zip(plog.season.values,plog.birthday)]
    plog['exp'] = plog.season.str[:4].astype(int) - plog.draft_year.astype(int)

from nba_api.stats.endpoints import PlayerProfileV2
def update_undrafted_year(pids):
    for pid in tqdm(pids):
        try:
            dy = PlayerProfileV2(pid).get_data_frames()[0].SEASON_ID.min()[:4]
            nba.conn.execute(''' UPDATE players
            SET draft_year = '{}'
            where player_id  = '{}'

            '''.format(dy,pid))
            nba.conn.commit()
        except:
            print('No Data for: {}'.format(demo[demo.player_id == pid].name))
#for season avg of team shoot allowed data
seasAvg = tm.groupby('season')[col].quantile([1/6,1/3,.5,2/3,5/6,1],'midpoint').reset_index().pivot(index='season',columns='level_1').shift(1)

#Pulling in if someone is a startrer
temp = pd.concat(GameRotation(gid).get_data_frames())
d[gid] = temp[temp.IN_TIME_REAL==0].PERSON_ID.values.tolist()