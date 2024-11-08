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