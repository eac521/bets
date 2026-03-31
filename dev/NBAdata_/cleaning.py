import pandas as pd

def threes(self, df):
    '''
    specifically to replace nans in the threes dataset
    Inputs: the completed Threes data set with distibuition information
    Ouput: dataframe removing nans
    '''
    lgAvgs = pd.read_sql('''select season,
                                   sum(open_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga)) open_fg3aLgSeason,
                                   sum(wide_fg3a) * 1.0 / (sum(abv_fga) + sum(lc_fga) + sum(rc_fga)) wide_fg3aLgSeason,
                                   avg(pace)     as                                                  paceLgSeason,
                                   avg(def_rate) as                                                  def_rateLgSeason
                            from team_def
                            group by season
                            HAVING season is not Null
                         ''', self.conn).set_index('season').reset_index().shift()

    df.loc[:, 'mvAvgThrees'] = df['mvAvgThrees'].fillna(df['past3AvgThrees'])
    df.loc[:, 'mvAvgUsage'] = df['mvAvgUsage'].fillna(df['past3Usage'])
    df.loc[:, 'mvAvgOffRating'] = df['mvAvgOffRating'].fillna(df['past3OffRating'])
    df.loc[:, 'mvAvgFtPrct'] = df['mvAvgFtPrct'].fillna(df['past3FtPrct'])
    df.loc[:, 'mvAvgThrPtPrct'] = df['mvAvgThrPtPrct'].fillna(df['past3ThrPtPrct'])
    df.loc[:, 'seasonUsage'] = df['seasonUsage'].fillna(df['past3Usage'])
    df.loc[:, 'seasonOffRating'] = df['seasonOffRating'].fillna(df['past3OffRating'])
    df.loc[:, 'seasonFtPrct'] = df['seasonFtPrct'].fillna(df['past3FtPrct'])
    df.loc[:, 'seasonThrPtPrct'] = df['seasonThrPtPrct'].fillna(df['past3ThrPtPrct'])
    idx = df.index

    df = df.merge(lgAvgs, how='left', on=['season'])
    df.index = idx
    df['mvAvgOppPace'] = df['mvAvgOppPace'].fillna(df['paceLgSeason'])
    df['seasonOppPace'] = df['seasonOppPace'].fillna(df['paceLgSeason'])
    df['mvAvgOppOpen3'] = df['mvAvgOppOpen3'].fillna(df['open_fg3aLgSeason'])
    df['seasonOppOpen3'] = df['seasonOppOpen3'].fillna(df['open_fg3aLgSeason'])
    df['seasonOppWide3'] = df['seasonOppWide3'].fillna(df['wide_fg3aLgSeason'])
    df['mvAvgOppWide3'] = df['mvAvgOppWide3'].fillna(df['wide_fg3aLgSeason'])
    df['mvAvgOppDefRating'] = df['mvAvgOppDefRating'].fillna(df['def_rateLgSeason'])
    df['seasonOppDefRating'] = df['seasonOppDefRating'].fillna(df['def_rateLgSeason'])
    df.drop([col for col in lgAvgs.columns if col.find('LgSeason') > -1], axis=1, inplace=True)
    df.fillna(0, inplace=True)
    return df
