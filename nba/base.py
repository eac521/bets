 import sqlite3
 import pandas
 
 def __init__(self):
        self.db = './data/database/nba.db'
        self.conn = sqlite3.connect(self.db)
        self.cur = self.conn.cursor()
        self.showTables = pd.read_sql("SELECT * FROM sqlite_master WHERE type in ('table','view');",self.conn)
        self.headers = {
                 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'
}
        #self.players = pd.read_sql('select * from roster_view')
        self.teams = pd.read_sql('select team_id,teamAbrv from teams',self.conn)
        
        
    def derive_season(self,date):
        '''Get date as a string value and determine the season.
           Inputs: Date as YYYY-MM-DD
           output: Season as YYYY-YY
        '''
        if isinstance(date,str):
            date = pd.to_datetime(date)
        if d.month <=9:
            return '{:%Y}-{:%y}'.format(pd.to_datetime(d)-  pd.to_timedelta(365.25,'days'),pd.to_datetime(d))
        else:
            return '{:%Y}-{:%y}'.format(pd.to_datetime(d),pd.to_datetime(d) + pd.to_timedelta(365.25,'days'))
    
    def insert_data(self,data,table):
        '''Simply writes the data insto the table, needs to be in the correct order
        Inputs: pandas DataFrame, string of table name
        ouput: Response that table has been uploaded
        '''
        # cols = pd.read_sql('''
        # PRAGMA table_info({})
        # '''.format(table),self.conn).name.values
        rows = data.shape[0]
        v = '?' + ',?' * (data.shape[1] -1)
        self.cur.executemany('''insert into {t} values ({v})'''.format(t=table,v=v),data.values.tolist())
        self.conn.commit()
        self.conn.close()
        return print('{} has been updated with {:,} rows'.format(table,rows))
        