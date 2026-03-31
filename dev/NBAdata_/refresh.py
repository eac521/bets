class data(base):
    def __init__(self):
        super().__init__()

def derive_opp_data_table(self):
    self.cur.execute(open('../../nba/data/sql/derive_opp_table.sql', 'r').read())
    self.cur.execute("CREATE INDEX IF NOT EXISTS idx_opp_data ON opp_data(game_id, opp_id)")
    self.conn.commit()
    print('opp_table derived')

def refresh_opp_data(self):
    """Rebuild opp_data from source tables"""
    print("Refreshing opp_data...")
    self.cur.execute("DROP TABLE IF EXISTS opp_data")
    self.derive_opp_data_table()

def refresh_views(self ,view_file):
    print("Refreshing views...")
    self.cur.execute(open(view_file ,'r').read())


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