NAME_MAP = {
    'Herb Jones': 'Herbert Jones',
    'Alex Sarr': 'Alexandre Sarr',
    'Tristan da Silva': 'Tristan Da Silva',
    'Derrick Jones Jr': 'Derrick Jones',
    'KyShawn George': 'Kyshawn George',
    'Isaiah Stewart': 'Isaiah Stewart II',
    'Ronald Holland II': 'Ron Holland',
}
derived_tables = {
    'opp_data': {
        'file': 'nba/data/sql/opp_data.sql',
        'indexes': [('idx_opp_data', 'game_id, opp_id')]
    },
    'pgames': {
        'file': 'nba/data/sql/derivPgamesTable.sql',
        'indexes': [
            ('idx_pgames_player_season_date', 'player_id, season, game_date'),
            ('idx_pgames_player_date', 'player_id, game_date')
        ]
    }
}