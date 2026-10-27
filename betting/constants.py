# Bookmakers pulled from The Odds API, keyed by API bookmaker key.
# role: primary = best book is chosen between these (no limits)
#       secondary = bettable, shown when it beats the primaries (limited stakes)
#       info = pulled for pricing / consensus only, never recommended
# Every group of 10 bookmakers is billed as one region: an 11th book doubles every call.
# Same 10 books as the historical backfill (HIST_BOOKS in nba/NBAodds.py), so live and historical prices stack.
books = {
    'draftkings': {'col_prefix': 'DraftKings', 'odds_col': 'DraftKings', 'role': 'primary'},
    'fanduel': {'col_prefix': 'FanDuel', 'odds_col': 'FanDuel', 'role': 'primary'},
    'betmgm': {'col_prefix': 'BetMgm', 'odds_col': 'BetMGM', 'role': 'primary'},
    'fanatics': {'col_prefix': 'fanatics', 'odds_col': 'Fanatics', 'role': 'secondary'},
    'espnbet': {'col_prefix': 'espn', 'odds_col': 'theScore_Bet', 'role': 'info'},
    'williamhill_us': {'col_prefix': 'Caesars', 'odds_col': 'Caesars', 'role': 'info'},
    'pinnacle': {'col_prefix': 'Pinnacle', 'odds_col': 'Pinnacle', 'role': 'info'},
    'kalshi': {'col_prefix': 'Kalshi', 'odds_col': 'Kalshi', 'role': 'info'},
    'betrivers': {'col_prefix': 'BetRivers', 'odds_col': 'BetRivers', 'role': 'info'},
    'novig': {'col_prefix': 'Novig', 'odds_col': 'Novig', 'role': 'info'},
}
assert len(books) <= 10, 'An 11th bookmaker doubles the cost of every odds call'

# Markets in every live snapshot (cost = markets returned x 1 region per game)
live_markets = [
    'player_threes', 'player_threes_alternate',
    'player_points', 'player_points_alternate',
    'player_points_rebounds_assists', 'player_points_rebounds_assists_alternate',
    'player_first_basket', 'player_first_team_basket', 'player_points_q1',
]
