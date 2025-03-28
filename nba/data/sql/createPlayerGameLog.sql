CREATE TABLE plyrLogs (player_id TEXT,
team_id TEXT,
game_id TEXT,
game_date TEXT,
min FLOAT,
ftm INT,
fta INT,
reb INT,
ast INT,
tov INT,
stl INT,
blk INT,
blka INT,
pf INT,
pfd INT,
pts INT,
plus_minus INT,
dd2 INT,
td3 INT,
offensiveRating FLOAT,
defensiveRating FLOAT,
usagePercentage FLOAT,
pace FLOAT,
possessions INT,
team_first INT,
game_first INT,
oreb INT,
oreb_contest INT,
oreb_chances INT,
oreb_chance_defer INT,
avg_oreb_dist INT,
dreb INT,
dreb_contest INT,
dreb_chances INT,
dreb_chance_defer INT,
avg_dreb_dist INT,
ra_fgm INT,
ra_fga INT,
paint_fga INT,
paint_fgm INT,
mid_fgm INT,
mid_fga INT,
lc_fgm INT,
lc_fga INT,
rc_fgm INT,
rc_fga INT,
abv_fgm INT,
abv_fga INT,
started INT,
 PRIMARY KEY (player_id,game_id) )