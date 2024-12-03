with teamInfo as (
    SELECT sum(abv_fga) as teamAbvFga, sum(crn_fga) as teamCrnFga, sum(ra_fga) as teamRaFga, sum(mid_fga) as teamMidFga, sum(paint_fga) as teamPaintFga, 
CREATE VIEW pgames AS
SELECT 
--identifiying information
name, teamAbrv as team,season,tmGameCt,
--players_game
RANK() OVER(PARTITION BY player_id,season ORDER BY game_date) plyrGameCt,

--player demo information
height, SUBSTR(season,1,4) - draft_year exp, (JULIANDAY(substr(season,1,4) || '-10-15') - JULIANDAY(birthday)) / 365.25 age, 
--game information if home and opponent shooting allowed
home,plogs.*,
 
--last games played
julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) - 1 daysBetweenGames,
(
case when julianday(game_date) - julianday(lag(game_date,5) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,4) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end + 
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end) + 1 
    gamesInFive,
(
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end) + 1 gamesInThree,

--threes made
coalesce(lc_fgm,0) + coalesce(rc_fgm,0) + coalesce(abv_fgm,0) as threesMade,

--percentages coalesce as 1 on denom only to avoid errors
(coalesce(lc_fgm,0) + coalesce(rc_fgm,0) + coalesce(abv_fgm,0)) / (coalesce(lc_fga,1) + coalesce(rc_fga,1) + coalesce(abv_fga,1)) thrPtPrct,
coalesce(ftm,0)/coalesce(fta,1) ftPrct,

-- aggregated season / rolling stats
AVG(pts) OVER (PARTITION BY player_id,season ORDER BY game_date) curPPG,
AVG(pts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 83 and 1 PRECEDING) rolPPG,

-- differences between team and player
offensiveRating - teamOffRating as marginOffRating,
pace - teamPace as marginPace,


--opponent information defined in subquery below
opp.* 
    
    
from plyrLogs plogs
JOIN 
    (SELECT team_id,season,game_id,home,off_rate as teamOffRating,pace as teamPace,
    RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) tmGameCt 
    from teamLog) tm USING (team_id,game_id) 
JOIN teams tms USING (team_id)
join players ply USING (player_id)
join 
INNER JOIN 
--get opponent shot profile
    (SELECT team_id as opp_id,game_id ,ra_fga as ra_fgallowed, paint_fga as paint_fgallowed, mid_fga as mid_fgallowed, lc_fga as lc_fgallowed, rc_fga as rc_fgallowed, abv_fga as abv_fgallowed, open_fg3a, wide_fg3a, open_fg2a, wide_fg2a, games_in_five as oppGamesFive, games_in_three as oppGamesThree, daysBetweenGames as oppDaysLastGame,pace as oppPace,open3_rate, wide3_rate, open2_rate, wide2_rate,count_inactive,

--to be used to help replace missing values, will be deleted
    --moving/season averages
    AVG(pace) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppPace,
    AVG(def_rate) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppDefRating,
,
    
--rolling averages on rate stats
    SUM(open_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppOpen3,
    SUM(wide_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppWide3,   

--season averages    
    AVG(pace) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS UNBOUNDED PRECEDING) AS seasonOppPace,
       AVG(def_rate) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS UNBOUNDED PRECEDING) AS seasonOppDefRating,
    
--season averages on rate stats
    SUM(open_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppOpen3,
    
    SUM(wide_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppWide3
 
    FROM team_def) opp on opp.game_id = plogs.game_id and plogs.team_id <> opp.opp_id
