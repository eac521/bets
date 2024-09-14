
CREATE VIEW pgames AS
SELECT 
--identifiying information
name, teamAbrv as team, game_number,season,
--game information if home and opponent shooting allowed
home,opp.*,plogs.*,
--last games played
julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) - 1 daysBetweenGames,
(
case when julianday(game_date) - julianday(lag(game_date,5) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,4) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end + 
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 5 then 1 else 0 end) + 1 games_in_five,
(
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) < 3 then 1 else 0 end) + 1 games_in_three,
--dependent variable
lc_fgm + rc_fgm + abv_fgm as threesMade,

--moving min/avg of threes    
avg(lc_fgm + rc_fgm + abv_fgm) OVER (PARTITION BY season,player_id
    ORDER BY game_number ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mvAvgThrees,
min(lc_fgm + rc_fgm + abv_fgm) OVER (PARTITION BY season,player_id
    ORDER BY game_number ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS mvMinThrees
--setting up percentiles for shot attempts

from plyrLogs plogs
JOIN 
    (SELECT team_id,season,game_id,home,
    RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) game_number   
    from teamLog) tm USING (team_id,game_id) 
JOIN teams tms USING (team_id)
join players ply USING (player_id)
INNER JOIN 
--get opponent shot profile
    (SELECT team_id as opp_id,game_id, ra_fga as ra_fgallowed, paint_fga as paint_fgallowed, mid_fga as mid_fgallowed, lc_fga as lc_fgallowed, rc_fga as rc_fgallowed, abv_fga as abv_fgallowed, open_fg3a, wide_fg3a, open_fg2a, wide_fg2a, games_in_five as opp_gm_five, games_in_three as opp_gm_three, daysBetweenGames as oppDaysLastGame
    --oepn shot rates
    open_fg3a / (lc_fga + rc_fga + abv_fga) as open3_rate,
    wide_fg3a / (lc_fga + rc_fga + abv_fga) as wide3_rate
    --rolling factors
    avg(team_def) OVER (PARTITION BY season,team_id ORDER BY game_number ROWS BETWEEN 5 and CURRENT ROW) as mvAvgOppDRating
    FROM team_def) opp on opp.game_id = plogs.game_id and plogs.team_id <> opp.opp_id
INNER JOIN
--
