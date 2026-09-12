-- with teamInfo as (
--     SELECT sum(abv_fga) as teamAbvFga, sum(crn_fga) as teamCrnFga, sum(ra_fga) as teamRaFga, sum(mid_fga) as teamMidFga, sum(paint_fga) as teamPaintFga, 


DROP TABLE IF EXISTS pgames;
CREATE TABLE pgames
    AS
WITH daysSince AS (
    SELECT player_id,game_id,
    julianday(lag(game_date,1) OVER (PARTITION BY player_id ORDER BY game_date)) as game1_date,
    julianday(lag(game_date,2) OVER (PARTITION BY player_id ORDER BY game_date)) as game2_date,
    julianday(lag(game_date,3) OVER (PARTITION BY player_id ORDER BY game_date)) as game3_date,
    julianday(lag(game_date,4) OVER (PARTITION BY player_id ORDER BY game_date)) as game4_date,
    julianday(lag(game_date,5) OVER (PARTITION BY player_id ORDER BY game_date)) as game5_date
    FROM plyrLogs
    )
SELECT 
--identifiying information
name, teamAbrv as team,season,tmGameCt,
--players_game


--player demo information
height,
--(JULIANDAY(substr(season,1,4) || '-10-15') - JULIANDAY(birthday)) / 365.25 age, 
--game information if home and opponent shooting allowed
--first quarter player info
--minFirst,crnFgaFirst,abvFgaFirst,
home,plogs.*,

--last games played
julianday(game_date) - game1_date - 1 daysBetweenGames,
(
case when julianday(game_date) - game1_date < 5 then 1 else 0 end +
case when julianday(game_date) - game2_date < 5 then 1 else 0 end + 
case when julianday(game_date) - game3_date < 5 then 1 else 0 end +
case when julianday(game_date) - game4_date < 5 then 1 else 0 end +
case when julianday(game_date) - game5_date < 5 then 1 else 0 end) + 1 
    gamesInFive,
(
case when julianday(game_date) - game3_date < 3 then 1 else 0 end +
case when julianday(game_date) - game2_date < 3 then 1 else 0 end +
case when julianday(game_date) - game1_date < 3 then 1 else 0 end) + 1 gamesInThree,
--players_game
RANK() OVER(PARTITION BY player_id,season ORDER BY game_date) plyrGameCt,
    
--threes made
coalesce(lc_fgm,0) + coalesce(rc_fgm,0) + coalesce(abv_fgm,0) as threesMade,
(coalesce(lc_fga,0) + coalesce(rc_fga,0) + coalesce(abv_fga,0)) as threesAtt,


--percentages coalesce as 1 on denom only to avoid errors
(coalesce(lc_fgm,0) + coalesce(rc_fgm,0) + coalesce(abv_fgm,0)) * 1.0 / NULLIF(coalesce(lc_fga,0) + coalesce(rc_fga,0) + 
coalesce(abv_fga,0),0) thrPtPrct,
coalesce(ftm,0)/coalesce(fta,1) ftPrct,

-- aggregated season / rolling stats


-- differences between team and player
offensiveRating - teamOffRating as marginOffRating,
pace - teamPace as marginPace,teamPace,teamOffRating,mvAvgTeamPace,


--opponent information defined in subquery below
opp_data.opp_id, opp_data.opponent, opp_data.ra_fgallowed, opp_data.paint_fgallowed,
opp_data.mid_fgallowed, opp_data.lc_fgallowed, opp_data.rc_fgallowed, opp_data.abv_fgallowed,
opp_data.open_fg3a, opp_data.wide_fg3a, opp_data.open_fg2a, opp_data.wide_fg2a,
opp_data.oppGamesFive, opp_data.oppGamesThree, opp_data.oppDaysLastGame, opp_data.oppPace,
opp_data.open3_rate, opp_data.wide3_rate, opp_data.open2_rate, opp_data.wide2_rate,
opp_data.count_inactive, opp_data.mvAvgOppPace, opp_data.mvAvgOppDefRating,
opp_data.mvAvgOppOpen3, opp_data.mvAvgOppOpen3Rate, opp_data.mvAvgOppWide3,
opp_data.mvAvgOppWide3Rate, opp_data.mvGood3Rate, opp_data.seasonOppPace,
opp_data.seasonOppDefRating, opp_data.seasonOppOpen3, opp_data.seasonOppWide3,
opp_data.mvLCAllowed, opp_data.mvRCAllowed, opp_data.mvABVAllowed
    
    
from plyrLogs plogs
LEFT JOIN 
(SELECT team_id,season,game_id,home,offensive_rating as teamOffRating,
    pace as teamPace,
    AVG(pace) OVER
    (PARTITION BY team_id,season ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgTeamPace,
    RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) tmGameCt 
    from teamLog) tm USING (team_id,game_id) 
LEFT JOIN teams tms USING (team_id)
LEFT JOIN players ply USING (player_id)
LEFT JOIN daysSince USING (player_id,game_id)
--LEFT JOIN q1 USING (player_id,game_id)
LEFT JOIN  opp_data on opp_data.game_id = plogs.game_id and plogs.team_id <> opp_data.opp_id
