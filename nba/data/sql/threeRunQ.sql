
-- This primiarily calculates rolling fields and game information for the days game to be used in the model.
-- Need to check game info in the future to see if we can preload a schedule
-- Shot attempts will be done in seperate queries as they are going through a python function for the rolling information
-- Future updates - how to incorporate inactives, can do a rolling value but would like more information on who is out and impact
WITH season_elig AS (
    SELECT player_id, season, CASE WHEN AVG(min) > 15 AND MAX(plyrGameCt) >= 10 THEN 1 ELSE 0 END AS eligible
    FROM pgames
    GROUP BY player_id, season
),
features AS (
SELECT 
--y
threesMade,

--identifiers
name, player_id, game_id, game_date, season, team,

--demo data
height,

--shot locations, will have percentiles done in pandas
ra_fga, paint_fga, mid_fga, (COALESCE(lc_fga,0) + COALESCE(rc_fga,0)) crn_fga, abv_fga,
--minFirst, crnFgaFirst, abvFgaFirst,

--games info
CASE WHEN daysBetweenGames > 9 THEN 10 ELSE daysBetweenGames END AS daysBetweenGames,
gamesInFive, gamesInThree, oppGamesFive, oppGamesThree,
CASE WHEN oppDaysLastGame > 9 THEN 10 ELSE oppDaysLastGame END AS oppDaysLastGame,
CASE WHEN daysBetweenGames > 9  THEN 10 ELSE daysBetweenGames END -
CASE WHEN oppDaysLastGame > 9 THEN 10 ELSE oppDaysLastGame END AS netRest,

 home, tmGameCt, Starter,
--rolling offensive (5 games and season) metrics
AVG(starter) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 11 PRECEDING AND 1 PRECEDING) AS mvAvgstart, 
AVG(threesMade) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgThrees,
AVG(usagePercentage) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgUsage,
AVG(offensiveRating) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOffRating,
AVG(marginOffRating) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgMarginOffRating,
AVG(min) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgMinutes,

    
SUM(ftm) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgFtPrct,

SUM(threesMade) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(threesAtt) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgThrPtPrct,    

AVG(usagePercentage) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS seasonUsage,
AVG(offensiveRating) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS seasonOffRating,
    
SUM(ftm) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) as seasonFtPrct,
SUM(threesMade) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(threesAtt) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) as seasonThrPtPrct,

--career metrics
SUM(ftm) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING)  as past3FtPrct,
    
SUM(threesMade) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(threesAtt) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) as past3ThrPtPrct, 

AVG(usagePercentage) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS past3Usage,
AVG(offensiveRating) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS past3OffRating,
AVG(threesMade) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS past3AvgThrees,
    

-- defensive information
opp_id, 
--moving (5 games) and season averages
mvAvgOppPace, mvAvgOppOpen3, mvAvgOppOpen3Rate, mvAvgOppWide3, mvAvgOppWide3Rate, mvAvgOppDefRating, 
seasonOppPace,  seasonOppOpen3,  seasonOppWide3,  seasonOppDefRating, mvGood3Rate,mvAvgTeamPace,
COUNT(*) OVER (PARTITION BY season, player_id ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS gamesToDate,
AVG(min) OVER (PARTITION BY season, player_id ORDER BY game_date ROWS BETWEEN 247 PRECEDING AND 1 PRECEDING) AS minToDate


FROM pgames
)
SELECT f.*,
       CASE WHEN f.gamesToDate >= 3 THEN f.minToDate > 15 ELSE COALESCE(prev.eligible, 0) = 1 END AS eligible
FROM features f
LEFT JOIN season_elig prev
    ON prev.player_id = f.player_id
   AND prev.season = printf('%d-%s', CAST(substr(f.season, 1, 4) AS INT) - 1, substr(f.season, 3, 2))
ORDER BY f.game_date
