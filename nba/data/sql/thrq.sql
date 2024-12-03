
-- This primiarily calculates rolling fields and game information for the days game to be used in the model.
-- Need to check game info in the future to see if we can preload a schedule
-- Shot attempts will be done in seperate queries as they are going through a python function for the rolling information
-- Future updates - how to incorporate inactives, can do a rolling value but would like more information on who is out and impact

SELECT 
--y
threesMade,

--identifiers
name, player_id, game_id, game_date, season, team,

--demo data
height, exp, age,

--shot locations, will have percentiles done in pandas
ra_fga, paint_fga, mid_fga, (coalesce(lc_fga,0) + coalesce(rc_fga,0)) crn_fga, abv_fga,

--games info
daysBetweenGames, gamesInFive, gamesInThree, oppGamesFive, OppGamesThree, oppDaysLastGame, home, plyrGameCt, tmGameCt,
CASE WHEN plyrGameCt<= 10 THEN 1 ELSE 0 END as plyrfirst10,

--roling offensive (5 games and season) metrics 

AVG(threesMade) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgThrees,
AVG(usagePercentage) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgUsage,
AVG(offensiveRating) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOffRating,
    
SUM(ftm) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgFtPrct,

SUM(lc_fgm+rc_fgm + abv_fgm) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(lc_fga + rc_fga + abv_fga) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) as mvAvgThrPtPrct,    

AVG(usagePercentage) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonUsage,
AVG(offensiveRating) OVER (PARTITION BY season,player_id
    ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOffRating,
    
SUM(ftm) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as seasonFtPrct,
SUM(lc_fgm+rc_fgm + abv_fgm) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(lc_fga + rc_fga + abv_fga) OVER (PARTITION BY season,player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as seasonThrPtPrct,

--career metrics
SUM(ftm) OVER (PARTITION BY player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
/ SUM(fta) OVER (PARTITION BY player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)  as careerFtPrct,
    
SUM(lc_fgm+rc_fgm + abv_fgm) OVER (PARTITION BY player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
/    SUM(lc_fga + rc_fga + abv_fga) OVER (PARTITION BY player_id
    ORDER BY plyrGameCt ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) as careerThrPtPrct, 

AVG(usagePercentage) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS careerUsage,
AVG(offensiveRating) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS careerOffRating,
AVG(threesMade) OVER (PARTITION BY player_id
    ORDER BY game_date ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS careerAvgThrees,
    

-- defensive information
opp_id, 
--moving (5 games) and season averages
mvAvgOppPace, mvAvgOppOpen3, mvAvgOppWide3, mvAvgOppDefrating, 
seasonOppPace,  seasonOppOpen3,  seasonOppWide3,  seasonOppDefRating


FROM pgames
WHERE player_id in --changing logic for runs - updating now for current year 11/01
                    (SELECT player_id
                    FROM pgames 
                    WHERE season = '2024-25'
                    and min is not Null
                    GROUP BY player_id
                    HAVING (AVG(coalesce(lc_fga,0) + coalesce(rc_fga,0) + coalesce(abv_fga,0)) >= 2.2
                    )
                    )
--adding filter for 
ORDER BY GAME_DATE