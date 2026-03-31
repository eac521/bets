'''
SELECT  name, game_date, min, paint_fgm + ra_fgm + mid_fgm AS two_fgm,paint_fga + ra_fga + mid_fga AS two_fga,
lc_fgm + rc_fgm + abv_fgm AS three_fgm,lc_fga + rc_fga + abv_fga AS three_fga, possessions, fta, ftm, ast, oreb,dreb,
usagePercentage,
teamPace, offensiveRating, teamOffRating,
(CAST((paint_fga + ra_fga + mid_fga+lc_fga + rc_fga + abv_fga) AS FLOAT)/possessions) AS shotsPerPossessions,
--oppenent information
opponent,mvAvgOppWide3Rate, mvGood3Rate, mvAvgOppDefRating,mvRCAllowed, mvLCAllowed mvABVAllowed
FROM pgames
WHERE game_date between '{}' AND '{}'
AND name in ({})
ORDER BY name, game_date

'''
