'''
SELECT  name, pl.game_date, opponent, min, paint_fgm + ra_fgm + mid_fgm AS two_fgm,paint_fga + ra_fga + mid_fga AS two_fga,
lc_fgm + rc_fgm + abv_fgm AS three_fgm,lc_fga + rc_fga + abv_fga AS three_fga, pl.possessions, fta, ftm, ast, oreb,dreb,
usagePercentage,
tl.pace as teamPace, offensiveRating, off_rate as teamOffensiveRating,
(CAST((paint_fga + ra_fga + mid_fga+lc_fga + rc_fga + abv_fga) AS FLOAT)/pl.possessions) AS shotsPerPossessions
FROM plyrLogs pl
LEFT JOIN teamLog tl  USING (team_id,game_id)
LEFT JOIN players USING(player_id)
LEFT JOIN
(SELECT teamName AS opponent,game_id,team_id
FROM teamLog
LEFT JOIN teams using (team_id)) opp
ON pl.game_id = opp.game_id AND opp.team_id <> pl.team_id
WHERE pl.game_date between '{}' AND '{}'
AND name in ({})
ORDER BY name, pl.game_date

''''