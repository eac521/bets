
CREATE VIEW team_def AS
SELECT 
season, substr(sht.game_date,6,2) AS month, teamAbrv as team, RANK() OVER(PARTITION BY team_id,season ORDER BY sht.game_date) game_number,
count_inactive,def_rate,pace,
pace * 2 - possessions as possessionsAllowed,
win, home, sht.*
FROM  shotsAllowed sht
JOIN teamLog log USING (team_id,game_id)
JOIN teams tms USING (team_id)
LEFT JOIN (SELECT team_id,game_id,
dreb,dreb_contest,dreb_chances,dreb_chance_defer,avg_dreb_dist,pf
FROM plyrLogs
GROUP BY game_id,team_id) ply USING (game_id,team_id)