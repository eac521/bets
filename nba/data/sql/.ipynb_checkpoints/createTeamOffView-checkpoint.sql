CREATE VIEW team_off AS
SELECT 
season, substr(game_date,6,2) AS MONTH, teamAbrv AS team, RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) game_number,
count_inactive,
possessions,off_rate,
win, home,
pace, sht.*
FROM teamLog
JOIN teams USING (team_id)
LEFT JOIN (SELECT  team_id,game_id,
ra_fgm,ra_fga,mid_fgm,mid_fga,lc_fgm,lc_fga,rc_fgm,rc_fga,abv_fgm ,abv_fga,ftm,fta,
oreb,oreb_contest,oreb_chances,oreb_chance_defer,avg_oreb_dist,pfd
FROM plyrLogs
GROUP BY  game_id,team_id) sht USING (team_id,game_id)

    