
CREATE VIEW pseason AS
SELECT name,
teamAbrv as team, RANK() OVER(PARTITION BY log.team_id,season ORDER BY log.game_date) game_number,   
season,
plogs.*

FROM teamLog log
JOIN plyrLogs plogs USING (team_id,game_id)
JOIN teams tms USING (team_id)
JOIN players USING (player_id)
