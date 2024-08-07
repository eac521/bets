
CREATE VIEW pgames AS
SELECT name, teamAbrv as team, game_number,season,
plogs.*
from plyrLogs plogs
JOIN 
    (SELECT team_id,season,game_id,
    RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) game_number   
    from teamLog) tm USING (team_id,game_id) 
JOIN teams tms USING (team_id)
JOIN players USING (player_id)
