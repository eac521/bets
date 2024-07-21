
CREATE VIEW team_def AS
select 
season, substr(sht.game_date,6,2) as month, teamAbrv as team, rank() OVER(PARTITION BY team_id,season ORDER BY sht.game_date) game_number,
count_inactive,def_rate,
pace * 2 - possessions as possessionsAllowed, round((pace * 2 - possessions) * def_rate / 100) as points_allowed,
win, home, sht.*
from  shotsAllowed sht
JOIN teamLog log USING (team_id,game_id)
JOIN teams tms USING (team_id)
left join (select team_id,game_id,
dreb,dreb_contest,dreb_chances,dreb_chance_defer,avg_dreb_dist,pf
from plyrLogs
group by game_id,team_id) ply USING (game_id,team_id)
