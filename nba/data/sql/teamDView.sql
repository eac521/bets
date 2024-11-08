
CREATE VIEW team_def AS
select 
season, substr(sht.game_date,6,2) as month, teamAbrv as team, rank() OVER(PARTITION BY team_id,season ORDER BY sht.game_date) game_number,
--last games played
julianday(sht.game_date) - julianday(lag(sht.game_date,1) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) - 1 daysBetweenGames,
(
case when julianday(sht.game_date) - julianday(lag(sht.game_date,5) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 5 then 1 else 0 end +
case when julianday(sht.game_date) - julianday(lag(sht.game_date,4) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 5 then 1 else 0 end + 
case when julianday(sht.game_date) - julianday(lag(sht.game_date,3) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 5 then 1 else 0 end +
case when julianday(sht.game_date) - julianday(lag(sht.game_date,2) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 5 then 1 else 0 end +
case when julianday(sht.game_date) - julianday(lag(sht.game_date,1) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 5 then 1 else 0 end) + 1 games_in_five,
(
case when julianday(sht.game_date) - julianday(lag(sht.game_date,3) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 3 then 1 else 0 end +
case when julianday(sht.game_date) - julianday(lag(sht.game_date,2) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 3 then 1 else 0 end +
case when julianday(sht.game_date) - julianday(lag(sht.game_date,1) OVER (PARTITION BY team_id,season ORDER BY sht.game_date)) < 3 then 1 else 0 end) + 1 games_in_three,
open_fg3a,
wide_fg3a,
--open shot rates    
open_fg3a / (CAST(lc_fga as FLOAT) + CAST(rc_fga AS FLOAT) + CAST(abv_fga AS FLOAT)) as open3_rate,
wide_fg3a / (CAST(lc_fga as FLOAT) + CAST(rc_fga AS FLOAT) + CAST(abv_fga AS FLOAT)) as wide3_rate,
open_fg2a / (CAST(mid_fga as FLOAT) + CAST(ra_fga AS FLOAT) + CAST(paint_fga AS FLOAT)) as open2_rate,
wide_fg2a / (CAST(mid_fga as FLOAT) + CAST(ra_fga AS FLOAT) + CAST(paint_fga AS FLOAT)) as wide2_rate,

count_inactive,def_rate,pace,
round(pace  * def_rate / 100) as points_allowed,
win, home, sht.*,crn_fga + abv_fga threes_fga
from  shotsAllowed sht
JOIN teamLog log USING (team_id,game_id)
JOIN teams tms USING (team_id)
left join (select team_id,game_id,
dreb,dreb_contest,dreb_chances,dreb_chance_defer,avg_dreb_dist,pf
from plyrLogs
group by game_id,team_id) ply USING (game_id,team_id)
