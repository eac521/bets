
CREATE VIEW team_def AS
    WITH grpdShots AS (
    SELECT team_id, game_id, game_date,
    CAST(lc_fga + rc_fga + abv_fga AS FLOAT) as threes_fga,
    CAST(mid_fga + ra_fga + paint_fga AS FLOAT) as twos_fga
    FROM shotsAllowed
)

WITH daysSince AS (
    SELECT player_id, game_date,
    julianday(lag(game_date,1) OVER (PARTITION BY player_id,season ORDER BY game_date)) as game1_date,
    julianday(lag(game_date,2) OVER (PARTITION BY player_id,season ORDER BY game_date)) as game2_date,
    julianday(lag(game_date,3) OVER (PARTITION BY player_id,season ORDER BY game_date)) as game3_date,
    julianday(lag(game_date,4) OVER (PARTITION BY player_id,season ORDER BY game_date)) as game4_date,
    julianday(lag(game_date,5) OVER (PARTITION BY player_id,season ORDER BY game_date)) as game5_date
    FROM teamLog
    )
select 
season, substr(sht.game_date,6,2) as month, teamAbrv as team, rank() OVER(PARTITION BY team_id,season ORDER BY sht.game_date) game_number,
--last games played
julianday(sht.game_date) - game1_date - 1 daysBetweenGames,
(
case when julianday(log.game_date) - game5_date < 5 then 1 else 0 end +
case when julianday(log.game_date) - game4_date < 5 then 1 else 0 end + 
case when julianday(log.game_date) - game3_date < 5 then 1 else 0 end +
case when julianday(log.game_date) - game2_date < 5 then 1 else 0 end +
case when julianday(log.game_date) - game1_date < 5 then 1 else 0 end) + 1 games_in_five,
(
case when julianday(log.game_date) - game3_date < 3 then 1 else 0 end +
case when julianday(log.game_date) - game2_date < 3 then 1 else 0 end +
case when julianday(log.game_date) - game1_date < 3 then 1 else 0 end) + 1 games_in_three,
open_fg3a,
wide_fg3a,
--open shot rates    
open_fg3a / threes_fga as open3_rate,
wide_fg3a / threes_fga as wide3_rate,
open_fg2a / twos_fga as open2_rate,
wide_fg2a / twos_fga as wide2_rate,

count_inactive,def_rate,pace,
round(pace  * def_rate / 100) as points_allowed,
win, home, sht.*, threes_fga
from  shotsAllowed sht
JOIN teamLog log USING (team_id,game_id)
JOIN daysSince ds using (team_id,g
JOIN teams tms USING (team_id)
left join (select team_id,game_id,
dreb,dreb_contest,dreb_chances,dreb_chance_defer,avg_dreb_dist,pf
from plyrLogs
group by game_id,team_id) ply USING (game_id,team_id)
