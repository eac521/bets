
CREATE TABLE player_game_features AS
WITH daysSince AS (
    SELECT player_id,game_id,
    julianday(lag(game_date,1) OVER (PARTITION BY player_id ORDER BY game_date)) as game1_date,
    julianday(lag(game_date,2) OVER (PARTITION BY player_id ORDER BY game_date)) as game2_date,
    julianday(lag(game_date,3) OVER (PARTITION BY player_id ORDER BY game_date)) as game3_date,
    julianday(lag(game_date,4) OVER (PARTITION BY player_id ORDER BY game_date)) as game4_date,
    julianday(lag(game_date,5) OVER (PARTITION BY player_id ORDER BY game_date)) as game5_date
    FROM plyrLogs
    )
SELECT
--basic identifiers
player_id,
game_id,
game_date,
season,
--running count of number of games played
RANK() OVER(PARTITION BY player_id,season ORDER BY game_date) plyrGameCt,
--time since last game
julianday(game_date) - game1_date - 1 daysBetweenGames,
(
case when julianday(game_date) - game1_date < 5 then 1 else 0 end +
case when julianday(game_date) - game2_date < 5 then 1 else 0 end +
case when julianday(game_date) - game3_date < 5 then 1 else 0 end +
case when julianday(game_date) - game4_date < 5 then 1 else 0 end +
case when julianday(game_date) - game5_date < 5 then 1 else 0 end) + 1
    gamesInFive,
(
case when julianday(game_date) - game3_date < 3 then 1 else 0 end +
case when julianday(game_date) - game2_date < 3 then 1 else 0 end +
case when julianday(game_date) - game1_date < 3 then 1 else 0 end) + 1 gamesInThree,
--rolling stats

AVG(pts) OVER (PARTITION BY player_id,season ORDER BY game_date) curPPG,
AVG(pts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 11 PRECEDING and 1 PRECEDING) mvAvg10pts,
AVG(pts) OVER (PARTITION BY player_id ORDER BY game_date ROWS BETWEEN 31 PRECEDING and 1 PRECEDING) mvAvg30pts,

-- Days since calculations
daysBetweenGames,
gamesInFive,
gamesInThree
FROM plyrLogs
LEFT JOIN
    (SELECT team_id,season,game_id,home
    from teamLog) tm USING (team_id,game_id)