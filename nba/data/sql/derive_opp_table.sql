CREATE TABLE IF NOT EXISTS opp_data
AS
SELECT team_id as opp_id, game_id, ra_fga as ra_fgallowed,
       paint_fga as paint_fgallowed, mid_fga as mid_fgallowed,
       lc_fga as lc_fgallowed, rc_fga as rc_fgallowed, abv_fga as abv_fgallowed,
       open_fg3a, wide_fg3a, open_fg2a, wide_fg2a, games_in_five as oppGamesFive,
       games_in_three as oppGamesThree, daysBetweenGames as oppDaysLastGame,
       pace as oppPace, open3_rate, wide3_rate, open2_rate, wide2_rate, count_inactive,
       AVG(pace) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppPace,
       AVG(def_rate) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppDefRating,
       SUM(open_fg3a) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0 /
       SUM(threes_fga) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppOpen3,
       SUM(wide_fg3a) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0 /
       SUM(threes_fga) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppWide3,
       AVG(pace) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS UNBOUNDED PRECEDING) AS seasonOppPace,
       AVG(def_rate) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS UNBOUNDED PRECEDING) AS seasonOppDefRating,
       SUM(open_fg3a) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0 /
       SUM(threes_fga) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppOpen3,
       SUM(wide_fg3a) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0 /
       SUM(threes_fga) OVER (PARTITION BY season, team_id ORDER BY game_number
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppWide3
FROM team_def