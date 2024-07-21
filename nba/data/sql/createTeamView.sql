CREATE VIEW team_game as 
SELECT season, game_id,
substr(game_date,6,2) MONTH, 
teamAbrv team, game_date,
RANK() OVER(PARTITION BY team_id,season ORDER BY game_date) game_number,
count_inactive,q1_pts,q2_pts,q3_pts,q4_pts,ot1_pts,ot2_pts,ot3_pts,ot4_pts,
q1_pts+q2_pts+q3_pts+q4_pts + COALESCE(ot1_pts, 0)+ COALESCE(ot2_pts, 0)+COALESCE(ot3_pts, 0)+COALESCE(ot4_pts, 0)  AS game_points_scored,
total_points - (q1_pts+q2_pts+q3_pts+q4_pts + COALESCE(ot1_pts, 0)+ COALESCE(ot2_pts, 0)+COALESCE(ot3_pts, 0)+COALESCE(ot4_pts, 0))  as points_allowed,
julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY team_id,season ORDER BY game_date)) - 1 daysBetweenGames,
(
case when julianday(game_date) - julianday(lag(game_date,5) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,4) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 5 then 1 else 0 end + 
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 5 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 5 then 1 else 0 end) + 1 games_in_five,
(
case when julianday(game_date) - julianday(lag(game_date,3) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,2) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 3 then 1 else 0 end +
case when julianday(game_date) - julianday(lag(game_date,1) OVER (PARTITION BY team_id,season ORDER BY game_date)) < 3 then 1 else 0 end) + 1 games_in_three,
case when q1_pts+q2_pts+q3_pts+q4_pts + COALESCE(ot1_pts, 0)+ COALESCE(ot2_pts, 0)+COALESCE(ot3_pts, 0)+COALESCE(ot4_pts, 0) > 
   total_points - (q1_pts+q2_pts+q3_pts+q4_pts + COALESCE(ot1_pts, 0)+ COALESCE(ot2_pts, 0)+COALESCE(ot3_pts, 0)+COALESCE(ot4_pts, 0)) then 1 else 0 end win, home 
FROM teamLog
JOIN teams USING (team_id)
JOIN 
    (SELECT game_id,sum(q1_pts+q2_pts+q3_pts+q4_pts + COALESCE(ot1_pts, 0)+ COALESCE(ot2_pts, 0)+COALESCE(ot3_pts, 0)+COALESCE(ot4_pts, 0)) total_points
    from teamLog
    group by game_id) gpts USING (game_id)