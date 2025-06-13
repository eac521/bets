CREATE VIEW rollingOppD
    AS (
SELECT  team_id as opp_id,game_id,
--rolling averages
    AVG(pace) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppPace,
    AVG(def_rate) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppDefRating,
    AVG(paint_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgPaintAtt,
    AVG(ra_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgRAAtt,
    AVG(mid_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgMidAtt,
    AVG(lc_fga + rc_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgCrnAtt,
    AVG(abv_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgAbvAtt,
    AVG(pf) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgFouls,
    
    
--rolling averages on rate stats
    SUM(open_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppOpen3,
    SUM(wide_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgOppWide3,   

--season averages    
    AVG(pace) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS UNBOUNDED PRECEDING) AS seasonOppPace,
    AVG(def_rate) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS UNBOUNDED PRECEDING) AS seasonOppDefRating,
    AVG(paint_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonPaintAtt,
    AVG(ra_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonRAAtt,
    AVG(mid_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonMidAtt,
    AVG(lc_fga + rc_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonCrnAtt,
    AVG(abv_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonAbvAtt,
    AVG(pf) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonFouls,
    
--season averages on rate stats
    SUM(open_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppOpen3,
    
    SUM(wide_fg3a) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) * 1.0
    / SUM(threes_fga) OVER (PARTITION BY season,team_id
    ORDER BY game_number ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS seasonOppWide3
 
    FROM team_def

    )