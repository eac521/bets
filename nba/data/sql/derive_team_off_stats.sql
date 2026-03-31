
CREATE TABLE team_rolling_off AS
WITH summedShots as (
SELECT
--group by infomration
team_id, game_id, game_date, season,
--team totals - summed player values
sum(ra_fga) as teamRAAtt, sum(ra_fgm) as teamRAMakes, sum(paint_fga) as teamPaintAtt, sum(paint_fgm) as teamPaintMakes,
sum(mid_fga) as teamMidAtt, sum(mid_fgm) as teamMidMakes, sum(rc_fga) + sum(lc_fga) AS teamCornerAtts,
sum(rc_fgm) + sum(lc_fgm) AS teamCornerMakes, sum(abv_fga) AS teamAbvAtts, sum(abv_fgm) AS teamAbvMakes,

sum(pfd) as teamFoulsDrawn, sum(fta) as teamFtAtt, sum(ftm) as teamFTMade



FROM plyrLogs
GROUP BY team_id, game_id, game_date, season
)
SELECT
    team_id,
    game_id,
    season,
    -- Running sums

    -- Other team aggregations
    AVG(pos) OVER (PARTITION BY team_id, season ORDRE BY game_date) AS seasonAvgPoss,

FROM teamLog
JOIN ;