CREATE VIEW teamShots AS

WITH playerAgg AS (
    SELECT team_id, game_id,game_date,
    sum(fta) as ftaTeam, sum(ftm) as ftmTeam, sum(ra_fgm) as ra_fgmTeam,
    SUM(ra_fga) AS ra_fgaTeam, SUM(paint_fga) AS paint_fgaTeam, SUM(paint_fgm) AS paint_fgmTeam, SUM(mid_fgm) AS mid_fgmTeam, 
    SUM(mid_fga) AS mid_fgaTeam, SUM(lc_fgm) + SUM(rc_fgm) AS crn_fgmTeam, SUM(lc_fga) + SUM(rc_fga) AS crn_fgaTeam,
    SUM(abv_fgm) AS abv_fgmTeam, SUM(abv_fga) as abv_fgaTeam
    FROM plyrLogs
    GROUP BY team_id, game_id, game_date
)
SELECT pa.team_id, pa.game_id, tl.season, pa.game_date,
---rolling information
AVG(pa.paint_fgaTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgPaintTAtt,
AVG(pa.ra_fgaTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgRATAtt,
AVG(pa.mid_fgaTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgMidTAtt,
AVG(pa.crn_fgaTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgCrnTAtt,
AVG(pa.abv_fgaTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgAbvTAtt,
AVG(pa.paint_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgPaintTMakes,
AVG(pa.ra_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgRATMakes,
AVG(pa.mid_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgMidTMakes,
AVG(pa.crn_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgCrnTMakes,
AVG(pa.abv_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgAbvTMakes,
AVG(pa.abv_fgmTeam + pa.crn_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS mvAvgThreeTMakes,
--residuals of rolling average - game
AVG(pa.abv_fgmTeam + pa.crn_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) - 
(pa.abv_fgmTeam + pa.crn_fgmTeam) as teamResidThrees,
AVG(pa.paint_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) - 
pa.paint_fgmTeam AS teamResidPaint,
AVG(pa.ra_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) - 
pa.ra_fgmTeam AS teamResidRA,
AVG(pa.mid_fgmTeam) OVER (PARTITION BY tl.season, pa.team_id
ORDER BY pa.game_date ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) - 
pa.mid_fgmTeam as teamResidMid
FROM playerAgg pa
LEFT JOIN teamLog tl USING (team_id, game_id)