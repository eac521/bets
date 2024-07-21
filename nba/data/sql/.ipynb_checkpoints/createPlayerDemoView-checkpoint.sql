CREATE VIEW playerDemo AS
SELECT player_id,name,height,weight,pos,allstars,allnba_first,allnba_second,allnba_third,alld_first,alld_second,mip,mvp,dpoy,finals_mvp,
CASE WHEN pick = 'Undrafted' THEN 99 ELSE pick END AS pick,
CASE WHEN draft_year = 'Undrafted' THEN 99 ELSE substr(game_date,1,4) - draft_year END AS exp,
(julianday(substr(game_date,1,4) || '-10-15') - julianday(birthday)) / 365.25 age
from players