CREATE VIEW roster_view AS
SELECT name, player_id, teamAbrv, team_id
FROM rosters r 
LEFT JOIN players p on r.playerId = p.player_id
LEFT JOIN teams t on r.teamId = t.team_id
WHERE endDate is Null
    