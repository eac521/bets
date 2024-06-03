
        CREATE VIEW playerGame as
        SELECT *
        FROM logs log
        left join rebounds rbs on log.PLAYER_ID = rbs.PLAYER_ID and log.GAME_ID = rbs.PLAYER_ID
        left join playershooting psh on log.PLAYER_ID = psh.PLAYER_ID and log.GAME_ID = psh.GAME_ID
        