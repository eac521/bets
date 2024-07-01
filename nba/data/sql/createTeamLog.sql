
    CREATE TABLE teamLog(
        game_id TEXT,
        team_id TEXT,
        inactive TEXT,
        count_inactive INT,
        assist_pct FLOAT,
        off_rate FLOAT,
        def_rate FLOAT,
        pace FLOAT,
        possessions INT,
        off_reb_pct FLOAT,
        def_reb_pct FLOAT,
        home INT,
        q1_pts INT,
        q2_pts INT,
        q3_pts INT,
        q4_pts INT,
        PRIMARY KEY (game_id,team_id)
    )
    