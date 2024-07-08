
    CREATE TABLE rosters(
    teamId TEXT,
    seasonYear TEXT,
    playerId TEXT,
    startDate TEXT,
    endDate TEXT,
    PRIMARY KEY (teamId,playerId,seasonYear),
    FOREIGN KEY (playerId) 
        REFERENCES players(playerId),
    FOREIGN KEY (teamId)
        REFERENCES teams(teamId)
    )
    
    