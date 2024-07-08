
    CREATE TABLE teams(
    teamId TEXT,
    teamAbrv TEXT,
    teamName TEXT,
    airportLat FLOAT,
    airportLong FLOAT,
    seasonYear TEXT,
    playerId INT,
    startDate DATE,
    endDate DATE,
    PRIMARY KEY teamId,
    FOREIGN KEY playerId 
        REFERENCES players(playerId)
    )
    
    