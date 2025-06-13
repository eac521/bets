
    CREATE TABLE teamBox(
    gameId TEXT,
    gameDate TEXT,
    teamId TEXT,
    home INT,
    Q1 INT,
    Q2 INT,
    Q3 INT,
    Q4 INT,
    OT INT,
    inactive TEXT,
    pace FLOAT,
    offRating FLOAT,
    defRating FLOAT,
    PRIMARY KEY (gameId,teamId),
    FOREIGN KEY (teamId)
        REFERENCES teams(teamId)
    )
    