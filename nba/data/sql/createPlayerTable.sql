
    CREATE TABLE box (
    playerId TEXT,
    teamId TEXT,
    gameId TEXT,
    min FLOAT,
    ftm INT,
    fta INT,
    resAreaM INT,
    resAreaA INT,
    paintM INT,
    paintA INT,
    midM INT,
    midA INT,
    lcThreeM INT,
    lcThreeA INT,
    rcThreeM INT,
    rcThreeA INT,
    aboveThreeM INT,
    aboveThreeA INT,
    orb INT,
    orbContest INT,
    orbChances INT,
    orbDefer INT,
    avgOrbDist FLOAT,
    drb INT,
    drbContest INT,
    drbChances INT,
    drbDefer INT,
    avgDrbDist FLOAT,
    ast INT,
    tov INT,
    stl INT,
    blk INT,
    shotBlk INT,
    pf INT,
    pfd INT,
    pts INT,
    plusMinus INT,
    dd INT,
    td INT,
    usage FLOAT,
    pace FLOAT,
    poss INT,
    offRating FLOAT,
    defRating FLOAT,
    teamFirstBasket INT,
    gameFirstBasket INT,
    PRIMARY KEY (playerId,gameId)
        FOREIGN KEY (playerId )
            REFERENCES players(playerId)
        FOREIGN KEY (gameId)
            REFERENCES games(gameId)
            
    
    )
    
    