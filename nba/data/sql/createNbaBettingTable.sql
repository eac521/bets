
    CREATE TABLE threes(
    player TEXT,
    time TEXT,
    amount FLOAT,
    DraftKings INT,
    FanDuel INT,
    ESPN INT,
    MGM INT
        
    PRIMARY KEY (player,time,amount)
    );

    CREATE TABLE points(
    player TEXT,
    time TEXT,
    amount FLOAT,
    DraftKings INT,
    FanDuel INT,
    ESPN INT,
    MGM INT
        
    PRIMARY KEY (player,time,amount)
    );

    CREATE TABLE firstBucket(
    player TEXT,
    time TEXT,
    amount FLOAT,
    DraftKings INT,
    FanDuel INT,
    ESPN INT,
    MGM INT
        
    PRIMARY KEY (player,time,amount)
    );