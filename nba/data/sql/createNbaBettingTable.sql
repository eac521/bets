
      CREATE TABLE threes(
    player_id TEXT,
    player TEXT,
    time TEXT,
    amount FLOAT,
    DraftKings INT,
    FanDuel INT,
    ESPN INT,
    MGM INT,
    PRIMARY KEY (player_id,time,amount)
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