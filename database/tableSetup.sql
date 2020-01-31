CREATE TABLE basics(
    tconst TEXT PRIMARY KEY,
    titleType TEXT,
    primaryTitle TEXT,
    originalTitle TEXT,
    isAdult INTEGER,
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER,
    genres TEXT
);

CREATE TABLE ratings(
    tconst TEXT PRIMARY KEY,
    rating REAL,
    votes INTEGER
);

/*

-------------UNIMPLEMENTED TABLES--------

CREATE TABLE crew(
    tconst TEXT PRIMARY KEY,
    
);

CREATE TABLE principals(
    tconst TEXT PRIMARY KEY,
);
*/

