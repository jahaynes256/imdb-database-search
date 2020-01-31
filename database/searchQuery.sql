SELECT 
    basics.tconst,
    ratings.rating,
    ratings.votes,
    basics.titleType, 
    basics.primaryTitle, 
    basics.startYear, 
    basics.genres, 
    basics.runtimeMinutes
FROM basics INNER JOIN ratings ON basics.tconst=ratings.tconst
WHERE 
    ratings.rating > 7.0
    AND ratings.votes > 10000 
    AND basics.startYear > 1970 
    AND basics.genres LIKE "%Horror%" 
    AND basics.genres LIKE "%Comedy%" 
ORDER BY ratings.votes DESC;