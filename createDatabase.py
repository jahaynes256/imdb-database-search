import sqlite3 as sql
import csv
import os
import sys
import subprocess
import requests
import gzip
import time

# ------------SETTINGS------------
basicsFile = "title.basics.tsv.gz"
basicsFileLoc = "downloads\\" + basicsFile

ratingsFile = "title.ratings.tsv.gz"
ratingsFileLoc = "downloads\\" + ratingsFile

databaseFile = "myDatabase.db"
databaseFileLoc = "database\\" + databaseFile

databaseTimeFile = "database.txt"
databaseTimeFileLoc = "database\\" + databaseTimeFile

downloadFolder = "downloads\\"

databaseSoftwareLoc = (
    "C:\\Program Files\\DB Browser for SQLite\\DB Browser for SQLite.exe"
)
useDatabaseFile = "useDatabase.py"
# ------------END SETTINGS------------


def getAgeOfDatabase():
    """
    INPUT: None -- RETURN: INT
    Opens databaseTimeFile to get last time of access in UNIX epoch time (ns) and returns it.
    EX:
    At time of documentation, getAgeOfDatabase() == 1580499024342698100
    
    """
    try:
        with open(databaseTimeFileLoc, "r") as f:
            lastUpdated = int(f.read())
    except:
        lastUpdated = 0
    return lastUpdated


def databaseUpToDate():
    """
    INPUT: None -- RETURN: BOOL
    Gets nanoseconds between current UNIX epoch time and databaseTimeFile and if greater
    than 1 week as hours, returns True. Else False
    EX:
    When it has been less than a week since last download, databaseUpToDate() == True
    
    """
    hrAsNanoSecs = 1000000000 * 60 * 60
    nanoSecSinceUpdate = time.time_ns() - getAgeOfDatabase()
    print(f"Hours since last update: {nanoSecSinceUpdate/hrAsNanoSecs:.2f}")
    return nanoSecSinceUpdate < (24 * 7 * hrAsNanoSecs)


def downloadIMDBInterface():
    """
    INPUT: None -- RETURN: None
    Downloads datasets of interests from IMDB from datasets.imdbws.com/ to the
    downloads folder
    
    """
    urlList = [
        "https://datasets.imdbws.com/title.basics.tsv.gz",
        "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "https://datasets.imdbws.com/title.akas.tsv.gz",
        "https://datasets.imdbws.com/title.crew.tsv.gz",
        "https://datasets.imdbws.com/title.principals.tsv.gz",
    ]

    if not databaseUpToDate():
        command = input("Download? y/n: ")
        if command.lower()[0] == "y":
            for url in urlList:
                print(f"Downloading {url}...")

                r = requests.get(url)
                with open(f"{downloadFolder}\\{os.path.basename(url)}", "wb") as f:
                    f.write(r.content)

                print("\tDone")
            with open(f"{databaseTimeFileLoc}", "w") as f:
                print(time.time_ns())
                f.write(str(time.time_ns()))
            print(f"Time Logged into {databaseTimeFileLoc}")

    else:
        print("Downloaded Data is up to Date.")


def openSQLLiteDBViewer():
    """
    INPUT: None -- RETURN: None
    Opens a SQL DB Viewer to check if DB was created as expected.
    Returns when program is closed.
    """
    return subprocess.call([databaseSoftwareLoc, databaseFileLoc])


def openDatabaseSearch():
    """
    INPUT: None -- RETURN: INT
    Opens useDatabase.py. Possibly unsecure as mentioned in documentation.
    Returns 0 if it ran successfully, negative otherwise.
    """
    return subprocess.call(f"python {useDatabaseFile}", shell=True)


def genbasicsSQLInsertCmd(
    tconst,
    titleType,
    primaryTitle,
    originalTitle,
    isAdult,
    startYear,
    endYear,
    runtimeMinutes,
    genres,
):
    """
    INPUT: STR, STR, STR, STR, INT(BOOL), INT, INT, INT, STR -- RETURNS: None
    Creates the SQL commands to insert rows into the SQL table "basics".
    The if statement allows for Null, if true, to be treated as a seperate datatype
    from TEXT in SQL.
    """
    if genres == "NULL":
        command = f"""INSERT INTO basics(tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes,genres) 
        VALUES("{tconst}","{titleType}","{primaryTitle}","{originalTitle}",{isAdult},{startYear},{endYear},{runtimeMinutes},{genres});"""
    else:
        command = f"""INSERT INTO basics(tconst,titleType,primaryTitle,originalTitle,isAdult,startYear,endYear,runtimeMinutes,genres) 
        VALUES("{tconst}","{titleType}","{primaryTitle}","{originalTitle}",{isAdult},{startYear},{endYear},{runtimeMinutes},"{genres}");"""
    return command


def loadbasicsIntoDB(crsr):
    """
    INPUT: sql.connect(databaseFileLoc).cursor() OBJ from SQLite3 -- RETURN: None
    Currently implemented to ignore all but movies.
    Extracts data from .tsv in gZip file from IMDB's interface page that's
    downloaded locally to the downloads folder. Converts Strings to appropriate
    data type. Saves each column in row to appropriate variable name.
    Calls genbasicsSQLInsertCmd(...) to create SQL insert command.
    """
    with gzip.open(basicsFileLoc, "rt", encoding="utf-8") as tsvfile:
        file = csv.reader(tsvfile, delimiter="\t")
        next(file)
        for row in file:
            if str(row[1]) == "movie":  # Only allow movies to be put into basics table
                try:
                    if row[8] == "\\N":
                        genres = "NULL"
                    else:
                        genres = row[8]
                except:
                    print("Skipping, Invalid entry (Genre): " + str(row))
                    continue

                tconst = row[0].replace("tt", "")
                titleType = str(row[1])
                primaryTitle = str(row[2]).replace('"', '""')
                originalTitle = str(row[3]).replace('"', '""')
                try:
                    isAdult = int(row[4])
                except:
                    isAdult = 0
                try:
                    startYear = int(row[5])
                except:
                    startYear = "NULL"
                try:
                    endYear = int(row[6])
                except:
                    endYear = "NULL"
                try:
                    runtimeMinutes = int(row[7])
                except:
                    runtimeMinutes = "NULL"
                sqlInsertCmd = genbasicsSQLInsertCmd(
                    tconst,
                    titleType,
                    primaryTitle,
                    originalTitle,
                    isAdult,
                    startYear,
                    endYear,
                    runtimeMinutes,
                    genres,
                )
                crsr.execute(sqlInsertCmd)


def genRatingsSQLInsertCmd(tconst, rating, votes):
    """
    INPUT: String, Float, Int -- Returns: String
    Creates the insert command for SQLite3 as a String.
    """
    command = (
        f'INSERT INTO ratings(tconst,rating,votes) VALUES("{tconst}",{rating},{votes});'
    )
    return command


def loadRatingsIntoDB(crsr):
    """
    INPUT: sql.connect(databaseFileLoc).cursor() OBJ from SQLite3 -- Return: None
    Extracts data from .tsv in gZip file from IMDB's interface page that's
    downloaded locally to the downloads folder for the
    "ratings" table. Converts Strings to appropriate data type.
    Saves each column in row to appropriate variable name.
    Calls genRatingsSQLInsertCmd(...) to create SQL insert command.
    """
    with gzip.open(ratingsFileLoc, "rt", encoding="utf-8") as tsvfile:
        file = csv.reader(tsvfile, delimiter="\t")
        next(file)
        for row in file:
            tconst = row[0].replace("tt", "")
            try:
                rating = float(row[1])
            except:
                print(row[1])
                rating = "NULL"
            try:
                votes = int(row[2])
            except:
                print(row[2])
                votes = "NULL"
            try:
                crsr.execute(genRatingsSQLInsertCmd(tconst, rating, votes))
            except:
                print(row)
                continue


def createTables(connection):
    """
    INPUT: sql.connect(databaseFileLoc) OBJ from SQLite3 Return: None
    Executes the SQL script for defining the tables.
    """
    with open("database\\tableSetup.sql", "r") as f:
        sqlScript = f.read()
    connection.executescript(sqlScript)


def debug():
    """
    This function purpose is only to test other functions during development.
    """
    print("DEBUG MODE")
    main()


def main():
    """
    INPUT: None -- Return: None
    Download Data for SQL DB(if Old) --> Delete Old DB (TEMP) --> Parse Data --> Create DB
    --> View DB (Continue on Close) --> Open useDatabase.py to Query DB
    """
    downloadIMDBInterface()
    if os.path.exists(databaseFileLoc):
        print("Database Exist: Deleting...")
        os.remove(databaseFileLoc)
    connection = sql.connect(databaseFileLoc)
    crsr = connection.cursor()
    print("Creating Database...")
    print("\tCreating Tables...")
    createTables(connection)
    print("\tLoading Basics into DB...")
    loadbasicsIntoDB(crsr)
    print("\tLoading Ratings into DB...")
    loadRatingsIntoDB(crsr)
    connection.commit()
    connection.close()

    print("Opening DB Viewer...")
    if openSQLLiteDBViewer() == 0:
        print("Completed Successfully")
    print("Opening useDatabase.py...")
    openDatabaseSearch()


if __name__ == "__main__":
    try:
        flag = sys.argv[1].strip()
    except:
        flag = ""

    if flag == "-d":
        debug()
    else:
        main()
