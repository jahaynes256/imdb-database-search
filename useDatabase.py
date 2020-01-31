import sqlite3 as sql
import csv
import os
import webbrowser

# -------------SETTINGS--------------
databaseFile = "myDatabase.db"
databaseFileLoc = "database\\" + databaseFile

sqlFile = "searchQuery.sql"
sqlFileLoc = "database\\" + sqlFile
# -------------END SETTINGS-----------

url = "https://www.imdb.com/title/"


def openTabs(numTabs, crsr):
    """
    INPUT: INT, sql.connect(databaseFileLoc).execute() -- RETURN: None
    Opens numTabs of browser tabs from top numTabs entries. Also prints
    the top numTabs entries of the new table.
    """
    for row in crsr:
        numTabs -= 1
        if numTabs > 0:
            print(row)
            webbrowser.open_new_tab(f"{url}tt{row[0]}")
        else:
            break


def getSQLQuery():
    """
    INPUT: None -- RETURN: String
    Gets query from sqlFileLoc.sql, returns command as String.
    """
    sql_command = ""

    with open(sqlFileLoc, "r") as sqlFile:
        for line in sqlFile:
            sql_command += line
    return sql_command


def main():
    """
    To change query, modify searchQuery.sql
    Query DB --> Open Associated Movies as New Tabs in Browser
    """
    connection = sql.connect(databaseFileLoc)
    crsr = connection.execute(getSQLQuery())
    openTabs(10, crsr)


if __name__ == "__main__":
    main()
