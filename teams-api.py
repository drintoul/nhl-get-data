#!/usr/bin/env python
# coding: utf-8

# # teams-api

# Use NHL API to get franchise info

# ## Imports

# In[1]:


import pyodbc
import requests
import json
import pandas as pd


# ## Functions

# In[2]:


def Connect_DB(database):

    """
    Connects to SQL Server database using userid and password.
    Returns connection string or exits.
    """

    try:

        with open('credentials.csv', 'r') as f:

            userid, password = pd.read_csv(f, skipinitialspace=True)

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)

    try:

        driver = '{ODBC Driver 17 for SQL Server}'
        query = 'DRIVER={};SERVER=linuxserver;DATABASE={};UID={};PWD={}'.format(driver, database, userid, password)

        cnxn = pyodbc.connect(query)
        cnxn.autocommit = True

        return cnxn

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)


# In[3]:


def Get_Teams():

    """
    API requests and processes Franchise info
    """

    page = requests.get('https://statsapi.web.nhl.com/api/v1/teams')

    data = json.loads(page.content)['teams']

    teams = []

    for i in range(len(data)):

        team_id = data[i]['id']
        franchise = data[i]['name']
        abbreviation = data[i]['abbreviation']
        division = data[i]['division']['name']
        conference = data[i]['conference']['name']
        city = data[i]['locationName']
        name = data[i]['teamName']
        location = data[i]['venue']['city']
        venue = data[i]['venue']['name']
        tz = data[i]['venue']['timeZone']['tz']
        offset = data[i]['venue']['timeZone']['offset']
        firstYearOfPlay = data[i]['firstYearOfPlay']
        url = data[i]['officialSiteUrl']

        teams.append((team_id, franchise, abbreviation, division, conference, city, name, location, venue, tz, offset, url, firstYearOfPlay))

    return teams


# In[4]:


def Save_Teams(teams):

    """
    Delete table Teams and load teams into it.
    """

    try:

        cursor = cnxn.cursor()

        if cursor.tables(table='Teams', tableType='TABLE').fetchone():

            cursor.execute("DROP TABLE Teams")        

        query = """CREATE TABLE Teams (                     team_id int PRIMARY KEY NOT NULL,                     franchise varchar(50) NOT NULL,                     abbreviation char(3) NOT NULL,                     division varchar(50) NOT NULL,                     conference varchar(50) NOT NULL,                     city varchar(50) NOT NULL,                     name varchar(50) NOT NULL,                     location varchar(50) NOT NULL,                     venue varchar(50) NOT NULL,                     tz char(3) NOT NULL,                     offset int NOT NULL,                     url varchar(100) NOT NULL,                     firstYearOfPlay int NOT NULL)"""

        cursor.execute(query)

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)

    try:

        cnxn.autocommit = False
        cursor.executemany("INSERT INTO Teams (team_id, franchise, abbreviation, division, conference, city, name, location, venue, tz, offset, url, firstYearOfPlay) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", teams)

    except Exception as e:

        print ('Error: {}'.format(e))
        cnxn.rollback()

    else:

        cnxn.commit()

    finally:

        cnxn.autocommit = True


# ## Main

# In[5]:


if __name__ == '__main__':

    cnxn = Connect_DB('NHL')

    teams = Get_Teams()

    Save_Teams(teams)

