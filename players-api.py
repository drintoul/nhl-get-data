#!/usr/bin/env python
# coding: utf-8

# # players-api

# Use NHL API to get player info

# ## Imports

# In[1]:


import sys
import pyodbc
import requests
import json
import pandas as pd
import datetime


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


def Get_Players():

    """
    API requests and parses player info.
    """

    teams = pd.read_sql("SELECT [team_id] from Teams", cnxn)

    team_ids = teams['team_id'].tolist()

    players = []

    for team_id in team_ids:

        query = 'https://statsapi.web.nhl.com/api/v1/teams/{}/roster'.format(team_id)
        page = requests.get(query)
        data = json.loads(page.content)['roster']

        for item in data:

            player_id = item['person']['id']
            players.append(player_id)

    people = list()

    for player in players:

        query = 'https://statsapi.web.nhl.com/api/v1/people/{}'.format(player)
        page = requests.get(query)
        data = json.loads(page.content)['people'][0]

        fullName = data['fullName']
        primaryNumber = data['primaryNumber']
        dob = data['birthDate']
        birthDate = datetime.datetime.strptime(dob, '%Y-%m-%d').date()
        birthCity = data['birthCity']

        birthStateProvince = data.get('birthStateProvince')

        birthCountry = data['birthCountry']
        nationality = data['nationality']
        height_parts = data['height'].replace('\"', '').split("'")
        height = int(height_parts[0]) * 12 + int(height_parts[1])
        weight = data['weight']
        alternateCaptain = data['alternateCaptain']
        captain = data['captain']
        rookie = data['rookie']
        shootsCatches = data['shootsCatches']
        team_id = data['currentTeam']['id']
        type = data['primaryPosition']['type']
        position = data['primaryPosition']['name']

        people.append((player, fullName, primaryNumber, birthDate, birthCity, birthStateProvince, birthCountry, nationality, height, weight, alternateCaptain, captain, rookie, shootsCatches, team_id, type, position))

    return people


# In[4]:


def Save_Players(players):

    """
    Delete table Teams and load teams into it.
    """

    try:

        cursor = cnxn.cursor()

        if cursor.tables(table='Players', tableType='TABLE').fetchone():

            cursor.execute("DROP TABLE Players")        

        query = """CREATE TABLE Players (                     player_id int PRIMARY KEY NOT NULL,                     fullName varchar(50) NOT NULL,                     primaryNumber varchar(5) NOT NULL,                     birthDate DATE NOT NULL,                     birthCity varchar(50) NOT NULL,                     birthStateProvince varchar(50),                     birthCountry varchar(5) NOT NULL,                     nationality varchar(5) NOT NULL,                     height int NOT NULL,
                    weight int NOT NULL, \
                    alternateCaptain BIT NOT NULL, \
                    captain BIT NOT NULL, \
                    rookie BIT NOT NULL, \
                    shootsCatches char(1) NOT NULL, \
                    team_id int NOT NULL, \
                    type varchar(10) NOT NULL,\
                    position varchar(10) NOT NULL)"""

        cursor.execute(query)

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)

    try:

        cnxn.autocommit = False
        cursor.executemany("INSERT INTO Players                            (player_id, fullName, primaryNumber, birthDate, birthCity, birthStateProvince, birthCountry,                            nationality, height, weight, alternateCaptain, captain, rookie, shootsCatches, team_id, type,                            position)                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", players)

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

    players = Get_Players()

    Save_Players(players)

