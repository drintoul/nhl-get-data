#!/usr/bin/env python
# coding: utf-8

# # Games

# Scrapes hockey-statistics.com for game data and populates SQL Server.

# ## Imports

# In[1]:


import sys
import pandas as pd
import pyodbc
import requests
from bs4 import BeautifulSoup
import datetime


# ## Functions

# In[2]:


def Connect_DB(database):

    """
    Connects to SQL Server database using userid and password. 
    Returns connection string or Null.
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

    except:

        return Null


# In[3]:


def Get_Games(url):

    """
    Scrapes url for game results.
    """

    try:

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        game_table = soup.findAll('tbody')

        games = list()

        for listing in game_table:

            rows = listing.find_all('tr')

            for row in rows:

                gamedate = row.find('th', {'data-stat': 'date_game'}).text
                gamedate = datetime.datetime.strptime(gamedate, '%Y-%m-%d').date()
                visitor = row.find('td', {'data-stat': 'visitor_team_name'}).text.strip()
                visitor_goals = row.find('td', {'data-stat': 'visitor_goals'}).text

                if visitor_goals == '':

                    break

                visitor_goals = int(visitor_goals)
                home = row.find('td', {'data-stat': 'home_team_name'}).text.strip()
                home_goals = row.find('td', {'data-stat': 'home_goals'}).text
                home_goals = int(home_goals)
                overtime = row.find('td', {'data-stat': 'overtimes'}).text
                attendance = row.find('td', {'data-stat': 'attendance'}).text.replace(',','')
                attendance = int(attendance)
                duration = row.find('td', {'data-stat': 'game_duration'}).text.split(':')
                duration = int(duration[0])*60 + int(duration[1])

                games.append((gamedate, visitor, visitor_goals, home, home_goals, overtime, attendance, duration))

        return games

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)


# In[4]:


def Save_Games(games):

    """
    Delete table Games and load games into it.
    """

    try:

        cursor = cnxn.cursor()

        if cursor.tables(table='Games', tableType='TABLE').fetchone():

            cursor.execute("DROP TABLE Games")

        query = """CREATE TABLE Games (                     gamedate date NOT NULL,                     visitor varchar(50) NOT NULL,                     visitor_goals int NOT NULL,                     home varchar(50) NOT NULL,                     home_goals int NOT NULL,                     overtime varchar(5) NOT NULL,                     attendance int NOT NULL,                     duration int NOT NULL)"""

        cursor.execute(query)

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)        

    try:

        cnxn.autocommit = False
        cursor.executemany("INSERT INTO Games(gamedate, visitor, visitor_goals, home, home_goals, overtime, attendance, duration)                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)", games)

    except Exception as e:

        print ('Error: {}'.format(e))
        cnxn.rollback()

    else:

        cnxn.commit()

    finally:

        cnxn.autocommit = True


# In[5]:


if __name__ == '__main__':

    cnxn = Connect_DB('NHL')

    url =  pd.read_sql("SELECT [url] FROM URLs WHERE [use] = 'games'", cnxn).values[0][0]

    games = Get_Games(url)

    Save_Games(games)

