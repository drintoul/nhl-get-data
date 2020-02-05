#!/usr/bin/env python
# coding: utf-8

# # Teams

# Get List of NHL Teams, including Conferences and Divisions and populates SQL Server.

# ## Imports

# In[1]:


import sys
import pandas as pd
import pyodbc
import requests
from bs4 import BeautifulSoup


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


def Get_Teams(url):

    """
    Scrapes url for Conference, Division, Team. Returns list of teams.
    """

    mapping = {'Anaheim Ducks': 'ANA', 'Arizona Coyotes': 'ARI', 'Boston Bruins': 'BOS', 'Buffalo Sabres': 'BUF',
           'Calgary Flames': 'CGY', 'Carolina Hurricanes': 'CAR', 'Chicago Blackhawks': 'CHI', 'Colorado Avalanche': 'COL',
           'Columbus Blue Jackets': 'CBJ', 'Dallas Stars': 'DAL', 'Detroit Red Wings': 'DET', 'Edmonton Oilers': 'EDM',
           'Florida Panthers': 'FLA', 'Los Angeles Kings': 'LAK', 'Minnesota Wild': 'MIN', 'Montréal Canadiens': 'MTL',
           'Nashville Predators': 'NSH', 'New Jersey Devils': 'NJD', 'New York Islanders': 'NYI', 'New York Rangers': 'NYR',
           'Ottawa Senators': 'OTT', 'Philadelphia Flyers': 'PHI', 'Pittsburgh Penguins': 'PIT', 'San Jose Sharks': 'SJS',
           'St. Louis Blues': 'STL', 'Tampa Bay Lightning': 'TBL', 'Toronto Maple Leafs': 'TOR', 'Vancouver Canucks': 'VAN',
           'Vegas Golden Knights': 'VEG', 'Washington Capitals': 'WSH', 'Winnipeg Jets': 'WPG'}

    try:

        page = requests.get(url)
        soup = BeautifulSoup(page.content, 'html.parser')

        conferences = soup.findAll('section', {'class': 'conference'})

        teams = list()

        for conference in conferences:

            conf = conference.h2.text.replace('Conference', '').strip()

            divisions = conference.findAll('div', {'class': 'division'})

            for division in divisions:

                div = division.h3.text.replace('Division', '').strip()

                team_cities = division.findAll('a', {'class': 'team-city'})

                for team_city in team_cities: 
                    
                    city = team_city.find('span').text.strip()
                    
                    if city.startswith('Seattle'):
                        
                        break
                        
                    name = team_city.find('span').findNext('span').text.strip()

                    abbreviation = mapping[city + ' ' + name]

                    listing = (city + ' ' + name, abbreviation, conf, div)

                    teams.append(listing)

        return teams

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)


# In[4]:


def Save_Data_to_Teams(teams):

    """
    Delete table Teams and load teams into it.
    """

    try:

        cursor = cnxn.cursor()

        if cursor.tables(table='Teams', tableType='TABLE').fetchone():

            cursor.execute("DROP TABLE Teams")        

        query = """CREATE TABLE Teams (                     team varchar(50) PRIMARY KEY NOT NULL, 
                    abbreviation char(3) NOT NULL, \
                    conference varchar(50) NOT NULL, \
                    division varchar(50) NOT NULL)"""

        cursor.execute(query)

    except Exception as e:

        print ('Error: {}'.format(e))
        sys.exit(1)

    try:

        cnxn.autocommit = False
        cursor.executemany("INSERT INTO Teams(team, abbreviation, conference, division) VALUES (?, ?, ?, ?)", teams)

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

    url = pd.read_sql("SELECT [url] FROM URLs WHERE [use] = 'teams'", cnxn).values[0][0]

    teams = Get_Teams(url)

    Save_Data_to_Teams(teams)

