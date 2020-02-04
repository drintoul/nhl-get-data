#!/usr/bin/env python
# coding: utf-8

# # Teams

# Get List of NHL Teams, including Conferences and Divisions and populate SQL Server.

# ## Imports


import sys
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pyodbc


# ## Functions


def Connect_DB(database):

    """
    Connects to SQL Server database using userid and password. 
    Returns connection string or exits.
    """

    try:
        
        with open('credentials.csv', 'r') as f:

            userid, password = pd.read_csv(f, skipinitialspace=True)
    
    except Exception as e:
        
        print (f'Error: {e}')
        sys.exit(1)

    try:

        driver = '{ODBC Driver 17 for SQL Server}'
        query = 'DRIVER={};SERVER=linuxserver;DATABASE={};UID={};PWD={}'.format(driver, database, userid, password)

        cnxn = pyodbc.connect(query)
        cnxn.autocommit = True

        return cnxn

    except Exception as e:
        
        print (f'Error: {e}')
        sys.exit(1)


def Get_Teams(url):

    """
    Scrapes url for Conference, Division, Team. Returns list of teams.
    """

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
                    name = team_city.find('span').findNext('span').text.strip()

                    listing = (city + ' ' + name, conf, div)

                    teams.append(listing)

        return teams[:-1] # exclude Seattle for now

    except Exception as e:
        
        print (f'Error: {e}')
        sys.exit(1)     


def Save_Data_to_Teams(teams):

    """
    Delete table Teams and load teams into it.
    """

    try:
        
        cursor = cnxn.cursor()
    
        if cursor.tables(table='Teams', tableType='TABLE').fetchone():
        
            cursor.execute("DROP TABLE Teams")        
        
        query = """CREATE TABLE Teams (team varchar(50) PRIMARY KEY NOT NULL, \
                                       conference varchar(50) NOT NULL, \
                                       division varchar(50) NOT NULL)"""
    
        cursor.execute(query)
        
    except Exception as e:
        
        print (f'Error: {e}')
        sys.exit(1)  
        
    try:

        cnxn.autocommit = False
        cursor.executemany("INSERT INTO Teams(team, conference, division) VALUES (?, ?, ?)", teams)

    except Exception as e:

        print (f'Error: {e}')
        cnxn.rollback()

    else:

        cnxn.commit()

    finally:

        cnxn.autocommit = True


# ## Main


if __name__ == '__main__':
    
    cnxn = Connect_DB('NHL')
    
    url = pd.read_sql("SELECT [url] FROM URLs WHERE [use] = 'teams'", cnxn).values[0][0]
    
    teams = Get_Teams(url)
    
    Save_Data_to_Teams(teams)