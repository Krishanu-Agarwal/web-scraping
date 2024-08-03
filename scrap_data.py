'''
Author: Krishanu Agarwal
Date: 08/02/2024

This script processes the data from an url and saves it into csv file
and displays it on the command window. 
This script processes data specifically from:
    url = "https://www.speedtest.net/global-index"

Usage:
Install the dependcies:
    - requests
    - pandas
    - beautifulsoup4
    - mysql
    - mysql-connector-python
    - sqlalchemy
    - mysqldump
    make sure that mysqldump.exe is in the path variable - resposible for creating mysql DB backup
run the script: python scrap_data.py

output:
Saves 2 csv files as extracted from the webpage in the current working directory. 
2 Country ranking using median (mobile and Broadband): displayed on the webpage
Creates a backup of mysql database

P.S.:
The mean values are present but not displayed on the webpage and not taken into consideration
'''

import requests
import pandas as pd
import mysql.connector as con
import subprocess
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from typing import Union, List, Tuple

def get_page(url: str) -> BeautifulSoup:
    '''
    Parses through the html page and makes a soup
    Param : url -> string
    Return: page -> bs4.BeatuifulSoup
    '''
    page = requests.get(url)
    return BeautifulSoup(page.text, 'html.parser')

def get_table(soup: BeautifulSoup, 
              get_titles = True) -> Union[Tuple[List[BeautifulSoup], List[str]], List[BeautifulSoup]]:
    '''
    Gets all the tables from the HTML soup.
    Gives a list of all the tables (and) list of table titles. 
    Param: page -> bs4.BeautifulSoup
           get_titles -> bool (default: True)
    Return: tables, table_titles -> tuple(list, list) -> default
            tables -> list -> get_titles = False
    '''
    tables = soup.find_all('table')
    if get_titles:
        table_titles = [titles.text.strip() 
                        for titles in tables[2].find_all('th')]
        return tables, table_titles
    return tables

def create_data_frame(table_titles: list, 
                      table: BeautifulSoup,
                      class_: str ) -> pd.DataFrame:
    '''
    Creates Dataframe from the tables with a given class of data.
    For data class (class_) refer the html code
    Param: table_titles -> List
           table -> bs4.BeautifulSoup
           class -> string
    Return: df -> pandas.DataFrame
    '''
    df  = pd.DataFrame(columns = table_titles)
    row_data = table.find_all('tr', class_ = class_)
    for row in row_data:
        individual_row_data = [data.text.strip() 
                               for data in row.find_all('td')]
        df.loc[len(df.index)] = individual_row_data
    return df


if __name__ == '__main__':
    
    url = "https://www.speedtest.net/global-index"
    soup = get_page(url)
    tables, table_titles = get_table(soup)
    table_titles[0] = 'Ranking'
    table_titles.insert(1, 'Change_in_Ranking')

    # print(soup)
    # These information, specific to Countries are gained after reading the html soup
    table_country_mobile = tables[2]
    table_country_broadband = tables[4]
    data_class = "data-result results"

    df_country_mobile = create_data_frame(table_titles, table_country_mobile, data_class)
    df_country_broadband = create_data_frame(table_titles, table_country_broadband, data_class)

    print("------ Median Country speeds: Mobile ------ ")
    print(df_country_mobile.to_string(index=False))
    print()
    print("------ Median Country speeds: Broadband ------ ")
    print(df_country_broadband.to_string(index=False))

    #Saving the data to csv files. 
    df_country_mobile.to_csv('Rank_Country_Mobile.csv', index = False)
    df_country_broadband.to_csv('Rank_Country_Broadband.csv', index= False)

    # Creating MYSQL DataBase and backup
    host = 'localhost'
    user = 'user_name'
    password = 'password'
    database = 'rank_country_internet_speed'
    mysql_connection = con.connect(
    host = host,
    user = user,
    password = password,
    )
    if mysql_connection.is_connected():
        cursor = mysql_connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        cursor.execute(f"CREATE DATABASE {database}")
        cursor.execute(f"USE {database}")

        engine = create_engine(f"mysql+mysqlconnector://root:0817@localhost/{database}")

        df_country_mobile.to_sql('rank_country_mobile', con = engine, if_exists='append', index=False)
        df_country_broadband.to_sql('rank_country_broadband', con = engine, if_exists='append', index=False)

        cursor.close()
        mysql_connection.close()
    
        command = f"mysqldump -h{host} −u{user} −p{password}  {database}> {database}_backup.sql"
        subprocess.run(command, shell=True)


