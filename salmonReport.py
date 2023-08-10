#!/usr/bin/env python
# coding: utf-8

# In[6]:


### DB AND TABLE SETUP
import sqlite3
import pandas as pd
from IPython.display import display, HTML

connection = sqlite3.connect(":memory:")
cursor = connection.cursor()

surveyURIs = {'2019':'https://five.epicollect.net/api/export/entries/salmon-survey-2019?form_ref=397fba6ecc674b74836efc190840c42d_5d6f454667a28',
              '2020':'https://five.epicollect.net/api/export/entries/salmon-survey-2020?form_ref=f550ab6c4dab44f49bcc33b7c1904be9_5d6f454667a28',
              '2021':'https://five.epicollect.net/api/export/entries/salmon-survey-2021?form_ref=ad5ffedf0a3246a18934e6ec36ed9569_5d6f454667a28',
              '2022':'https://five.epicollect.net/api/export/entries/salmon-survey-2022?form_ref=d46b5d8451f8410ea407bae5c8eb9f49_5d6f454667a28'}
salmonURIs = {'2019':'https://five.epicollect.net/api/export/entries/salmon-survey-2019?form_ref=397fba6ecc674b74836efc190840c42d_5d6f509867795',
              '2020':'https://five.epicollect.net/api/export/entries/salmon-survey-2020?form_ref=f550ab6c4dab44f49bcc33b7c1904be9_5d6f509867795',
              '2021':'https://five.epicollect.net/api/export/entries/salmon-survey-2021?form_ref=ad5ffedf0a3246a18934e6ec36ed9569_5d6f509867795',
              '2022':'https://five.epicollect.net/api/export/entries/salmon-survey-2022?form_ref=d46b5d8451f8410ea407bae5c8eb9f49_5d6f509867795'}

create_salmon_table_query = '''
    CREATE TABLE IF NOT EXISTS salmon (
        ec5_uuid TEXT PRIMARY KEY,
        ec5_parent_uuid TEXT,
        year DATE,
        created_at DATE,
        uploaded_at DATE,
        title TEXT,
        Distance INTEGER,
        Stream TEXT,
        Species TEXT,
        Sex TEXT,
        latitude REAL,
        longitude REAL,
        accuracy INTEGER,
        UTM_Northing INTEGER,
        UTM_Easting INTEGER,
        UTM_Zone TEXT,
        Notes TEXT,
        Photo_URL TEXT,
        Type TEXT,
        Carcass_Location TEXT,
        Predation TEXT,
        Length_Inches REAL,
        Width_Inches REAL,
        Carcass_Age TEXT,
        Hours_Since_Death INTEGER,
        Spawning_Success TEXT,
        Adipose_Fin TEXT,
        Activity TEXT
    );
'''
cursor.execute(create_salmon_table_query)

create_survey_table_query = '''
    CREATE TABLE surveys (
        ec5_uuid TEXT,
        year DATE,
        created_at DATE,
        uploaded_at DATE,
        title TEXT,
        Survey_Date DATE,
        Data_Recorder TEXT,
        Weather TEXT,
        Hours_Since_Storm TEXT,
        Water_Visibility TEXT,
        Flow TEXT,
        Water_Temperature INTEGER,
        Notes TEXT
);
'''
cursor.execute(create_survey_table_query)


# In[7]:


### DATA LOADING
import requests
salmon_insert_query = '''
        INSERT OR IGNORE INTO salmon (
            ec5_uuid,
            ec5_parent_uuid,
            year,
            created_at,
            uploaded_at,
            title,
            Distance,
            Stream,
            Species,
            Sex,
            latitude,
            longitude,
            accuracy,
            UTM_Northing,
            UTM_Easting,
            UTM_Zone,
            Notes,
            Photo_URL,
            Type,
            Carcass_Location,
            Predation,
            Length_Inches,
            Width_Inches,
            Carcass_Age,
            Hours_Since_Death,
            Spawning_Success,
            Adipose_Fin,
            Activity
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''
survey_insert_query = '''
    INSERT OR IGNORE INTO surveys (
        ec5_uuid,
        year,
        created_at,
        uploaded_at,
        title,
        Survey_Date,
        Data_Recorder,
        Weather,
        Hours_Since_Storm,
        Water_Visibility,
        Flow,
        Water_Temperature,
        Notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
'''
print('loading salmon into database')
for year in salmonURIs:
    print('loading for year: ' + year)
    uri = salmonURIs[year] + '&per_page=1000'
    allDataInserted = False
    while not allDataInserted:
        response = requests.get(uri)
        data = response.json()        
        for entry in data['data']['entries']:
            values = (
                entry['ec5_uuid'],
                entry['ec5_parent_uuid'],
                year,
                entry['created_at'],
                entry['uploaded_at'],
                entry['title'],
                entry['Distance'],
                entry['Stream'],
                entry['Species'],
                entry['Sex'],
                entry['Location']['latitude'],
                entry['Location']['longitude'],
                entry['Location']['accuracy'],
                entry['Location']['UTM_Northing'],
                entry['Location']['UTM_Easting'],
                entry['Location']['UTM_Zone'],
                entry['Notes'],
                entry['15_Photo'] if '15_Photo' in entry else None,
                entry['Type'],
                entry['Carcass_Location'],
                entry['Predation'],
                entry['Length_Inches'],
                entry['Width_Inches'],
                entry['Carcass_Age'] if 'Carcass_Age' in entry else None,
                entry['Hours_Since_Death'] if 'Hours_Since_Death' in entry else None,
                entry['Spawning_Success'],
                entry['Adipose_Fin'],
                entry['Activity'] if 'Activity' in entry else None
            )
            cursor.execute(salmon_insert_query, values)
        if data['links']['next'] is None:
            allDataInserted = True
        else:
            uri = data['links']['next'] + '&per_page=1000'

print('loading surveys into database')
for year in surveyURIs:
    print('loading for year: ' + year)
    uri = surveyURIs[year] + '&per_page=1000'
    response = requests.get(uri)
    data = response.json()
    for entry in data['data']['entries']:
        values = (
            entry['ec5_uuid'],
            year,
            entry['created_at'],
            entry['uploaded_at'],
            entry['title'],
            entry['Survey_Date'],
            entry['Data_Recorder'],
            entry['Weather'],
            entry['Hours_Since_Storm'],
            entry['Water_Visibility'],
            entry['Flow'],
            entry['Water_Temperature'],
            entry['Notes']         
        )
        cursor.execute(survey_insert_query, values)


# In[8]:


### STATS BY SURVEY TABLE
import IPython.core.display as ip
stats_by_survey_query = '''
SELECT
    Survey_Date,
    COUNT(CASE WHEN Species in ('Chum', 'Coho', 'Unknown', 'Sea-run Cutthroat') AND Type = 'Live' THEN salmon.ec5_uuid END) AS total_live_salmon_count,
    COUNT(CASE WHEN Species in ('Chum', 'Coho', 'Unknown', 'Sea-run Cutthroat') AND Type in ('Dead', 'Remnant') THEN salmon.ec5_uuid END) AS total_dead_salmon_count,
    COUNT(CASE WHEN Species = 'Chum' AND Type in ('Dead', 'Remnant') THEN salmon.ec5_uuid END) AS dead_chum_count,
    COUNT(CASE WHEN Species = 'Coho' AND Type in ('Dead', 'Remnant') THEN salmon.ec5_uuid END) AS dead_coho_count,
    COUNT(CASE WHEN Species = 'Unknown' AND Type in ('Dead', 'Remnant') THEN salmon.ec5_uuid END) AS dead_unknown_count,
    COUNT(CASE WHEN Species = 'Chum' AND Type = 'Live' THEN salmon.ec5_uuid END) AS live_chum_count,
    COUNT(CASE WHEN Species = 'Coho' AND Type = 'Live' THEN salmon.ec5_uuid END) AS live_coho_count,
    COUNT(CASE WHEN Species in ('Resident Cutthroat', 'Sea-run Cutthroat') AND Type = 'Live' THEN salmon.ec5_uuid END) as live_cutthroat_count,
    COUNT(CASE WHEN Type = 'Redd' THEN salmon.ec5_uuid END) AS redds_count
FROM
    salmon
INNER JOIN
    surveys ON surveys.ec5_uuid = salmon.ec5_parent_uuid
WHERE
    Species IN ('Coho', 'Chum')
GROUP BY
    Survey_Date;
'''
df = pd.read_sql(stats_by_survey_query, connection)
display(ip.HTML(df.to_html(index=False)))


# In[9]:


### REDDS TABLE
redds_table_query = '''
SELECT
    Stream, Distance, Survey_Date
FROM
    salmon
INNER JOIN
    surveys ON surveys.ec5_uuid = salmon.ec5_parent_uuid
WHERE Type = 'Redd'
'''
df = pd.read_sql(redds_table_query, connection)
display(ip.HTML(df.to_html(index=False)))


# In[ ]:


### USER INPUT QUERY
done = False
while not done:
    try:
        query = input("Enter a query: ")
        print("entering query: " + query)
        cursor.execute(query)
        print(cursor.fetchall())
    except sqlite3.Error as e:
        print("SQLite error:", e)


# In[ ]:


### CLOSE CONNECTION
connection.close()

