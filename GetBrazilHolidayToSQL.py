import json
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import Date

# Get data from API
url = 'https://parseapi.back4app.com/classes/Brazilianholidays'
headers = {
    'X-Parse-Application-Id': 'A6QtGS1Iuu6BiHnA7LWIVRRGw4RDyrQlKlY8p6Yg',
    'X-Parse-Master-Key': 'PVtQSsrAfqFz9jPrGOf94GbAPVaBuGYLNxisw1c6'
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8'))

# Format json response in a data frame
df = pd.json_normalize(data, record_path=['results'])[['Date','Name','WeekDay']]
df.Date = df.Date.map(lambda x: x.split('T')[0])
df = df.set_index('Date')

dataType = {
    'Date': Date()
}

# load data to mysql
engine = create_engine('mysql://root:MySql2019!@localhost/testepythondb')
with engine.connect() as conn, conn.begin():
    df.to_sql('brazil_holiday', conn, if_exists='replace', index= True, chunksize=200, dtype=dataType)