import json
import requests
import pandas as pd
import pymongo

# Get data from API
url = 'https://parseapi.back4app.com/classes/Brazilianholidays'
headers = {
    'X-Parse-Application-Id': 'A6QtGS1Iuu6BiHnA7LWIVRRGw4RDyrQlKlY8p6Yg',
    'X-Parse-Master-Key': 'PVtQSsrAfqFz9jPrGOf94GbAPVaBuGYLNxisw1c6'
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8'))

# Format json response in a data frame
df = pd.json_normalize(data, record_path=['results']) \
    [['Date','Name','WeekDay']] \
    .rename(columns={'Date':'_id'})
df._id = df._id.map(lambda x: x.split('T')[0])
    
parsed = json.loads(df.to_json(orient='records'))

# load data to mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
mycol = mydb["holiday"]

x = mycol.insert_many(parsed)