import json
import urllib
import requests
import pandas as pd
import pymongo

url = 'https://parseapi.back4app.com/classes/Covid19Case?count=1&limit=10'
headers = {
    'X-Parse-Application-Id': 'zoZ3zW1YABEWJMPInMwruD5XHgqT4QluDAAVx0Zz', # This is the fake app's application id
    'X-Parse-Master-Key': 'gIo7p0nTyt72aROJqf0ronfzxGKw8Unjw0Zk6qFm' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need

# Format json response in a data frame
df = pd.json_normalize(data, record_path=['results']) \
    [['date.iso', 'countryName','cases','deaths', 'recovered']]  \
    .rename(columns={'date.iso':'date'})

df.date = df.date.map(lambda x: x.split('T')[0]) 
parsed = json.loads(df.to_json(orient='records'))

# load data to mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
mycol = mydb["covid"]

x = mycol.insert_many(parsed)