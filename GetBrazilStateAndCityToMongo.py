import json
import urllib
from numpy.lib import index_tricks
import requests
import pandas as pd
import pymongo
from requests import api

# Get Countries
url = 'https://parseapi.back4app.com/classes/Country?limit=500'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
countries = pd.json_normalize(data, record_path=['results']) \
      [['objectId', 'country', 'currencyName', 'phone']] \
    .rename(columns={
        'country': 'countryName', 
        'currencyName': 'countryCurrencyName', 
        'phone': 'countryPhone'}
    ) \
    .set_index('objectId')

# Get States
url = 'https://parseapi.back4app.com/classes/StateProvince?limit=100'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
states = pd.json_normalize(data, record_path=['results']) \
      [['objectId','name']] \
    .rename(columns={'name': 'stateName'}) \
    .set_index('objectId')

# Get Cities
url = 'https://parseapi.back4app.com/classes/City?count=1&limit=10000'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
cities = (pd.json_normalize(data, record_path=['results']) 
    .join(states, on="state.objectId") 
    .join(countries, on="country.objectId") 
    .rename(columns={'objectId':'_id'}) 
    [['_id', 'name', 'stateName', 'countryName','countryCurrencyName', 'countryPhone']]
)

# convert countries to nested object
cities = (cities.groupby(['_id', 'name', 'stateName'], as_index=False)
    .apply(lambda x: pd.Series(
        x[['countryName','countryCurrencyName', 'countryPhone']]
            .rename(columns={
                'countryName':'name', 
                'countryCurrencyName':'currencyName',
                'countryPhone':'phone',
            })
            .to_dict('records'), 
        index=['country']
    ))
)

# convert states to nested object
cities['stateName'] = cities['stateName'].map(lambda x: dict([('name', x)]))
cities = cities.rename(columns={'stateName':'state'}) 

# load data to mongo
parsed = json.loads(cities.to_json(orient='records'))

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
collection = mydb["cities"]

for js in parsed:
    collection.replace_one({'_id': js['_id']}, js, upsert=True)