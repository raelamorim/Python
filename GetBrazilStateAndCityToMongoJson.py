import json
import urllib
from numpy.lib import index_tricks
import requests
import pymongo
from requests import api
import pandas as pd

# Get Countries
url = 'https://parseapi.back4app.com/classes/Country?limit=500'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
countries = pd.json_normalize(data, record_path=['results']).set_index('objectId')

# Get States
url = 'https://parseapi.back4app.com/classes/StateProvince?limit=10000'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
data= json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
states = pd.json_normalize(data, record_path=['results']).set_index('objectId')

# Get Cities
url = 'https://parseapi.back4app.com/classes/City?count=1&limit=3'
headers = {
    'X-Parse-Application-Id': 'fYzlRsd8SQNB2PE8AQw6CItJvqbS34BsKeCGgWED', # This is the fake app's application id
    'X-Parse-Master-Key': 'lvEumoJGgkmjOPcWICZpnfm2a8UqfyHDVJiWipMX' # This is the fake app's readonly master key
}
citiesJson = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
cities = []

# Convert countries to json
for c in citiesJson['results']:
    state = states.loc[c['state']['objectId'], 'name']
    country = countries.loc[c['country']['objectId'], ['country', 'currencyName','phone']]
    cities.append({ 
        "_id" : c['objectId'], 
        "name" : c['name'], 
        "state" : {
            "name" : state
        }, 
        "country" : {
            "name" : country[0],
            "currencyName" : country[1], 
            "phone" : country[2]
        }
    })

# load data to mongo
parsed = json.loads(json.dumps(cities))

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
collection = mydb["cities"]

for js in parsed:
    collection.replace_one({'_id': js['_id']}, js, upsert=True)

