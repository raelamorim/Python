import json
import urllib
import requests
import pandas as pd
import pymongo

# Get data from API
where = urllib.parse.quote_plus("""
{
    "cidade": "Monte Alto"
}
""")
url = 'https://parseapi.back4app.com/classes/CEP?limit=1000&order=estado,cidade&where=%s' % where
headers = {
    'X-Parse-Application-Id': 'QqDpRY6ILPi2ze5mPrSfLgoN3HuYLfJytg30AtBq',
    'X-Parse-Master-Key': 'YqmyWSSMx8F17rGuWFJYYQ0SH734Im8KGCzn4FY5'
}
data = json.loads(requests.get(url, headers=headers).content.decode('utf-8'))

# Format json response in a data frame
result = pd.json_normalize(data, record_path=['results']) \
    [['CEP','cidade','estado', 'bairro', 'logradouro', 'numero']] \
    .rename(columns={'CEP':'_id'}) \
    .to_json(orient='records')

parsed = json.loads(result)

# load data to mongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
mycol = mydb["cep"]

x = mycol.insert_many(parsed)