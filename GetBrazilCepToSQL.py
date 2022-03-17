import json
import urllib
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.sqltypes import Integer

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
df = pd.json_normalize(data, record_path=['results']) \
    [['CEP','cidade','estado', 'bairro', 'logradouro', 'numero']] \
    .set_index('CEP')

dataType = {
    'CEP': Integer(),
    'numero': Integer()
}

# load data to mysql
engine = create_engine('mysql://root:MySql2019!@localhost/testepythondb')
with engine.connect() as conn, conn.begin():
    df.to_sql('brazil_cep', conn, if_exists='replace', index = True, chunksize=200, dtype=dataType)