import json
import requests
from Endereco import Endereco

URL = "https://viacep.com.br/ws/09371605/json/"
r = requests.get(url = URL)
r.encoding = r.apparent_encoding

if  r.status_code == requests.codes.ok:
    data = r.json()
    print(data)
    cep = json.dumps(data['cep'])
    logradouro = json.dumps(data['logradouro'])
    bairro = json.dumps(data['bairro'])
    localidade = json.dumps(data['localidade'])
    uf = json.dumps(data['uf'])
    e = Endereco(cep, logradouro, bairro, localidade, uf)
    print(e)