import json
import urllib
from numpy.lib import index_tricks
import requests
import pymongo
from requests import api
import pandas as pd

# Get aircrafts
url = 'https://parseapi.back4app.com/classes/Registro_Aeronaves_Brasil?limit=1000000'
headers = {
    'X-Parse-Application-Id': 'zLaEPLxVu3R4jBLCwjN67pzB6jiNcdpLA7jqGJ6A', # This is the fake app's application id
    'X-Parse-Master-Key': 'CNxTwluuEAl6KEexvwy207ayytvRuCrKC60yRBJO' # This is the fake app's readonly master key
}
aircraftsJson = json.loads(requests.get(url, headers=headers).content.decode('utf-8')) # Here you have the data that you need
aircrafts = []

# Convert aircrafts to json
for a in aircraftsJson['results']:
    registrationId = a['NR_CERT_MATRICULA'] if 'NR_CERT_MATRICULA' in a else None
    registrationId = None if registrationId == 'String' else registrationId

    if registrationId != None:
        brand = a['MARCA'] if 'MARCA' in a else None
        serialNumber = a['NR_SERIE'] if 'NR_SERIE' in a else None
        yearManufacture = a['NR_ANO_FABRICACAO'] if 'NR_ANO_FABRICACAO' in a else None
        makerName = a['NM_FABRICANTE'] if 'NM_FABRICANTE' in a else None
        ownerName = a['PROPRIETARIO'] if 'PROPRIETARIO' in a else None
        ownerState = a['SG_UF'] if 'SG_UF' in a else None
        ownerDocumentId = a['CPF_CNPJ'] if 'CPF_CNPJ' in a else None
        makerName = a['NM_FABRICANTE'] if 'NM_FABRICANTE' in a else None
        userName = a['NM_OPERADOR'] if 'NM_OPERADOR' in a else None
        userState = a['UF_OPERADOR'] if 'UF_OPERADOR' in a else None
        userDocumentId = a['CPF_CGC'] if 'CPF_CGC' in a else None
        minCrew = int(a['NR_TRIPULACAO_MIN']) if 'NR_TRIPULACAO_MIN' in a else None
        maxPassengers = int(a['NR_PASSAGEIROS_MAX']) if 'NR_PASSAGEIROS_MAX' in a else None
        numberSeats = int(a['NR_ASSENTOS']) if 'NR_ASSENTOS' in a else None

        aircrafts.append({ 
            "_id" : a['objectId'], 
            "registration_id": registrationId,
            "brand": brand,
            "serial_number": serialNumber,
            "year_manufacture": yearManufacture,
            "owner": {
                "name": ownerName, 
                "state": ownerState, 
                "document_id": ownerDocumentId
            },
            "user": {
                "name": userName, 
                "state": userState, 
                "document_id": userDocumentId
            },
            "maker": {
                "name" : makerName
            },
            "capacity": {
                "min_crew": minCrew, 
                "max_passengers": maxPassengers, 
                "number_seats": numberSeats
            },
        })

# load data to mongo
identeded = json.dumps(aircrafts, indent=2)
parsed = json.loads(identeded)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["testepythondb"]
collection = mydb["aircrafts"]

for js in parsed:
    collection.replace_one({'_id': js['_id']}, js, upsert=True)