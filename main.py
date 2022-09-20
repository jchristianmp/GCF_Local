import json
import pandas as pd
import requests
from google.cloud import storage
from urllib.parse import  parse_qs, urlparse

# py -m pip install pandas
#google-cloud-bigquery
#google-cloud-storage
#requests
#pytz
#pandas
#numpy
#flask

def CargarJson(request):

    request_json=request.get_json(silent=True)

# Obtener parametros de url
url = 'http://ei.wisetrack.cl/Peru/PacasMayo/UltimaPosicion'
Usuario='TMPacasMayo'
Patente='0'

queryurl=url + f'?Usuario={Usuario}&Patente={Patente}'

response = requests.get(queryurl)
print(response.text)

print(response.url)
parametros = parse_qs(urlparse(response.url).query)
parametros['Usuario']
parametros['Patente']




#archivo = json.load('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json')

with open('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json') as f:
    jfile = json.load(f)

jfile

with open('json_ejemplo.json', 'w') as f:
    json.dump(jfile, f, indent=2)


with open('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json','w') as fp:
    #data = json.load(fp)
    data = json.dump('myfile.json',fp)

print(data)

## codigo para subir a google cloud storage
storage_client = storage.Client.from_service_account_json('D:\Aynitech\Cementos\POC\Funciones\F1\gc-funcions-tests-3e5122367a53.json')
bucket = storage_client.bucket("json_local_f")
blob = bucket.blob('json_ejemplo.json')
with open('D:\Aynitech\Cementos\POC\Funciones\F1\json_ejemplo.json', 'rb') as f:
  blob.upload_from_file(f)


blob = bucket.blob('t.json')
with open('t.json', 'rb') as f:
  blob.upload_from_file('f')



