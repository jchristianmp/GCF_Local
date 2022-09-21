import json
import pandas as pd
import requests
from google.cloud import storage
from urllib.parse import  parse_qs, urlparse
from requests.structures import CaseInsensitiveDict


# py -m pip install pandas
# py -m pip install requests
# py -m pip install google.cloud
#google-cloud-bigquery
#google-cloud-storage
#requests
#pytz
#pandas
#numpy
#flask

def CargarJson(request):

    request_json=request.get_json(silent=True)


# url
url = 'http://ei.wisetrack.cl/Peru/PacasMayo/UltimaPosicion'

# parametros necesarios
token = '9347818e-8471-34eb-b121-d4ec668937ee'
parametros = {
"Usuario":"TMPacasMayo",
"Patente":"0"
}

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "Bearer "+token

# enviammos request
response = requests.get(url,params=parametros,headers=headers).json()
#print(response['Posiciones'])

type(response)

## codigo para subir a google cloud storage
storage_client = storage.Client.from_service_account_json('G:\Aynitech\Cementos\POC\Funciones\GCF_Local\gc-funcions-tests-3e5122367a53.json')
bucket = storage_client.bucket("json_local_f")
blob = bucket.blob('response.json')
blob.upload_from_string(data=json.dumps(response),content_type='application/json')


#archivo = json.load('G:\Aynitech\Cementos\POC\Funciones\GCF_Local\response.json')

# prueba de lectura json
with open('G:/Aynitech/Cementos/POC/Funciones/GCF_Local/response_json.json') as f:
    ejemplo = json.load(f)
ejemplo

rv
print(rv)

blob.upload_from_filename(json_data)

blob = bucket.blob('response.json')
with open(response, 'rb') as f:
  blob.upload_from_file(f)

blob = bucket.blob('json_ejemplo_2.json')
blob.upload_from_string(response)
with open(response, 'rb') as f:
  blob.upload_from_file(f)


blob = bucket.blob('t.json')
with open('t.json', 'rb') as f:
  blob.upload_from_file('f')




"""

Usuario='TMPacasMayo'
Patente='0'
headers = {"Authorization": "Bearer MYREALLYLONGTOKENIGOT"}

token_c = 'Bearer '+ token

queryurl=url + f'?Usuario={Usuario}&Patente={Patente}'

requests.post(endpoint, data=data, headers=headers).json())

response = requests.get(queryurl)


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




"""