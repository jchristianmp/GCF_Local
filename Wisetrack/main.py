import json
import pandas as pd
import requests
from google.cloud import storage
from urllib.parse import parse_qs, urlparse
from requests.structures import CaseInsensitiveDict


# py -m pip install pandas
# py -m pip install requests
# py -m pip install google.cloud
# google-cloud-bigquery
# google-cloud-storage
# requests
# pytz
# pandas
# numpy
# flask


def CargarJson(request):

    request_json = request.get_json(silent=True)


# url
url = "http://ei.wisetrack.cl/Peru/PacasMayo/UltimaPosicion"

# parametros necesarios
token = "9347818e-8471-34eb-b121-d4ec668937ee"
parametros = {"Usuario": "TMPacasMayo", "Patente": "0"}

headers = CaseInsensitiveDict()
headers["Accept"] = "application/json"
headers["Authorization"] = "Bearer " + token

# enviammos request
response = requests.get(url, params=parametros, headers=headers).json()
# print(response['Posiciones'])

type(response)

## codigo para subir a google cloud storage
storage_client = storage.Client.from_service_account_json(
    "G:\Aynitech\Cementos\POC\Funciones\GCF_Local\gc-funcions-tests-3e5122367a53.json"
)
bucket = storage_client.bucket("json_local_f")
blob = bucket.blob("response.json")
blob.upload_from_string(data=json.dumps(response), content_type="application/json")


# archivo = json.load('G:\Aynitech\Cementos\POC\Funciones\GCF_Local\response.json')

# prueba de lectura json
with open("G:/Aynitech/Cementos/POC/Funciones/GCF_Local/response_json.json") as f:
    ejemplo = json.load(f)
ejemplo

rv
print(rv)

blob.upload_from_filename(json_data)

blob = bucket.blob("response.json")
with open(response, "rb") as f:
    blob.upload_from_file(f)

blob = bucket.blob("json_ejemplo_2.json")
blob.upload_from_string(response)
with open(response, "rb") as f:
    blob.upload_from_file(f)


blob = bucket.blob("t.json")
with open("t.json", "rb") as f:
    blob.upload_from_file("f")


# bucket = storage_client.bucket('prueba')
bucket = storage_client.create_bucket("prueba1")
bucket.location = "json_local_f"
bucket.create()

bucket = storage_client.bucket("prueba_repo/prueba2")
# bucket.storage_class = "Standard"
new_bucket = storage_client.create_bucket(bucket, location="us")


# Crear carpeta dentro de un bucket
bucket = storage_client.get_bucket("prueba_repo")
blob = bucket.blob("carpeta")

blob.upload_from_string(
    "", content_type="application/x-www-form-urlencoded;charset=UTF-8"
)

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





de main 2



Nombre del Cloud Storage: pecst_pacasmayo_raw_01_d
Ruta:
{FUENTE}/{dataset}/{grupo o tabla}/{yyyy}/{MM}/{dd}/{HH?}/{dataset}{grupo o tabla}{yyyyMMdd_HHmm}.{extension}

Ultima-Posicion : ej.
WISETRACK/wisetrack/ultima-posicion/2022/09/07/19/wisetrack_ultima_posicion_20220907_1947.json
WISETRACK/wisetrack/ultima-posicion/2022/09/07/19/wisetrack_ultima_posicion_20220907_1948.json
WISETRACK/wisetrack/ultima-posicion/2022/09/07/19/wisetrack_ultima_posicion_20220907_1949.json
Dentro de esta ruta se grabará el minuto a minuto por cada hora (60 archivos por hora)

 nombre_archivo='wisetrack_ultima_posicion_'+str(AgregarCeroMenor9(ANIO))+str(AgregarCeroMenor9(MES))+str(AgregarCeroMenor9(DIA))+'_'+str(AgregarCeroMenor9(HORA))+str(AgregarCeroMenor9(MINUTO))+'.json'
        bucket_gs=ruta_raiz_post+ruta_anio+ruta_mes+ruta_dia+ruta_hora+nombre_archivo
      
        #Ruta raiz en el GCP
        bucket_name='wisetrack'
        #convierte el diccionario a string
        contents=json.dumps(dataJsonUltimaPosicion)
        
        destination_blob_name=bucket_gs
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name, chunk_size=None)
        blob.upload_from_string(contents, content_type="text/json")


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




  #bucket = storage_client.bucket('prueba')
  bucket = storage_client.create_bucket('prueba1')
  bucket.location = 'json_local_f'
  bucket.create()

  bucket = storage_client.bucket('prueba_repo/prueba2')
  #bucket.storage_class = "Standard"
  new_bucket = storage_client.create_bucket(bucket, location="us")
  

  # Crear carpeta dentro de un bucket 
  bucket = storage_client.get_bucket('prueba_repo')
  blob = bucket.blob('carpeta')

  blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')




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











"""
