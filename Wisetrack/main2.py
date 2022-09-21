import json
from random import random
from tkinter import ANCHOR
import pandas as pd
import requests
import os
import pytz
import datetime 
from google.cloud import storage
from urllib.parse import  parse_qs, urlparse
from requests.structures import CaseInsensitiveDict
from datetime import date, datetime


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


 # headers = CaseInsensitiveDict()
 # headers["Accept"] = "application/json"
 # headers["Authorization"] = "Bearer "+token

def prefijocero(num):

  if(num<=9):
    num='0'+str(num)
  else:
    str(num)
   
  return num

def CargarJson(request):

  try:
    request_json=request.get_json(silent=True)

    # Variables de entorno
         
    #### Obteniendo la llave del service account
    serviceAccount = r'D:\Aynitech\Cementos\POC\Funciones\F1\gc-funcions-tests-3e5122367a53.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

    token='Bearer '+str(os.getenv("token"))
    env=str(os.getenv("env"))

    #### Parametros
    patente = request.get('patente')
    
    #token = '9347818e-8471-34eb-b121-d4ec668937ee'
    #token_c = 'Bearer '+ token
    #parametros = {
    #"Usuario":"TMPacasMayo",
    #"Patente":"0"
    #}


    # enviammos request para obtener json
    url = 'http://ei.wisetrack.cl/Peru/PacasMayo/UltimaPosicion?Usuario=TMPacasMayo&Patente='+str(patente)
    response = requests.get(url,headers={"Authorization":token})
    ultima_posicion_json = response.json()
  
    # parametros de cloud storage
    project_name='gc-functions-test'
    bucket_name_raiz='pecst_pacasmayo_raw_01_'+str(env)
    nombre_json = 'response1.json'
    #nombre_final_ruta = raiz+nombre_json

    #Obteniendo fecha actual
    fecha_actual_ult_pos=datetime.datetime.now(pytz.timezone('America/Lima'))  
  
    # Ruta y nombre de archivo
    raiz='wisetrack/ultima-posicion/' 
    r_anho = str(fecha_actual_ult_pos.year)
    r_mes = prefijocero(fecha_actual_ult_pos.month)
    r_dia = str(prefijocero(fecha_actual_ult_pos.day))
    r_hora = str(prefijocero(fecha_actual_ult_pos.hour))
    r_min = str(prefijocero(fecha_actual_ult_pos.minute))
    prefijo = 'wisetrack_ultima_posicion_'
    
    nombre_archivo = prefijo+r_anho+r_mes+r_dia+'_'+r_hora+r_min+'.json'
    nombre_final_ruta = raiz+r_anho+'/'+r_mes+'/'+r_dia+'/'+nombre_archivo
  
    #Carga de json a cloud storage teniendo en cuenta la estructura de carpetas
    storage_client = storage.Client()
    #storage_client = storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    bucket = storage_client.bucket(bucket_name_raiz)
    blob = bucket.blob(nombre_final_ruta,chunk_size=None)
    blob.upload_from_string(data=json.dumps(ultima_posicion_json,ensure_ascii=False),content_type='text/json')


  except ValueError:
            print(ValueError)
            LOG='ERROR CARGA ULTIMA POSICION '



  return 'PROCESO TERMINADO:'

  """



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