import json
# from random import random
# from tkinter import ANCHOR
# import pandas as pd
import requests
import datetime 
import os
import pytz
from google.cloud import storage

# from urllib.parse import  parse_qs, urlparse
# from requests.structures import CaseInsensitiveDict
# from datetime import date, datetime


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
    #serviceAccount = r'D:\Aynitech\Cementos\POC\Funciones\F1\gc-funcions-tests-3e5122367a53.json'
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

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