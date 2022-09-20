import json
import pandas as pd
import requests
from google.cloud import storage

#google-cloud-bigquery
#google-cloud-storage
#requests
#pytz
#pandas
#numpy
#flask

def CargarJson(request):

    request_json=request.get_json(silent=True)




# py -m pip install pandas

#archivo = json.load('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json')

with open('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json','w') as fp:
    #data = json.load(fp)
    data = json.dump('myfile.json',fp)

storage_client = storage.Client.from_service_account_json('D:\Aynitech\Cementos\POC\Funciones\F1\gold-vault-360521-5e392301aef3.json')
bucket = storage_client.bucket("test_json1")

blob = bucket.blob(data)
blob.upload_from_filename(data)


 map_index_to_word = pd.read_json('D:\Aynitech\Cementos\POC\Funciones\F1\ejemplo.json', typ='dictionary')
df = pd.DataFrame(map_index_to_word)
df

data
print(data['patente'])

df = pd.DataFrame({'count':data})


print(df)

