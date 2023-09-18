import requests
import json

#Documentación de la API https://github.com/TBMSP/ChileAlertaApi

response_API = requests.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&country=chile")
print(response_API.status_code) #Estado de la solicutud (200 OK)
JSONText = json.loads(response_API.text) #Formato JSON a Diccionario python
JSONText['ultimos_sismos_chile'] #Los ultimos 15 sismos en Chile.


