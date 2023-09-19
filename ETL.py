import requests
import json

#Documentación de la API https://github.com/TBMSP/ChileAlertaApi

response_API = requests.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&limit=100&country=Chile")
print(response_API.status_code) #Estado de la solicutud (200 OK)
JSONText = json.loads(response_API.text) #Formato JSON a Diccionario python
ultimos_sismos = JSONText['ultimos_sismos_Chile']
print(ultimos_sismos[0:10])

# Respecto a la variabilidad de los datos se puede realizar:
# 1) Análisis de magnitud, agrupar eventos sísmicos por su magnitud 
# y categorizarlos en función de su intensidad (pequeños, moderados, grandes)
# 2) La referencia geográfica no me indicaría el epicentro del suceso realmente, 
# por lo que veo más conveniente transformar las coordenadas de latitud y longitud al lugar exacto del suceso, 
# por lo tanto, se podría categorizar por países y ciudades.