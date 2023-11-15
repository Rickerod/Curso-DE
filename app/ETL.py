import requests
import json
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
import os 
import smtplib
from email.mime.text import MIMEText

def send_email(subject, body, sender, recipients, password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
        smtp_server.login(sender, password)
        smtp_server.sendmail(sender, recipients, msg.as_string())
    print("Message sent!")

response_API = requests.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&limit=100&country=Chile")
print("Request API: ", response_API.status_code)

#JSON a diccionario
ultimos_sismos = json.loads(response_API.text)
ultimos_sismos = ultimos_sismos['ultimos_sismos_Chile']


# Variables de entorno 
load_dotenv()

url= os.getenv('URL')
data_base= os.getenv('DATA_BASE')
user= os.getenv('USER')
pwd= os.getenv('PWD')
port = os.getenv('PORT')

print(load_dotenv())

# Creando la conexión a Redsshift
try:
    conn = psycopg2.connect(
        host=url,
        dbname=data_base,
        user=user,
        password=pwd,
        port=port
    )
    print("¡Conexión existosa!")
    
except Exception as e:
    print("¡Conexión invalida!", e)
    
    
#Crear la tabla si no existe
with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sfparevalo_coderhouse.earthquake (
            id INTEGER primary key,
            state_earthquake INTEGER,
            utc_time TIMESTAMP,
            reference VARCHAR(255),
            magnitude DECIMAL(5, 2),
            scale_earthquake VARCHAR(5),
            latitude DECIMAL(8, 4),
            longitude DECIMAL(8, 4),
            depth_earthquake DECIMAL(8, 2)
        ) DISTSTYLE EVEN SORTKEY (utc_time);
    """)
    conn.commit()
    
#Consultar la ultima fecha del ultimo sismo registrado en la base de datos.
with conn.cursor() as cur:
    cur.execute('''
        SELECT utc_time
        FROM earthquake
        ORDER BY utc_time DESC
        LIMIT 1;
        ''')
    ultima_fecha = cur.fetchone()
    if ultima_fecha is None:
        ultima_fecha = pd.Timestamp.min #Fecha mas baja tipo Timestamp.
    else:
        ultima_fecha = ultima_fecha[0] 

    
df = pd.DataFrame(ultimos_sismos)

#Eliminar elementos duplicados
df.drop_duplicates(subset=['id'], inplace=True)

#Transformar el campo fecha a tipo fecha.
df['utc_time']= pd.to_datetime(df['utc_time'], format='%Y/%m/%d %H:%M:%S')

# Eliminar registros que posean valores nulos 
df = df.dropna(how='any',axis=0)

#Considerar datos que se van a guardar en la base de datos.
df = df[['id', 'state', 'utc_time', 'reference', 'magnitude', 'scale', 'latitude', 'longitude', 'depth']]

# Considerar solo los nuevos sismos desde la ultima fecha guardada en la base de datos
df = df[df['utc_time'] > ultima_fecha]

#Filtrar para los sismos con una magnitud mayor a 5mb escala richter.
df_magnitude = df[df['magnitude'] > 5]

#Si existen sismos con una magnitud mayor a 5 grados en al escala de richter, se envia el correo.
if(df_magnitude.shape[0] > 0):
    sismos_string = ""
    for index, row in df_magnitude.iterrows():  
        sismos_string += f"Fecha: {row['utc_time']} UTC, Lugar: {row['reference']}, Magnitud: {row['magnitude']} grados escala richter\n"
    subject = "Ultimos sismos - Magnitudes mayor a 5, escala richter"
    body = sismos_string
    sender = os.getenv('USER_EMAIL')
    recipients = ["pitersito5647@gmail.com"]
    password = os.getenv('USER_PASS')
    send_email(subject, body, sender, recipients, password)
else:
    print("No hubieron sismos mayores a 5 grados en la escala de richter")

#Insertando los datos en Redsfhift
with conn.cursor() as cur:
    try:
        execute_values(
            cur,
            '''
            INSERT INTO earthquake (id, state_earthquake, utc_time, reference, magnitude, scale_earthquake, latitude, longitude, depth_earthquake)
            VALUES %s
            ''',
            [tuple(row) for row in df.values],
            page_size=len(df)
        )
        conn.commit()
        print("¡Datos ingresados satisfactoriamente!")
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")

#Cerrar conexión
cur.close()
conn.close()
print("Conexión cerrada! ")
        