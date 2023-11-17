import requests
import json
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from airflow.models import Variable

#-------------------- 
url = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
data_base = "data-engineer-database"
port = "5439"

user= Variable.get("user_redshift")
pwd= Variable.get("secret_pass_redshift")


def conectar_redshift():
    #Creando la conexión a Redsshift
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port=port
        )
        
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
            )  DISTSTYLE EVEN SORTKEY (utc_time);
        """)
        conn.commit()
        
def insert_data(ti):

    def get_earthquakes():
        response_API = requests.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&limit=100&country=Chile")
        
        #JSON a diccionario
        ultimos_sismos = json.loads(response_API.text)
        ultimos_sismos = ultimos_sismos['ultimos_sismos_Chile']

        return ultimos_sismos

    #Conexion redshift
    try:
        conn = psycopg2.connect(
            host=url,
            dbname=data_base,
            user=user,
            password=pwd,
            port=port
        )
        
    except Exception as e:
        print("¡Conexión invalida!", e)

    #Consultar la ultima fecha del ultimo sismo registrado en la base de datos.
    with conn.cursor() as cur:
        cur.execute('''
            SELECT utc_time
            FROM earthquake
            ORDER BY utc_time DESC
            LIMIT 1;
            ''')
        ultima_fecha = cur.fetchone()
        
        #Si no existen registros en la base de datos
        if ultima_fecha is None: 
            ultima_fecha = pd.Timestamp.min #Fecha mas baja tipo Timestamp.
        else:
            ultima_fecha = ultima_fecha[0]

    def transform_data(ultima_fecha):
        df = pd.DataFrame(get_earthquakes())
        #Eliminar elementos duplicados
        df.drop_duplicates(subset=['id'], inplace=True)
        
        #Transformar el campo fecha a tipo fecha.
        df['utc_time']= pd.to_datetime(df['utc_time'], format='%Y/%m/%d %H:%M:%S')
        
        # Eliminar registros que posean valores nulos 
        df = df.dropna(how='any',axis=0)
        
        #Considerar datos que se van a guardar en la base de datos.
        df = df[['id', 'state', 'utc_time', 'reference', 'magnitude', 'scale', 'latitude', 'longitude', 'depth']]

        #Considerar solo los sismos de la ultima fecha registrada en la base de datos, para evitar duplicados
        df = df[df['utc_time'] > ultima_fecha]

        return df 

    df = transform_data(ultima_fecha)

    #Filtrar para los sismos con una magnitud mayor al grado que se define en escala richter.
    magnitude = ti.xcom_pull(key="magnitude_earthquake")
    df_magnitude = df[df['magnitude'] > magnitude]

    #En el caso de que exista algun registro de un sismo con una magnitud mayor a 5, se envia el correo.
    if(df_magnitude.shape[0] > 0):
        sismos_string = ""
        for index, row in df_magnitude.iterrows():  
            sismos_string += f"Fecha: {row['utc_time']} UTC, Lugar: {row['reference']}, Magnitud: {row['magnitude']} grados escala richter\n"
        body = sismos_string

        #send_email(subject, body, sender, recipients, password)
        ti.xcom_push(key="earthquakes", value=True)
        ti.xcom_push(key="body", value=body)
    else:
        ti.xcom_push(key="earthquakes", value=False)

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
        except Exception as err:
            print(err)

    #Cerrar conexión
    cur.close()
    conn.close()

#-----------------------------------------------   