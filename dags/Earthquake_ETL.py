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

user=Variable.get("user_redshift")
pwd= Variable.get("secret_pass_redshift")


def conectar_Redshift():
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
    
    #Vaciar la tabla para evitar duplicados o inconsistencias
    with conn.cursor() as cur:
        cur.execute("Truncate table earthquake")
        count = cur.rowcount
    cur.close()
    conn.close()

def insert_data():

    def get_earthquakes():
        response_API = requests.get("https://chilealerta.com/api/query/?user=demo&select=ultimos_sismos&limit=100&country=Chile")
        
        #JSON a diccionario
        ultimos_sismos = json.loads(response_API.text)
        ultimos_sismos = ultimos_sismos['ultimos_sismos_Chile']

        return ultimos_sismos

    def transform_data():
        df = pd.DataFrame(get_earthquakes())
        #Eliminar elementos duplicados
        df.drop_duplicates(subset=['id'], inplace=True)
        
        #Transformar el campo fecha a tipo fecha.
        df['utc_time']= pd.to_datetime(df['utc_time'], format='%Y/%m/%d %H:%M:%S')
        
        # Eliminar registros que posean valores nulos 
        df = df.dropna(how='any',axis=0)
        
        #Considerar datos que se van a guardar en la base de datos.
        df = df[['id', 'state', 'utc_time', 'reference', 'magnitude', 'scale', 'latitude', 'longitude', 'depth']]

        return df 

    df = transform_data()

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