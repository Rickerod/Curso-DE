from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from Earthquake_ETL import conectar_Redshift, insert_data, send_email

default_args={
    'owner': 'SergioP',
    'retries': 5,
    'retry_delay': timedelta(minutes=2) # 2 min de espera antes de cualquier re intento
}

with DAG(
    dag_id="dag_pipeline_earthquake",
    default_args= default_args,
    description="Recopilar los ultimos 100 terremotos",
    start_date=datetime(2023,10,29,2),
    catchup=False,
    schedule_interval='@daily' ) as dag:
    

    task1 = BashOperator(
        task_id= 'iniciando',
        bash_command='echo Iniciando...'
    )

    task2 = PythonOperator(
        task_id='conexion_redshift',
        python_callable=conectar_Redshift,
    )

    task3 = PythonOperator(
        task_id='ETL_earthquake',
        python_callable=insert_data,
    )

    task4 = PythonOperator(
        task_id='send_email',
        python_callable=send_email,
    )

    task5 = BashOperator(
        task_id= 'completado',
        bash_command='echo Proceso completado...'
    )
    
    task1 >> task2 >> task3 >> task4 >> task5