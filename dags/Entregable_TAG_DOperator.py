from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.python_operator import BranchPythonOperator

with DAG(
    dag_id="dag_pipeline_DockerOperator",
    description="Recopilar los ultimos 100 terremoto",
    start_date=datetime(2023, 10, 28),  # Fecha de inicio
    schedule_interval=timedelta(days=1) 
    ) as dag:
    
    task1= BashOperator(
        task_id='dag_execute',
        bash_command='echo Iniciando... '
    )

    docker_task = DockerOperator(
        task_id='my_task',
        image='app:latest',
        container_name='ETL-earthquake',
        api_version='auto',
        auto_remove=True,
        network_mode="bridge"
    )

    task2= BashOperator(
        task_id='dag_finish',
        bash_command='echo Finalizando... '
    )

    task1 >> docker_task >> task2