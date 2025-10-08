from airflow import DAG,Dataset

from datetime import datetime
from airflow.decorators import task

my_file=Dataset("/tmp/my_file.txt")#give the url

with DAG(
    dag_id="constumer",
    schedule=[my_file]          , #Here tills this dag run when the dataset is updated
    start_date=datetime(2025,8,10),
    catchup=False,


)as d:
    
    @task
    def read_my_file():
        with open(my_file.uri,'r') as f:
            print(f.read())
    
    read_my_file()
