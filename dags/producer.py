from airflow import DAG,Dataset
from airflow.decorators import task
from datetime import datetime



my_file=Dataset("/tmp/my_file.txt")#give the url


with DAG(
dag_id="Producer",
schedule="@daily",
start_date=datetime(2025,10,8),
catchup=False #donot want to run untrigger dags auto
)as d:
    @task(outlets=[my_file])
    def updat_my_file():
        with open(my_file.uri,"a+")as f:
            f.write("Producer update")
    
    updat_my_file()

    
