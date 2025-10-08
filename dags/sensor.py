


from airflow import DAG

from datetime import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor

with DAG(
dag_id="sensordag",
start_date=datetime(2025,10,7),
schedule="@daily",
catchup=False,


)as d:
    waitingflie=FileSensor(  task_id="waitingfile",
        poke_interval=30,
        timeout=60*5 ,# 5 min
        mode='reschedule',
        soft_fail=True)
      