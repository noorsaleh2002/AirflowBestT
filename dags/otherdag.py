from random import randint
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.decorators import task

with DAG(
 dag_id='otherdag',
    start_date=datetime(2025,10,7),
    schedule="@daily",
    catchup=False,
    description="Training model",
    tags=["Data eng"]


) as d:
    @task
    def training_model(accuracy):
      return accuracy
    
    @task.branch
    def choose_best(accuracies):
        
        
        best_accuracy = max(accuracies)
        print(f"Best accuracy after conversion: {best_accuracy}")
        
        if best_accuracy > 8:
            return 'accurate'
        return 'inaccurate'
    accurate = BashOperator(
        task_id='accurate',
        bash_command='echo "Model is accurate"'
    )

    inaccurate = BashOperator(
        task_id='inaccurate', 
        bash_command='echo "Model is inaccurate"'
    )
    choose_best(training_model.expand(accuracy=[5,19,6])) >> [accurate,inaccurate]