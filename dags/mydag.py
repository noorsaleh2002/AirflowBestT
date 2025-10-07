from random import randint
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator

def _training_model():
    return randint(1,20)

def _choose_best(ti):
    accuracies = ti.xcom_pull(task_ids=['trainingA', 'trainingB', 'trainingc'])
    
    print(f"Raw XCom values: {accuracies}, types: {[type(acc) for acc in accuracies]}")
    
    # Convert all values to float (handles both strings and numbers)
    try:
        accuracies = [float(acc) if acc is not None else 0 for acc in accuracies]
    except (ValueError, TypeError) as e:
        print(f"Error converting values: {e}")
        return 'inaccurate'
    
    best_accuracy = max(accuracies)
    print(f"Best accuracy after conversion: {best_accuracy}")
    
    if best_accuracy > 8:
        return 'accurate'
    return 'inaccurate'
    



with DAG(
    dag_id='my_dag',
    start_date=datetime(2025,10,7),
    schedule="@daily",
    catchup=False,
    description="Training model",
    tags=["Data eng"]


) as dag:
    training_A=PythonOperator(
        task_id="trainingA",
        python_callable=_training_model
    )
    training_B=PythonOperator(
        task_id="trainingB",
        python_callable=_training_model
    )
    training_C=PythonOperator(
        task_id="trainingC",
        python_callable=_training_model
    )
    chose_best_model=BranchPythonOperator(
        task_id="chooseBestModel",
        python_callable=_choose_best
    )
    accurate = BashOperator(
        task_id='accurate',
        bash_command='echo "Model is accurate"'
    )

    inaccurate = BashOperator(
        task_id='inaccurate', 
        bash_command='echo "Model is inaccurate"'
    )
    [training_A,training_B,training_C] >> chose_best_model >> [accurate,inaccurate]