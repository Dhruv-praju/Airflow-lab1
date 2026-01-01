# create DAG/pipeline

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.tasks import load_data, data_preprocessing, build_save_model, load_model_elbow
from airflow import configuration as conf
from airflow.operators.bash import BashOperator
from airflow.decorators import task


conf.set('core', 'enable_xcom_pickling', 'True') # enable picking for allowing data to be passed between tasks

# create a DAG
with DAG(dag_id='demo-lab', start_date=datetime(2025, 1, 31), schedule="0 0 * * *", catchup=False) as dag:
    hello = BashOperator(task_id="hello", bash_command="/bin/sh -c echo hello")

    @task()
    def airflow():
        print("airflow")

    hello >> airflow() # pipeline



with DAG(
    dag_id="Airflow-Lab1",
    start_date=datetime(2025, 1, 31),
    schedule_interval="*/5 * * * *", # run every 5 minutes
    catchup=False,
) as dag:
    
    # pipeline
    data = load_data()
    processed_data = data_preprocessing(data)
    sse = build_save_model(processed_data, "model.sav")
    load_model_elbow("model.sav", sse)
    

if __name__=="main":
    dag.test()
    