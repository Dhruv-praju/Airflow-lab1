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
    
    # Task to load data, calls the 'load_data' Python function
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data,
    )

    # Task to perform data preprocessing, depends on 'load_data_task'
    data_preprocessing_task = PythonOperator(
        task_id='data_preprocessing_task',
        python_callable=data_preprocessing,
        op_args=[load_data_task.output]
        # op_kwargs={
        #     "data_b64":"{{ ti.xcom_pull(task_ids='load_data_task') }}"
        # }
    )

    # Task to build and save a model, depends on 'data_preprocessing_task'
    build_save_model_task = PythonOperator(
        task_id='build_save_model_task',
        python_callable=build_save_model,
        op_args=[data_preprocessing_task.output, "model.sav"],

    )

    # Task to load a model using the 'load_model_elbow' function, depends on 'build_save_model_task'
    load_model_task = PythonOperator(
        task_id='load_model_task',
        python_callable=load_model_elbow,
        op_args=["model.sav", build_save_model_task.output],
    )

    # Set task dependencies
    load_data_task >> data_preprocessing_task >> build_save_model_task >> load_model_task

    

if __name__=="main":
    dag.test()
    