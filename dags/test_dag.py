
"""Example DAG demonstrating the usage of the BashOperator."""


from __future__ import annotations
from datetime import datetime , timedelta
#import datetime 
#import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
#from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="a_test_dag",
    #schedule="0 0 * * *", # run every at midnight
    schedule_interval=timedelta(seconds=30),
    #start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    start_date= datetime(2024, 12, 7),
    catchup=False,
    #dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["example"],
    #params={"example_key": "example_value"},
) as dag:

#     path = "/home/loic-cheong/Desktop/Airflow_projet" # path to the project folder on Linux

   # always write the last dag first 
    last_task = BashOperator(
        task_id="last_task",
        bash_command="echo 'End of task' ",
    )

    first_task = BashOperator(
            task_id="first_task",
            bash_command='echo "first_task ran by airflow at $(date)"  ',
    )

    second_task = BashOperator(
            task_id="second_task",
            bash_command='echo "second_task ########### at $(date)" ',
    )

    task3 = BashOperator(
            task_id="task3",
            bash_command='echo ######################################################################',
    )

    # Order of the task
    [first_task, second_task, task3] >> last_task


