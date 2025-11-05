from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    tags=['test', 'team_2'],
)
def team_2_dag_1():

    @task
    def hello():
        print('Hello - Team 2 - DAG 1')
    
    hello()

team_2_dag_1()
